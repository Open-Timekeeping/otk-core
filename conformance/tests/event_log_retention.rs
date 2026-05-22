//! Retention conformance for [`EventLog`].
//!
//! Verifies that backends honour the `RetentionExpired` contract in
//! [`port_out_event_log`]:
//!
//! - `read_range(from, _)` with `from` below the earliest retained offset
//!   returns [`StorageError::RetentionExpired`] (this check takes precedence,
//!   even when the requested range is otherwise empty).
//! - `subscribe(from)` with `from` below the earliest retained offset
//!   likewise returns `RetentionExpired`.
//! - Fully-evicted logs return `RetentionExpired { earliest_available: None }`.
//! - `latest_offset` / `earliest_offset` reflect the retention low-water mark.
//!
//! Exercised against the in-crate [`MemLog`] which exposes an explicit
//! `evict_below` helper so the test is deterministic. A real backend
//! (`adapter-event-log-segment`) honours the same contract via segment deletion.

use conformance::mem_log::MemLog;
use event_model::{
    Detection, DetectionId, DetectorId, OtkEvent, SensorData, SourceAttestation, TimebaseId,
    TimestampingMethod, TimingPointId,
};
use port_out_event_log::{EventLog, Offset, StorageError};

fn det(seq: u64) -> OtkEvent {
    OtkEvent::Detection(Detection {
        detection_id: DetectionId::new(format!("d-{seq}")),
        detector_id: DetectorId::new("loop-1"),
        timing_point_id: TimingPointId::new("tp"),
        subject_id: None,
        detected_at_ns: 1_000_000_000 + seq * 1_000_000,
        detected_at_uncertainty_ns: None,
        received_at_ns: None,
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("tb"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: seq,
        sensor: SensorData::BeamBreak,
    })
}

async fn populated_log(n: u64) -> MemLog {
    let mut log = MemLog::new();
    for i in 0..n {
        log.append(&[det(i)]).await.unwrap();
    }
    log
}

#[tokio::test]
async fn read_range_below_earliest_returns_retention_expired() {
    let mut log = populated_log(10).await;
    log.evict_below(Offset::new(5)).await;

    let err = log
        .read_range(Offset::new(2), None)
        .await
        .expect_err("must error");
    match err {
        StorageError::RetentionExpired { requested, earliest_available } => {
            assert_eq!(requested, Offset::new(2));
            assert_eq!(earliest_available, Some(Offset::new(5)));
        }
        other => panic!("expected RetentionExpired, got {other:?}"),
    }
}

#[tokio::test]
async fn read_range_above_earliest_succeeds_after_eviction() {
    let mut log = populated_log(10).await;
    log.evict_below(Offset::new(5)).await;
    let entries = log
        .read_range(Offset::new(5), Some(Offset::new(8)))
        .await
        .expect("ok");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].offset, Offset::new(5));
}

#[tokio::test]
async fn fully_evicted_log_reports_earliest_available_none() {
    let mut log = populated_log(10).await;
    log.evict_all().await;

    let err = log
        .read_range(Offset::new(0), None)
        .await
        .expect_err("must error");
    match err {
        StorageError::RetentionExpired { earliest_available, .. } => {
            assert_eq!(earliest_available, None, "fully-evicted log must report None");
        }
        other => panic!("expected RetentionExpired, got {other:?}"),
    }

    assert_eq!(log.latest_offset().await.unwrap(), None);
    assert_eq!(log.earliest_offset().await.unwrap(), None);
}

#[tokio::test]
async fn subscribe_below_earliest_returns_retention_expired() {
    let mut log = populated_log(10).await;
    log.evict_below(Offset::new(5)).await;

    let result = log.subscribe(Offset::new(0)).await;
    assert!(
        matches!(&result, Err(StorageError::RetentionExpired { .. })),
        "expected RetentionExpired error"
    );
}

#[tokio::test]
async fn earliest_offset_advances_with_eviction() {
    let mut log = populated_log(10).await;
    assert_eq!(log.earliest_offset().await.unwrap(), Some(Offset::new(0)));
    log.evict_below(Offset::new(3)).await;
    assert_eq!(log.earliest_offset().await.unwrap(), Some(Offset::new(3)));
}

#[tokio::test]
async fn read_range_inside_retained_window_unaffected() {
    let mut log = populated_log(10).await;
    log.evict_below(Offset::new(2)).await;
    let entries = log
        .read_range(Offset::new(2), Some(Offset::new(5)))
        .await
        .expect("ok");
    assert_eq!(entries.len(), 3);
}

#[tokio::test]
async fn in_flight_subscription_surfaces_retention_expired_when_evicted_past_cursor() {
    // Subscriptions held across an eviction that overtakes their cursor must
    // surface RetentionExpired, matching the behaviour real backends
    // (adapter-event-log-segment) exhibit when segment deletion overtakes
    // an active reader.
    let mut log = populated_log(10).await;
    let mut sub = log.subscribe(Offset::new(0)).await.expect("subscribe");

    // Consume the first entry so cursor is now 1.
    let first = sub.next_entry().await.expect("entry").expect("ok");
    assert_eq!(first.offset, Offset::new(0));

    // Evict past the subscription's cursor.
    log.evict_below(Offset::new(5)).await;

    // The next next_entry call must surface RetentionExpired.
    match sub.next_entry().await.expect("must yield a result") {
        Err(StorageError::RetentionExpired { requested, earliest_available }) => {
            assert_eq!(requested, Offset::new(1));
            assert_eq!(earliest_available, Some(Offset::new(5)));
        }
        other => panic!("expected RetentionExpired for evicted in-flight cursor, got {other:?}"),
    }

    sub.close().await.unwrap();
}

#[tokio::test]
async fn subscription_ahead_of_eviction_boundary_continues_normally() {
    let mut log = populated_log(10).await;
    let mut sub = log.subscribe(Offset::new(7)).await.expect("subscribe");

    // Evict everything below 5; subscription cursor is at 7, still inside
    // the retained window, so it should keep serving entries normally.
    log.evict_below(Offset::new(5)).await;

    let entry = sub.next_entry().await.expect("entry").expect("ok");
    assert_eq!(entry.offset, Offset::new(7));

    sub.close().await.unwrap();
}

#[tokio::test]
async fn evict_below_clamps_boundary_past_log_end() {
    // Caller passes a boundary past entries.len(); MemLog clamps to
    // next_offset (= entries.len()) so subsequent appends are not
    // immediately considered evicted. Without the clamp the log would
    // be soft-bricked: every new append would land at an offset < the
    // requested boundary and surface as RetentionExpired on read.
    let mut log = populated_log(10).await;
    log.evict_below(Offset::new(999)).await;

    // earliest_offset reports the clamped boundary, not the original 999.
    assert_eq!(
        log.earliest_offset().await.unwrap(),
        None,
        "all 10 entries are at offsets 0..10, clamped boundary is 10, so nothing retained"
    );

    // A new append succeeds and is readable, not immediately evicted.
    log.append(&[det(10)]).await.unwrap();
    let entries = log.read_range(Offset::new(10), Some(Offset::new(11))).await.unwrap();
    assert_eq!(entries.len(), 1, "post-clamp append must be readable, not soft-evicted");
    assert_eq!(entries[0].offset, Offset::new(10));
}
