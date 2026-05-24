//! Event Log conformance: append / read / subscribe round-trips against the
//! in-crate [`MemLog`] reference implementation.
//!
//! These tests act as both a contract check and a behavioural reference for
//! adapter authors: any storage backend must satisfy the same invariants the
//! `MemLog` does, or it isn't an `EventLog`.

use conformance::mem_log::MemLog;
use event_model::{
    Detection, DetectionId, DetectorId, OtkEvent, SensorData, SourceAttestation, TimebaseId,
    TimestampingMethod, TimingPointId,
};
use port_out_event_log::{EventLog, Offset};

fn det(seq: u64) -> OtkEvent {
    OtkEvent::Detection(Detection {
        detection_id: DetectionId::new(format!("det-{seq}")),
        detector_id: DetectorId::new("loop-1"),
        timing_point_id: TimingPointId::new("tp-start"),
        subject_id: None,
        detected_at_ns: 1_700_000_000_000_000_000 + seq * 1_000_000_000,
        detected_at_uncertainty_ns: None,
        received_at_ns: None,
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("gps-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: seq,
        sensor: SensorData::BeamBreak,
    })
}

#[tokio::test]
async fn append_single_returns_offset_zero() {
    let mut log = MemLog::new();
    let offset = log
        .append("test-producer", &[det(0)])
        .await
        .expect("append");
    assert_eq!(offset, Offset::new(0));
}

#[tokio::test]
async fn append_batch_returns_last_offset() {
    let mut log = MemLog::new();
    let offset = log
        .append("test-producer", &[det(0), det(1), det(2)])
        .await
        .expect("append");
    assert_eq!(offset, Offset::new(2));
}

#[tokio::test]
async fn append_empty_rejected_as_invalid_input() {
    let mut log = MemLog::new();
    let err = log
        .append("test-producer", &[])
        .await
        .expect_err("empty append must error");
    assert!(matches!(
        err,
        port_out_event_log::StorageError::InvalidInput(_)
    ));
}

#[tokio::test]
async fn read_range_returns_entries_in_order() {
    let mut log = MemLog::new();
    log.append("test-producer", &[det(0), det(1), det(2)])
        .await
        .unwrap();
    let entries = log
        .read_range(Offset::new(0), Some(Offset::new(3)))
        .await
        .unwrap();
    assert_eq!(entries.len(), 3);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.offset.as_u64(), i as u64);
        assert_eq!(
            e.producer_id, "test-producer",
            "producer_id must be carried through to read_range"
        );
    }
}

#[tokio::test]
async fn append_rejects_empty_producer_id() {
    let mut log = MemLog::new();
    let err = log
        .append("", &[det(0)])
        .await
        .expect_err("empty producer_id must error");
    assert!(matches!(
        err,
        port_out_event_log::StorageError::InvalidInput(_)
    ));
}

#[tokio::test]
async fn latest_and_earliest_offsets_reflect_state() {
    let mut log = MemLog::new();
    assert!(log.latest_offset().await.unwrap().is_none());
    assert!(log.earliest_offset().await.unwrap().is_none());
    log.append("test-producer", &[det(0), det(1)])
        .await
        .unwrap();
    assert_eq!(log.latest_offset().await.unwrap(), Some(Offset::new(1)));
    assert_eq!(log.earliest_offset().await.unwrap(), Some(Offset::new(0)));
}

#[tokio::test]
async fn subscribe_delivers_backfill_and_live() {
    let mut log = MemLog::new();
    log.append("test-producer", &[det(0)]).await.unwrap();
    let mut sub = log.subscribe(Offset::new(0)).await.unwrap();

    // Backfill: first entry already on disk.
    let first = sub.next_entry().await.expect("entry").expect("ok");
    assert_eq!(first.offset, Offset::new(0));

    // Live: append after subscribe must surface.
    log.append("test-producer", &[det(1)]).await.unwrap();
    let second = sub.next_entry().await.expect("entry").expect("ok");
    assert_eq!(second.offset, Offset::new(1));

    sub.close().await.unwrap();
}

#[tokio::test]
async fn distinct_producer_ids_are_preserved_per_batch() {
    // The contract: `producer_id` belongs to each entry (not each
    // segment, not each session, not the log as a whole). A backend
    // that stored producer_id only once per segment, or coalesced
    // consecutive appends with the same producer_id, would still pass
    // the basic round-trip in `read_range_returns_entries_in_order`.
    // This interleaved scenario forces the field to travel per-entry.
    //
    // Required for the runtime's sequence-gate restart resume: on
    // startup, `seed_from_log` walks every retained Detection and
    // keys the gate by `(producer_id, detector_id)`. A backend that
    // dropped producer_id distinctions would silently break that
    // resume path.
    let mut log = MemLog::new();
    log.append("alpha", &[det(0)]).await.unwrap();
    log.append("beta", &[det(1), det(2)]).await.unwrap();
    log.append("alpha", &[det(3)]).await.unwrap();

    let entries = log.read_range(Offset::new(0), None).await.unwrap();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0].producer_id, "alpha", "offset 0 was alpha's");
    assert_eq!(
        entries[1].producer_id, "beta",
        "offset 1 was beta's first event"
    );
    assert_eq!(
        entries[2].producer_id, "beta",
        "offset 2 was beta's second event in the same batch"
    );
    assert_eq!(
        entries[3].producer_id, "alpha",
        "offset 3 was alpha's second batch"
    );
}

#[tokio::test]
async fn subscribe_carries_producer_id_in_backfill_and_live() {
    // Same producer_id-per-entry contract as the read_range path, but
    // exercised through subscribe so a backend can't satisfy one path
    // and quietly miss the other. Backfill (entry already on disk
    // before subscribe) and live (entry appended after subscribe) are
    // separate code paths in `MemLog` and `SegmentLog`, so both are
    // verified.
    let mut log = MemLog::new();
    log.append("alpha", &[det(0)]).await.unwrap();

    let mut sub = log.subscribe(Offset::new(0)).await.unwrap();

    let backfilled = sub.next_entry().await.expect("backfill entry").expect("ok");
    assert_eq!(backfilled.offset, Offset::new(0));
    assert_eq!(
        backfilled.producer_id, "alpha",
        "backfill must carry producer_id"
    );

    log.append("beta", &[det(1)]).await.unwrap();
    let live = sub.next_entry().await.expect("live entry").expect("ok");
    assert_eq!(live.offset, Offset::new(1));
    assert_eq!(
        live.producer_id, "beta",
        "live delivery must carry producer_id"
    );

    sub.close().await.unwrap();
}

#[tokio::test]
async fn event_log_is_dyn_safe() {
    // Compile-time evidence that any backend can be boxed behind the trait.
    let mut log: Box<dyn EventLog> = Box::new(MemLog::new());
    assert!(log.latest_offset().await.unwrap().is_none());
    log.append("test-producer", &[det(0)]).await.unwrap();
    assert_eq!(log.latest_offset().await.unwrap(), Some(Offset::new(0)));
}
