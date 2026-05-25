//! Per-`(producer_id, detector_id)` sequence-number high-water enforcement.
//!
//! The spec promises: detector adapters are responsible for monotonically
//! increasing `sequence_number` per detector across the producer-runtime link,
//! with no gaps and no duplicates across reconnects. The runtime enforces the
//! "no duplicates" half (idempotent storage) and observes the "no gaps" half
//! (operator visibility, metric, log).
//!
//! # Behaviour
//!
//! - **First detection** for a `(producer, detector)` pair: accepted; the gate
//!   seeds its high-water mark at that sequence number.
//! - **Strictly increasing** sequence: accepted; high-water advances.
//! - **Duplicate** (≤ high-water): rejected as [`GateDecision::Duplicate`].
//!   The service drops the event without persisting. Producers may safely
//!   re-send after a reconnect without polluting the log.
//! - **Gap** (> high-water + 1): accepted but reported as
//!   [`GateDecision::Gap`] so the runtime can log/meter it. The producer is
//!   responsible for not gapping; the gate observes.
//!
//! # Restart semantics
//!
//! On startup, [`seed_from_log`] walks the persisted event log and rebuilds
//! the per-`(producer_id, detector_id)` high-water map from every stored
//! Detection. After seeding, a producer reconnecting with the same
//! `producer_id` after a node restart sees the same idempotence guarantee
//! it had before the restart: a previously-acknowledged sequence number is
//! rejected as a duplicate, not silently re-persisted.

use std::collections::HashMap;
use std::sync::Mutex;

use event_model::{Detection, DetectorId, OtkEvent};
use tracing::{debug, info};

use crate::ports::outbound::{EventLog, StorageError};

/// Outcome of checking a single detection against the gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateDecision {
    /// First detection for this `(producer, detector)`. Persist normally.
    Accept,
    /// Strictly-greater sequence than previously seen. Persist normally.
    Advance { previous_high_water: u64 },
    /// The producer skipped at least one sequence number. The detection is
    /// still accepted, but the runtime should log + meter the gap.
    Gap { expected: u64, got: u64 },
    /// Sequence number ≤ previously-seen high water. Drop the detection.
    Duplicate { high_water: u64, got: u64 },
}

impl GateDecision {
    /// True if the detection should be persisted.
    pub fn persist(&self) -> bool {
        !matches!(self, GateDecision::Duplicate { .. })
    }
}

/// In-memory per-`(producer, detector)` sequence-number high-water gate.
///
/// # Lookup-allocation note
///
/// `peek` / `commit` build a `(String, DetectorId)` key on every call to
/// look up the high-water mark. That's one short-String allocation per
/// detection on the hot ingest path. At realistic timing-fabric rates
/// (low thousands of detections per second peak across an entire node)
/// the allocator pressure is well under 1 MB/s and below the noise
/// floor of the storage append's allocator cost. If profiling ever
/// shows the gate dominating allocator time, the fix is one of:
///
/// - Switch the underlying map to `hashbrown::HashMap` and use
///   `raw_entry_mut().from_hash(...)` to look up by a borrowed
///   `(&str, &DetectorId)` without allocating the owned tuple.
/// - Change the key to a single interned id allocated once per
///   `(producer, detector)` pair the first time it's seen, then look
///   up by that.
///
/// Either change is invisible to callers; the current `(String,
/// DetectorId)` shape is kept for clarity until a benchmark says
/// otherwise.
#[derive(Debug, Default)]
pub struct SequenceGate {
    /// Maps `(producer_id, detector_id)` to the highest accepted sequence number.
    high_water: Mutex<HashMap<(String, DetectorId), u64>>,
}

impl SequenceGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compute what the gate WOULD return without mutating state.
    ///
    /// Use this when the call site needs to drive a downstream action
    /// (e.g. storage append) that can fail: peek first, do the work,
    /// then call [`commit`](Self::commit) only on success. The previous
    /// `check` API mutated state up front, which meant an append failure
    /// after `check` left the high-water advanced and any producer retry
    /// of the same sequence was silently treated as a duplicate, losing
    /// the event permanently.
    ///
    /// `producer_id` is the producer the detection arrived from (typically
    /// `IngestSession::producer_id()`). It scopes the gate so two different
    /// producers can publish events for the same detector without colliding;
    /// in practice the deployment topology forbids this but the gate refuses
    /// to assume it.
    pub fn peek(&self, producer_id: &str, detection: &Detection) -> GateDecision {
        let key = (producer_id.to_string(), detection.detector_id.clone());
        let seq = detection.sequence_number;
        let map = self.high_water.lock().unwrap_or_else(|e| e.into_inner());
        match map.get(&key).copied() {
            None => GateDecision::Accept,
            Some(hw) if seq <= hw => GateDecision::Duplicate {
                high_water: hw,
                got: seq,
            },
            Some(hw) => {
                if seq == hw + 1 {
                    GateDecision::Advance {
                        previous_high_water: hw,
                    }
                } else {
                    GateDecision::Gap {
                        expected: hw + 1,
                        got: seq,
                    }
                }
            }
        }
    }

    /// Advance the high-water mark for `(producer_id, detection.detector_id)`
    /// to `detection.sequence_number`. Call only after the corresponding
    /// detection has been durably committed (storage append returned `Ok`).
    ///
    /// No-op if the new sequence is not strictly greater than the current
    /// high-water (e.g. a Duplicate decision was committed in error).
    pub fn commit(&self, producer_id: &str, detection: &Detection) {
        let key = (producer_id.to_string(), detection.detector_id.clone());
        let seq = detection.sequence_number;
        let mut map = self.high_water.lock().unwrap_or_else(|e| e.into_inner());
        match map.get(&key).copied() {
            None => {
                map.insert(key, seq);
            }
            Some(hw) if seq > hw => {
                map.insert(key, seq);
            }
            Some(_) => {} // duplicate or out-of-order; don't advance
        }
    }

    /// Decide + commit in one shot. Retained for callers that don't need
    /// the peek/commit split (currently: tests and any direct consumer
    /// that doesn't gate on storage durability).
    pub fn check(&self, producer_id: &str, detection: &Detection) -> GateDecision {
        let decision = self.peek(producer_id, detection);
        if decision.persist() {
            self.commit(producer_id, detection);
        }
        decision
    }

    /// Reset the gate's state for tests.
    #[cfg(test)]
    pub fn reset(&self) {
        self.high_water
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

/// Replay every retained Detection in `log` through `gate.commit` so the
/// gate's per-`(producer_id, detector_id)` high-water marks reflect what
/// was actually persisted before this process started.
///
/// Returns the total number of entries scanned (Detections plus everything
/// else; the count is for logging, not correctness).
///
/// Call from the composition root after the log is opened and before any
/// ingest listener begins accepting connections. Without this seed, a
/// producer reconnecting after a node restart with the same `producer_id`
/// could replay sequences and have them silently re-persisted, defeating
/// the gate's idempotence guarantee.
///
/// # Memory cost
///
/// This implementation calls `read_range(earliest, None)` and so
/// materializes every retained entry into memory at once. That mirrors
/// the documented contract of [`EventLog::read_range`] and is acceptable
/// for v0 (a node with retention bounded to one race weekend holds at
/// most a few hundred thousand entries). A paginated variant is tracked
/// for the storage-layer follow-ups.
///
/// Non-Detection events (Crossing, Health, TimebaseStatus, Metadata) are
/// ignored: the gate is detection-keyed and nothing else carries the
/// `(producer_id, detector_id, sequence_number)` triple it needs.
pub async fn seed_from_log(
    gate: &SequenceGate,
    log: &mut dyn EventLog,
) -> Result<usize, StorageError> {
    let earliest = match log.earliest_offset().await? {
        Some(o) => o,
        None => {
            debug!("sequence_gate: log is empty; nothing to seed");
            return Ok(0);
        }
    };

    // `to = None` reads through the latest retained offset.
    let entries = log.read_range(earliest, None).await?;
    let scanned = entries.len();
    let mut detections_seeded = 0usize;

    for entry in entries {
        if let OtkEvent::Detection(det) = entry.event {
            gate.commit(&entry.producer_id, &det);
            detections_seeded += 1;
        }
    }

    info!(
        scanned,
        detections_seeded,
        from_offset = earliest.as_u64(),
        "sequence_gate: seeded from event log"
    );
    Ok(scanned)
}

/// Type-erased version of [`seed_from_log`] for the common case where
/// the caller holds the log as `&mut Box<dyn EventLog>` (the runtime's
/// composition shape). Forwards to [`seed_from_log`] via a single
/// reborrow so callers don't have to write the auto-deref incantation
/// at the call site.
pub async fn seed_from_log_box(
    gate: &SequenceGate,
    log: &mut Box<dyn EventLog>,
) -> Result<usize, StorageError> {
    seed_from_log(gate, log.as_mut()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_model::{
        DetectionId, SensorData, SourceAttestation, TimebaseId, TimestampingMethod, TimingPointId,
    };

    fn det(detector_id: &str, seq: u64) -> Detection {
        Detection {
            detection_id: DetectionId::new(format!("d-{seq}")),
            detector_id: DetectorId::new(detector_id),
            timing_point_id: TimingPointId::new("tp"),
            subject_id: None,
            detected_at_ns: 0,
            detected_at_uncertainty_ns: None,
            received_at_ns: None,
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            timebase_id: TimebaseId::new("tb"),
            source_attestation: SourceAttestation::RuntimeDiscovered,
            sequence_number: seq,
            sensor: SensorData::BeamBreak,
        }
    }

    #[test]
    fn first_detection_accepted() {
        let gate = SequenceGate::new();
        assert_eq!(gate.check("p", &det("loop-1", 5)), GateDecision::Accept);
    }

    #[test]
    fn strictly_increasing_sequence_advances() {
        let gate = SequenceGate::new();
        gate.check("p", &det("loop-1", 5));
        let d = gate.check("p", &det("loop-1", 6));
        assert!(matches!(
            d,
            GateDecision::Advance {
                previous_high_water: 5
            }
        ));
    }

    #[test]
    fn duplicate_sequence_rejected() {
        let gate = SequenceGate::new();
        gate.check("p", &det("loop-1", 5));
        let d = gate.check("p", &det("loop-1", 5));
        assert!(matches!(
            d,
            GateDecision::Duplicate {
                high_water: 5,
                got: 5
            }
        ));
        assert!(!d.persist());
    }

    #[test]
    fn older_sequence_rejected() {
        let gate = SequenceGate::new();
        gate.check("p", &det("loop-1", 10));
        let d = gate.check("p", &det("loop-1", 7));
        assert!(matches!(d, GateDecision::Duplicate { .. }));
    }

    #[test]
    fn gap_observed_but_accepted() {
        let gate = SequenceGate::new();
        gate.check("p", &det("loop-1", 5));
        let d = gate.check("p", &det("loop-1", 9));
        assert!(matches!(
            d,
            GateDecision::Gap {
                expected: 6,
                got: 9
            }
        ));
        assert!(d.persist());
    }

    #[test]
    fn separate_detectors_have_independent_high_water() {
        let gate = SequenceGate::new();
        gate.check("p", &det("loop-1", 5));
        gate.check("p", &det("loop-2", 1));
        // loop-2's gate is fresh; this is its second detection, expecting Advance.
        let d = gate.check("p", &det("loop-2", 2));
        assert!(matches!(
            d,
            GateDecision::Advance {
                previous_high_water: 1
            }
        ));
    }

    #[test]
    fn separate_producers_have_independent_high_water() {
        let gate = SequenceGate::new();
        gate.check("p-a", &det("loop-1", 100));
        // Same detector_id, different producer: must accept as fresh.
        assert_eq!(gate.check("p-b", &det("loop-1", 5)), GateDecision::Accept);
    }

    // ── seed_from_log ─────────────────────────────────────────────────────
    //
    // These tests exercise `seed_from_log` against the in-memory
    // [`crate::testing::MockEventLog`]. The end-to-end "real backend +
    // restart" behaviour is covered by `restart_resume_test` in
    // `timing-node`; see the `testing` module's doc comment for why we
    // don't dev-depend the real segment-log adapter here.

    use event_model::OtkEvent;

    use crate::testing::MockEventLog;

    #[tokio::test]
    async fn seed_from_empty_log_is_noop() {
        let mut log = MockEventLog::new();
        let gate = SequenceGate::new();
        let scanned = seed_from_log(&gate, &mut log).await.unwrap();
        assert_eq!(scanned, 0);
        // Fresh gate behaviour is unchanged.
        assert_eq!(gate.check("p", &det("loop-1", 1)), GateDecision::Accept);
    }

    #[tokio::test]
    async fn seed_rebuilds_high_water_from_persisted_detections() {
        let mut log = MockEventLog::new();

        // Producer "p" wrote sequences 1, 2, 3 for loop-1; producer "q"
        // wrote sequence 5 for loop-2. After a restart the gate must
        // know about both keys with the correct high-water marks.
        log.append("p", &[OtkEvent::Detection(det("loop-1", 1))])
            .await
            .unwrap();
        log.append("p", &[OtkEvent::Detection(det("loop-1", 2))])
            .await
            .unwrap();
        log.append("p", &[OtkEvent::Detection(det("loop-1", 3))])
            .await
            .unwrap();
        log.append("q", &[OtkEvent::Detection(det("loop-2", 5))])
            .await
            .unwrap();

        let gate = SequenceGate::new();
        let scanned = seed_from_log(&gate, &mut log).await.unwrap();
        assert_eq!(scanned, 4);

        // Now drive the gate as if the producer reconnected after restart.
        // Replays of any sequence <= the persisted high water must be
        // rejected as duplicates; the next sequence must Advance.
        assert!(matches!(
            gate.check("p", &det("loop-1", 3)),
            GateDecision::Duplicate { high_water: 3, .. }
        ));
        assert!(matches!(
            gate.check("p", &det("loop-1", 4)),
            GateDecision::Advance {
                previous_high_water: 3
            }
        ));
        assert!(matches!(
            gate.check("q", &det("loop-2", 5)),
            GateDecision::Duplicate { high_water: 5, .. }
        ));
    }

    #[tokio::test]
    async fn seed_ignores_non_detection_events() {
        use event_model::{DetectorHealthEvent, DetectorHealthStatus};

        let mut log = MockEventLog::new();

        log.append(
            "p",
            &[OtkEvent::DetectorHealth(DetectorHealthEvent {
                detector_id: DetectorId::new("loop-1"),
                reported_at_ns: 0,
                status: DetectorHealthStatus::Healthy,
                message: None,
            })],
        )
        .await
        .unwrap();

        let gate = SequenceGate::new();
        let scanned = seed_from_log(&gate, &mut log).await.unwrap();
        // Scanned counts every entry, including the health event.
        assert_eq!(scanned, 1);
        // But the gate state is untouched: the next Detection is fresh.
        assert_eq!(gate.check("p", &det("loop-1", 1)), GateDecision::Accept);
    }
}
