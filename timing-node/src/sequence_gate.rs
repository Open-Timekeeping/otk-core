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
//!   The pipeline drops the event without persisting. Producers may safely
//!   re-send after a reconnect without polluting the log.
//! - **Gap** (> high-water + 1): accepted but reported as
//!   [`GateDecision::Gap`] so the runtime can log/meter it. The producer is
//!   responsible for not gapping; the gate observes.
//!
//! # Restart semantics
//!
//! The current implementation holds the high-water map in memory only;
//! restarting the runtime resets it. Persisting across restart (so a producer
//! that restarts in parallel still resumes idempotently) is tracked as a
//! follow-up — see `spec/open-questions.md`.

use std::collections::HashMap;
use std::sync::Mutex;

use event_model::{Detection, DetectorId};

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
            Some(hw) if seq <= hw => GateDecision::Duplicate { high_water: hw, got: seq },
            Some(hw) => {
                if seq == hw + 1 {
                    GateDecision::Advance { previous_high_water: hw }
                } else {
                    GateDecision::Gap { expected: hw + 1, got: seq }
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
        self.high_water.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
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
        assert!(matches!(d, GateDecision::Advance { previous_high_water: 5 }));
    }

    #[test]
    fn duplicate_sequence_rejected() {
        let gate = SequenceGate::new();
        gate.check("p", &det("loop-1", 5));
        let d = gate.check("p", &det("loop-1", 5));
        assert!(matches!(d, GateDecision::Duplicate { high_water: 5, got: 5 }));
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
        assert!(matches!(d, GateDecision::Gap { expected: 6, got: 9 }));
        assert!(d.persist());
    }

    #[test]
    fn separate_detectors_have_independent_high_water() {
        let gate = SequenceGate::new();
        gate.check("p", &det("loop-1", 5));
        gate.check("p", &det("loop-2", 1));
        // loop-2's gate is fresh; this is its second detection, expecting Advance.
        let d = gate.check("p", &det("loop-2", 2));
        assert!(matches!(d, GateDecision::Advance { previous_high_water: 1 }));
    }

    #[test]
    fn separate_producers_have_independent_high_water() {
        let gate = SequenceGate::new();
        gate.check("p-a", &det("loop-1", 100));
        // Same detector_id, different producer: must accept as fresh.
        assert_eq!(gate.check("p-b", &det("loop-1", 5)), GateDecision::Accept);
    }
}
