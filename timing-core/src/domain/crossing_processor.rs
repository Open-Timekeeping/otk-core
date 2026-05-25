use std::collections::HashMap;

use event_model::{Detection, SensorData, SubjectId, TimingPointId};

use super::crossing::{Crossing, CrossingId};
use super::processor_config::ProcessorConfig;

type GroupKey = (TimingPointId, SubjectId);

struct PendingGroup {
    detections: Vec<Detection>,
}

impl PendingGroup {
    /// Returns the `detected_at_ns` of the first detection by *arrival order*.
    ///
    /// This is the window anchor used in `commit_detection`. Out-of-order arrival
    /// (a detection with an earlier timestamp pushed into an already-started group)
    /// is not handled in v0; the processor assumes arrival order approximates
    /// timestamp order per `(timing_point_id, subject_id)`.
    fn first_detected_at_ns(&self) -> u64 {
        self.detections[0].detected_at_ns
    }

    fn commit(self) -> Crossing {
        let first_at = self
            .detections
            .iter()
            .map(|d| d.detected_at_ns)
            .min()
            .unwrap();
        let last_at = self
            .detections
            .iter()
            .map(|d| d.detected_at_ns)
            .max()
            .unwrap();
        let span = last_at.saturating_sub(first_at);

        let peak_idx = pick_peak_index(&self.detections);

        let crossed_at_ns = self.detections[peak_idx].detected_at_ns;
        let peak_uncertainty = self.detections[peak_idx].detected_at_uncertainty_ns;
        let timebase_id = self.detections[peak_idx].timebase_id.clone();
        let timestamping_method = self.detections[peak_idx].timestamping_method;
        let source_attestation = self.detections[peak_idx].source_attestation;

        let crossed_at_uncertainty_ns = match (peak_uncertainty, span) {
            (Some(u), s) if s > 0 => Some(u.max(s)),
            (Some(u), _) => Some(u),
            (None, s) if s > 0 => Some(s),
            (None, _) => None,
        };

        let timing_point_id = self.detections[0].timing_point_id.clone();
        let subject_id = self.detections[0].subject_id.clone();

        let detection_ids = self
            .detections
            .into_iter()
            .map(|d| d.detection_id)
            .collect();

        Crossing {
            crossing_id: CrossingId::new_random(),
            timing_point_id,
            subject_id,
            crossed_at_ns,
            crossed_at_uncertainty_ns,
            timebase_id,
            timestamping_method,
            source_attestation,
            detection_ids,
        }
    }
}

/// Choose the index of the "peak" detection in a group.
///
/// For `LoopTransponder` detections that have an `rssi_dbm` value: pick the highest RSSI
/// (strongest signal). For all other cases, pick the detection with the lowest
/// `detected_at_ns` (earliest physical timestamp).
fn pick_peak_index(detections: &[Detection]) -> usize {
    let has_rssi = detections.iter().any(|d| {
        matches!(
            d.sensor,
            SensorData::LoopTransponder {
                rssi_dbm: Some(_),
                ..
            }
        )
    });

    if has_rssi {
        detections
            .iter()
            .enumerate()
            .max_by_key(|(_, d)| {
                if let SensorData::LoopTransponder {
                    rssi_dbm: Some(rssi),
                    ..
                } = d.sensor
                {
                    rssi
                } else {
                    i16::MIN
                }
            })
            .map(|(i, _)| i)
            .unwrap()
    } else {
        detections
            .iter()
            .enumerate()
            .min_by_key(|(_, d)| d.detected_at_ns)
            .map(|(i, _)| i)
            .unwrap()
    }
}

/// Streaming processor that converts [`Detection`] events into [`Crossing`] events.
///
/// Detections from the same subject at the same timing point that arrive within
/// [`ProcessorConfig::grouping_window_ns`] of the first detection in the group are merged
/// into a single crossing. Detections with no `subject_id` are never grouped and produce a
/// crossing immediately.
///
/// # Peek / commit API
///
/// State-mutating work is split across two calls so that a caller (notably
/// the runtime's `NodePipeline`) can compute the crossings that *would* be
/// produced, attempt a downstream operation that may fail (a storage
/// append), and apply the state change only on success.
///
/// - [`peek_detection`](Self::peek_detection): pure. Returns the crossings
///   that [`commit_detection`](Self::commit_detection) would return for the
///   same detection, without mutating any pending state. Internally clones
///   the relevant pending group when an eviction would occur.
/// - [`commit_detection`](Self::commit_detection): mutates. Applies the same
///   grouping logic and returns the same crossings.
///
/// `peek` followed by `commit` is the safe pattern. Calling only `commit`
/// is correct when no downstream failure can leave the processor and the
/// caller's persistent state out of sync (tests, simple consumers).
///
/// [`flush`](Self::flush) commits all currently-pending groups at the end
/// of a session.
pub struct CrossingProcessor {
    config: ProcessorConfig,
    pending: HashMap<GroupKey, PendingGroup>,
}

impl CrossingProcessor {
    pub fn new(config: ProcessorConfig) -> Self {
        Self {
            config,
            pending: HashMap::new(),
        }
    }

    /// Compute the crossings that [`commit_detection`](Self::commit_detection)
    /// would return for `det`, without mutating any pending state.
    ///
    /// Mirrors the [`SequenceGate`-style peek/commit pattern] in the runtime:
    /// the caller uses this result to drive a downstream operation that may
    /// fail (e.g. a storage append), and only invokes `commit_detection`
    /// after the downstream operation succeeds. If the caller skipped the
    /// peek and went straight to `commit_detection`, an append failure
    /// would leave the processor's grouping window advanced past what was
    /// actually persisted, and a producer's retry of the same detection
    /// would observe a different crossing shape than the first attempt
    /// would have.
    ///
    /// Cost: when an eviction would occur (an existing pending group is
    /// about to be committed because the new detection falls outside its
    /// window), this method clones the group's accumulated detections so
    /// the same crossing can be computed without removing the group.
    /// Groups typically hold one to four detections, so the clone is
    /// cheap; the alternative (a borrowing commit helper on `PendingGroup`)
    /// duplicated the eviction logic and was harder to keep in sync with
    /// `commit`.
    ///
    /// [`SequenceGate`-style peek/commit pattern]: <https://crates.io/crates/sequence_gate>
    pub fn peek_detection(&self, det: &Detection) -> Vec<Crossing> {
        // Anonymous detections never group; they would commit immediately.
        let Some(subject_id) = det.subject_id.clone() else {
            return vec![PendingGroup {
                detections: vec![det.clone()],
            }
            .commit()];
        };

        let key = (det.timing_point_id.clone(), subject_id);

        let outside_window = self
            .pending
            .get(&key)
            .map(|g| {
                det.detected_at_ns.saturating_sub(g.first_detected_at_ns())
                    > self.config.grouping_window_ns
            })
            .unwrap_or(false);

        if outside_window {
            // Clone the pending group so commit() can compute the crossing
            // without removing the original.
            let group_clone = PendingGroup {
                detections: self.pending.get(&key).unwrap().detections.clone(),
            };
            vec![group_clone.commit()]
        } else {
            // Group continues or is newly created on commit: no crossing emitted yet.
            Vec::new()
        }
    }

    /// Process one detection, mutating internal state. Returns crossings that
    /// were committed as a result.
    ///
    /// A crossing is returned when an incoming detection falls outside the
    /// grouping window of the existing pending group for the same key,
    /// causing the old group to be flushed. Anonymous detections
    /// (`subject_id = None`) are committed immediately.
    ///
    /// In single-threaded use, the returned `Vec` is identical to what
    /// [`peek_detection`](Self::peek_detection) would have returned for the
    /// same detection immediately prior. The runtime's `NodePipeline`
    /// relies on this property: it uses `peek_detection` to build the
    /// storage batch and `commit_detection` to advance state after the
    /// append succeeds.
    pub fn commit_detection(&mut self, det: Detection) -> Vec<Crossing> {
        // Anonymous detections never group; commit immediately.
        let Some(subject_id) = det.subject_id.clone() else {
            return vec![PendingGroup {
                detections: vec![det],
            }
            .commit()];
        };

        let key = (det.timing_point_id.clone(), subject_id);

        let outside_window = self
            .pending
            .get(&key)
            .map(|g| {
                det.detected_at_ns.saturating_sub(g.first_detected_at_ns())
                    > self.config.grouping_window_ns
            })
            .unwrap_or(false);

        let mut committed = Vec::new();

        if outside_window {
            let old_group = self.pending.remove(&key).unwrap();
            committed.push(old_group.commit());
        }

        self.pending
            .entry(key)
            .or_insert_with(|| PendingGroup {
                detections: Vec::new(),
            })
            .detections
            .push(det);

        committed
    }

    /// Commit all pending groups and return their crossings.
    ///
    /// After this call the processor is empty. A second call with no intervening
    /// [`commit_detection`](Self::commit_detection) returns an empty `Vec`.
    pub fn flush(&mut self) -> Vec<Crossing> {
        self.pending
            .drain()
            .map(|(_, group)| group.commit())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use event_model::{
        DetectionId, DetectorId, SensorData, SourceAttestation, SubjectId, TimebaseId,
        TimestampingMethod, TimingPointId,
    };

    use super::*;

    fn det(
        id: &str,
        timing_point: &str,
        subject: Option<&str>,
        detected_at_ns: u64,
        sensor: SensorData,
    ) -> Detection {
        Detection {
            detection_id: DetectionId::new(id),
            detector_id: DetectorId::new("det-1"),
            timing_point_id: TimingPointId::new(timing_point),
            subject_id: subject.map(SubjectId::new),
            detected_at_ns,
            detected_at_uncertainty_ns: None,
            received_at_ns: None,
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            timebase_id: TimebaseId::new("tb-1"),
            source_attestation: SourceAttestation::RuntimeDiscovered,
            sequence_number: 0,
            sensor,
        }
    }

    fn loop_det(
        id: &str,
        tp: &str,
        subj: Option<&str>,
        at_ns: u64,
        rssi: Option<i16>,
    ) -> Detection {
        det(
            id,
            tp,
            subj,
            at_ns,
            SensorData::LoopTransponder {
                rssi_dbm: rssi,
                pulse_count: None,
            },
        )
    }

    fn beam_det(id: &str, tp: &str, subj: Option<&str>, at_ns: u64) -> Detection {
        det(id, tp, subj, at_ns, SensorData::BeamBreak)
    }

    fn processor() -> CrossingProcessor {
        CrossingProcessor::new(ProcessorConfig {
            grouping_window_ns: 1_000_000_000, // 1 second
        })
    }

    #[test]
    fn single_detection_becomes_crossing_on_flush() {
        let mut p = processor();
        let emitted = p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1000, None));
        assert!(emitted.is_empty());
        let crossings = p.flush();
        assert_eq!(crossings.len(), 1);
        assert_eq!(crossings[0].detection_ids.len(), 1);
        assert_eq!(crossings[0].timing_point_id.as_str(), "tp-a");
        assert_eq!(crossings[0].subject_id.as_ref().unwrap().as_str(), "s1");
    }

    #[test]
    fn two_detections_within_window_merged() {
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));
        let emitted = p.commit_detection(loop_det("d2", "tp-a", Some("s1"), 1_500_000_000, None));
        assert!(emitted.is_empty(), "should not commit mid-window");
        let crossings = p.flush();
        assert_eq!(crossings.len(), 1);
        assert_eq!(crossings[0].detection_ids.len(), 2);
    }

    #[test]
    fn two_detections_outside_window_separate() {
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));
        // Arrives 2 seconds later, outside the 1-second window
        let emitted = p.commit_detection(loop_det("d2", "tp-a", Some("s1"), 3_000_000_000, None));
        assert_eq!(emitted.len(), 1, "old group should have been committed");
        assert_eq!(emitted[0].detection_ids[0].as_str(), "d1");
        let crossings = p.flush();
        assert_eq!(crossings.len(), 1);
        assert_eq!(crossings[0].detection_ids[0].as_str(), "d2");
    }

    #[test]
    fn different_subjects_not_merged() {
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1000, None));
        p.commit_detection(loop_det("d2", "tp-a", Some("s2"), 1001, None));
        let crossings = p.flush();
        assert_eq!(crossings.len(), 2);
    }

    #[test]
    fn different_timing_points_not_merged() {
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1000, None));
        p.commit_detection(loop_det("d2", "tp-b", Some("s1"), 1001, None));
        let crossings = p.flush();
        assert_eq!(crossings.len(), 2);
        let points: Vec<&str> = crossings
            .iter()
            .map(|c| c.timing_point_id.as_str())
            .collect();
        assert!(points.contains(&"tp-a"));
        assert!(points.contains(&"tp-b"));
    }

    #[test]
    fn anonymous_detections_not_merged() {
        let mut p = processor();
        let c1 = p.commit_detection(beam_det("d1", "tp-a", None, 1000));
        let c2 = p.commit_detection(beam_det("d2", "tp-a", None, 1001));
        assert_eq!(c1.len(), 1);
        assert_eq!(c2.len(), 1);
        assert!(p.flush().is_empty());
    }

    #[test]
    fn peak_rssi_selected_as_crossing_time() {
        let mut p = processor();
        // Weak signal first, strong signal second
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, Some(-80)));
        p.commit_detection(loop_det("d2", "tp-a", Some("s1"), 1_100_000_000, Some(-50)));
        let crossings = p.flush();
        assert_eq!(crossings.len(), 1);
        // Should use the -50 dBm (stronger) detection's timestamp
        assert_eq!(crossings[0].crossed_at_ns, 1_100_000_000);
        assert_eq!(crossings[0].detection_ids.len(), 2);
    }

    #[test]
    fn no_rssi_earliest_timestamp_selected() {
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));
        p.commit_detection(loop_det("d2", "tp-a", Some("s1"), 1_100_000_000, None));
        let crossings = p.flush();
        assert_eq!(crossings[0].crossed_at_ns, 1_000_000_000);
    }

    #[test]
    fn flush_clears_pending_state() {
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1000, None));
        let first_flush = p.flush();
        assert_eq!(first_flush.len(), 1);
        let second_flush = p.flush();
        assert!(second_flush.is_empty());
    }

    #[test]
    fn uncertainty_widened_by_span() {
        let mut p = processor();
        // Two detections 500ms apart, first has 10ms uncertainty
        let mut d1 = loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, Some(-60));
        d1.detected_at_uncertainty_ns = Some(10_000_000); // 10ms
        p.commit_detection(d1);
        p.commit_detection(loop_det("d2", "tp-a", Some("s1"), 1_500_000_000, Some(-50)));
        let crossings = p.flush();
        let uncertainty = crossings[0].crossed_at_uncertainty_ns.unwrap();
        // span = 500ms; uncertainty must be at least the span
        assert!(
            uncertainty >= 500_000_000,
            "uncertainty {uncertainty} < span 500ms"
        );
    }

    // ── peek / commit semantics ────────────────────────────────────────

    /// Snapshot the processor's pending state in a form cheap to compare.
    /// We don't derive `Eq` on the internal map (it carries `Detection`
    /// values without `PartialEq`), so equality is checked indirectly by
    /// asking the processor for the crossings it would emit on flush.
    fn flush_clone(p: &CrossingProcessor) -> Vec<Crossing> {
        // CrossingId values are random per `commit`, so we can't compare
        // them; the relevant invariant for these tests is "same number of
        // crossings, same detection ids, same timing-point / subject".
        // ProcessorConfig is Copy, hence direct field access (clippy's
        // `clone_on_copy` lint would flag a `.clone()` here).
        let mut clone = CrossingProcessor::new(p.config);
        for (key, group) in &p.pending {
            clone.pending.insert(
                key.clone(),
                PendingGroup {
                    detections: group.detections.clone(),
                },
            );
        }
        clone.flush()
    }

    #[test]
    fn peek_does_not_mutate_state() {
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));
        let before = flush_clone(&p);

        // Peek a follow-up that would extend the existing group.
        let _ = p.peek_detection(&loop_det("d2", "tp-a", Some("s1"), 1_200_000_000, None));

        let after = flush_clone(&p);
        assert_eq!(
            before.len(),
            after.len(),
            "peek must not mutate pending state"
        );
        // Pending group should still hold exactly d1.
        assert_eq!(after[0].detection_ids.len(), 1);
        assert_eq!(after[0].detection_ids[0].as_str(), "d1");
    }

    #[test]
    fn peek_returns_same_crossings_as_commit_for_eviction() {
        // Same setup driven through peek then commit; both calls must
        // agree on what crossings are emitted.
        let mut p1 = processor();
        let mut p2 = processor();

        p1.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));
        p2.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));

        // Out-of-window arrival evicts the existing group.
        let next = loop_det("d2", "tp-a", Some("s1"), 3_000_000_000, None);

        let peeked = p1.peek_detection(&next);
        let committed = p2.commit_detection(next.clone());

        assert_eq!(peeked.len(), committed.len());
        assert_eq!(peeked.len(), 1);
        assert_eq!(peeked[0].detection_ids, committed[0].detection_ids);
        assert_eq!(peeked[0].timing_point_id, committed[0].timing_point_id);
        assert_eq!(peeked[0].subject_id, committed[0].subject_id);
        assert_eq!(peeked[0].crossed_at_ns, committed[0].crossed_at_ns);
    }

    #[test]
    fn peek_anonymous_returns_immediate_crossing_without_mutation() {
        let mut p = processor();
        // Peek alone must not change processor state.
        let peeked = p.peek_detection(&beam_det("d1", "tp-a", None, 1_000_000_000));
        assert_eq!(peeked.len(), 1);
        assert_eq!(peeked[0].detection_ids[0].as_str(), "d1");
        // Flush must still be empty (peek did not insert anything).
        assert!(p.flush().is_empty());
    }

    #[test]
    fn peek_within_window_returns_empty() {
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));
        // Inside the window: peek should return no crossings.
        let peeked = p.peek_detection(&loop_det("d2", "tp-a", Some("s1"), 1_200_000_000, None));
        assert!(peeked.is_empty());
    }

    #[test]
    fn peek_can_be_safely_skipped_then_called_again() {
        // Sanity: nothing about peek_detection makes it dangerous to
        // call multiple times in a row. Each call sees the same state.
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));

        let next = loop_det("d2", "tp-a", Some("s1"), 3_000_000_000, None);
        let first = p.peek_detection(&next);
        let second = p.peek_detection(&next);

        assert_eq!(first.len(), second.len());
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].detection_ids[0].as_str(), "d1");
    }

    #[test]
    fn discarded_peek_leaves_processor_unchanged_for_retry() {
        // The core safety property: callers can peek, choose not to
        // commit (e.g. a downstream append failed), and a later commit
        // of the same detection produces the same crossings the
        // discarded peek would have. This is what `NodePipeline` relies
        // on when a storage append fails partway through.
        let mut p = processor();
        p.commit_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));

        let next = loop_det("d2", "tp-a", Some("s1"), 3_000_000_000, None);

        let peeked = p.peek_detection(&next);
        // ... imagine a downstream failure here; the caller does NOT commit ...

        // Later retry of the same detection commits.
        let committed = p.commit_detection(next.clone());

        // The crossings emitted by the committed call match what the
        // discarded peek had shown.
        assert_eq!(peeked.len(), committed.len());
        assert_eq!(peeked[0].detection_ids, committed[0].detection_ids);
    }
}
