use std::collections::HashMap;

use event_model::{Detection, SensorData, SubjectId, TimingPointId};

use crate::config::ProcessorConfig;
use crate::crossing::{Crossing, CrossingId};

type GroupKey = (TimingPointId, SubjectId);

struct PendingGroup {
    detections: Vec<Detection>,
}

impl PendingGroup {
    /// Returns the `detected_at_ns` of the first detection by *arrival order*.
    ///
    /// This is the window anchor used in `push_detection`. Out-of-order arrival
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
/// Call [`push_detection`] for each incoming detection and handle any crossings it returns.
/// Call [`flush`] at the end of a session (or to force-emit any held-back crossings).
///
/// [`push_detection`]: CrossingProcessor::push_detection
/// [`flush`]: CrossingProcessor::flush
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

    /// Process one detection. Returns crossings that were committed as a result.
    ///
    /// A crossing is returned when an incoming detection falls outside the grouping window
    /// of the existing pending group for the same key, causing the old group to be flushed.
    /// Anonymous detections (`subject_id = None`) are committed immediately.
    pub fn push_detection(&mut self, det: Detection) -> Vec<Crossing> {
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
    /// `push_detection` calls returns an empty `Vec`.
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
        let emitted = p.push_detection(loop_det("d1", "tp-a", Some("s1"), 1000, None));
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
        p.push_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));
        let emitted = p.push_detection(loop_det("d2", "tp-a", Some("s1"), 1_500_000_000, None));
        assert!(emitted.is_empty(), "should not commit mid-window");
        let crossings = p.flush();
        assert_eq!(crossings.len(), 1);
        assert_eq!(crossings[0].detection_ids.len(), 2);
    }

    #[test]
    fn two_detections_outside_window_separate() {
        let mut p = processor();
        p.push_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));
        // Arrives 2 seconds later, outside the 1-second window
        let emitted = p.push_detection(loop_det("d2", "tp-a", Some("s1"), 3_000_000_000, None));
        assert_eq!(emitted.len(), 1, "old group should have been committed");
        assert_eq!(emitted[0].detection_ids[0].as_str(), "d1");
        let crossings = p.flush();
        assert_eq!(crossings.len(), 1);
        assert_eq!(crossings[0].detection_ids[0].as_str(), "d2");
    }

    #[test]
    fn different_subjects_not_merged() {
        let mut p = processor();
        p.push_detection(loop_det("d1", "tp-a", Some("s1"), 1000, None));
        p.push_detection(loop_det("d2", "tp-a", Some("s2"), 1001, None));
        let crossings = p.flush();
        assert_eq!(crossings.len(), 2);
    }

    #[test]
    fn different_timing_points_not_merged() {
        let mut p = processor();
        p.push_detection(loop_det("d1", "tp-a", Some("s1"), 1000, None));
        p.push_detection(loop_det("d2", "tp-b", Some("s1"), 1001, None));
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
        let c1 = p.push_detection(beam_det("d1", "tp-a", None, 1000));
        let c2 = p.push_detection(beam_det("d2", "tp-a", None, 1001));
        assert_eq!(c1.len(), 1);
        assert_eq!(c2.len(), 1);
        assert!(p.flush().is_empty());
    }

    #[test]
    fn peak_rssi_selected_as_crossing_time() {
        let mut p = processor();
        // Weak signal first, strong signal second
        p.push_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, Some(-80)));
        p.push_detection(loop_det("d2", "tp-a", Some("s1"), 1_100_000_000, Some(-50)));
        let crossings = p.flush();
        assert_eq!(crossings.len(), 1);
        // Should use the -50 dBm (stronger) detection's timestamp
        assert_eq!(crossings[0].crossed_at_ns, 1_100_000_000);
        assert_eq!(crossings[0].detection_ids.len(), 2);
    }

    #[test]
    fn no_rssi_earliest_timestamp_selected() {
        let mut p = processor();
        p.push_detection(loop_det("d1", "tp-a", Some("s1"), 1_000_000_000, None));
        p.push_detection(loop_det("d2", "tp-a", Some("s1"), 1_100_000_000, None));
        let crossings = p.flush();
        assert_eq!(crossings[0].crossed_at_ns, 1_000_000_000);
    }

    #[test]
    fn flush_clears_pending_state() {
        let mut p = processor();
        p.push_detection(loop_det("d1", "tp-a", Some("s1"), 1000, None));
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
        p.push_detection(d1);
        p.push_detection(loop_det("d2", "tp-a", Some("s1"), 1_500_000_000, Some(-50)));
        let crossings = p.flush();
        let uncertainty = crossings[0].crossed_at_uncertainty_ns.unwrap();
        // span = 500ms; uncertainty must be at least the span
        assert!(
            uncertainty >= 500_000_000,
            "uncertainty {uncertainty} < span 500ms"
        );
    }
}
