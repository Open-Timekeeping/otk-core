use minicbor::{Decode, Encode};

use crate::ids::{DetectionId, DetectorId, OperatorId, SubjectId, TimebaseId, TimingPointId};
use crate::timestamp::{SourceAttestation, TimestampingMethod};

/// A single canonical timing observation. The one event shape used for all resolution levels.
///
/// The stream this event is published on carries the semantic resolution:
/// - raw stream: one event per sensor pulse (if the adapter emits raw signals)
/// - detections stream: one event per passage, firmware or adapter processed
/// - processed stream: timing-core output, possibly consolidated across detectors
///
/// Sensor-specific metadata lives in the `sensor` field. Only fields relevant to the
/// sensor type are populated; no spurious nullables appear in the common fields.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Detection {
    #[n(0)]
    pub detection_id: DetectionId,

    #[n(1)]
    pub detector_id: DetectorId,

    #[n(2)]
    pub timing_point_id: TimingPointId,

    /// The subject observed (transponder ID, bib number, vehicle ID, etc.).
    /// `None` when the subject is unknown or not applicable (e.g. beam break with no ID).
    #[n(3)]
    pub subject_id: Option<SubjectId>,

    /// When the physical event occurred, in nanoseconds since the Unix epoch,
    /// referenced to `timebase_id`.
    #[n(4)]
    pub detected_at_ns: u64,

    /// Estimated error bound on `detected_at_ns`, in nanoseconds. `None` means unknown,
    /// not zero. Consumers must treat `None` as "unbounded uncertainty".
    #[n(5)]
    pub detected_at_uncertainty_ns: Option<u64>,

    /// When the adapter received or processed the event. Always later than `detected_at_ns`.
    /// Useful for latency analysis; never used in timing calculations.
    #[n(6)]
    pub received_at_ns: Option<u64>,

    /// How `detected_at_ns` was produced. Required; honest reporting is non-negotiable.
    #[n(7)]
    pub timestamping_method: TimestampingMethod,

    /// The upstream physical time reference `detected_at_ns` is disciplined against.
    #[n(8)]
    pub timebase_id: TimebaseId,

    /// Whether the timebase identity was discovered at runtime or asserted by operator config.
    #[n(9)]
    pub source_attestation: SourceAttestation,

    /// Monotonically increasing counter per detector adapter. Consumers use this to detect
    /// gaps, duplicates, and reorders without relying on timestamps.
    #[n(10)]
    pub sequence_number: u64,

    /// Sensor-specific fields. Does not affect the common fields above.
    #[n(11)]
    pub sensor: SensorData,
}

/// Sensor-specific metadata for a `Detection`. Each variant carries exactly the fields
/// that make sense for that sensor type; no variant imposes nullables on another.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SensorData {
    /// Inductive loop with active transponder. Common in karting, motorsport, cycling.
    #[n(0)]
    LoopTransponder {
        /// Signal strength at this detection. On a raw stream: current pulse level.
        /// On a detections/processed stream: peak level across grouped pulses.
        #[n(0)]
        rssi_dbm: Option<i16>,

        /// Number of transponder pulses grouped into this detection.
        /// Populated on detections/processed streams; `None` on raw streams.
        #[n(1)]
        pulse_count: Option<u32>,
    },

    /// Beam break gate (light barrier, infrared gate, laser trip).
    /// Instantaneous event; no signal strength or pulse grouping metadata.
    #[n(1)]
    BeamBreak,

    /// Manually triggered by an operator (button, keyboard shortcut, remote trigger, etc.).
    #[n(2)]
    Manual {
        /// Identity of the operator who triggered the event, if known.
        #[n(0)]
        operator_id: Option<OperatorId>,
    },
}

/// Common field accessors for code that handles any `Detection` without caring about
/// the sensor type. Implemented directly on `Detection`; no trait objects required.
impl Detection {
    pub fn detector_id(&self) -> &DetectorId {
        &self.detector_id
    }

    pub fn timing_point_id(&self) -> &TimingPointId {
        &self.timing_point_id
    }

    pub fn detected_at_ns(&self) -> u64 {
        self.detected_at_ns
    }

    pub fn timestamping_method(&self) -> TimestampingMethod {
        self.timestamping_method
    }

    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{DetectionId, DetectorId, OperatorId, TimebaseId, TimingPointId};
    use crate::timestamp::{SourceAttestation, TimestampingMethod};

    fn base_detection(sensor: SensorData) -> Detection {
        Detection {
            detection_id: DetectionId::new("det-1"),
            detector_id: DetectorId::new("loop-a"),
            timing_point_id: TimingPointId::new("tp-finish"),
            subject_id: None,
            detected_at_ns: 1_700_000_000_000_000_000,
            detected_at_uncertainty_ns: Some(500),
            received_at_ns: None,
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            timebase_id: TimebaseId::new("ptp-gm-1"),
            source_attestation: SourceAttestation::RuntimeDiscovered,
            sequence_number: 42,
            sensor,
        }
    }

    fn cbor_round_trip(d: &Detection) -> Detection {
        let encoded = minicbor::to_vec(d).expect("encode failed");
        minicbor::decode(&encoded).expect("decode failed")
    }

    #[test]
    fn round_trip_loop_transponder() {
        let d = base_detection(SensorData::LoopTransponder { rssi_dbm: Some(-72), pulse_count: Some(3) });
        assert_eq!(d, cbor_round_trip(&d));
    }

    #[test]
    fn round_trip_beam_break() {
        let d = base_detection(SensorData::BeamBreak);
        assert_eq!(d, cbor_round_trip(&d));
    }

    #[test]
    fn round_trip_manual() {
        let d = base_detection(SensorData::Manual { operator_id: Some(OperatorId::new("alice")) });
        assert_eq!(d, cbor_round_trip(&d));
    }

    #[test]
    fn accessors_return_correct_values() {
        let d = base_detection(SensorData::BeamBreak);
        assert_eq!(d.detector_id().as_str(), "loop-a");
        assert_eq!(d.timing_point_id().as_str(), "tp-finish");
        assert_eq!(d.detected_at_ns(), 1_700_000_000_000_000_000);
        assert_eq!(d.timestamping_method(), TimestampingMethod::HardwareEventCapture);
        assert_eq!(d.sequence_number(), 42);
    }
}
