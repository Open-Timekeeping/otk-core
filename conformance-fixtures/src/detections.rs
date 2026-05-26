//! [`Detection`] constructors for common physical-layer scenarios.
//!
//! These build typical-shape detections (loop transponder with /
//! without RSSI, beam break) parameterised by sequence number so
//! callers can compose monotonically-increasing streams.
//!
//! All constructors use `SourceAttestation::RuntimeDiscovered` and
//! `TimestampingMethod::HardwareEventCapture` unless noted; tests
//! exercising other provenance / method values should construct
//! [`Detection`] directly or extend this module with named
//! constructors.

use event_model::{
    Detection, DetectionId, DetectorId, SensorData, SourceAttestation, SubjectId, TimebaseId,
    TimestampingMethod, TimingPointId,
};

/// Wall-clock base used by every constructor here: 2023-11-14T22:13:20 UTC,
/// in nanoseconds since the Unix epoch. Picked so per-sequence offsets at
/// 1 ms granularity stay comfortably inside a `u64` for any realistic
/// stream length, and so test output reads as a recent timestamp at a
/// glance.
pub const FIXTURE_BASE_NS: u64 = 1_700_000_000_000_000_000;

/// Beam-break detection from a single anonymous detector ("loop-1" at
/// timing point "tp-start"). No `subject_id`, no RSSI. Timestamp is
/// `FIXTURE_BASE_NS + seq * 1_000_000_000` (one detection per second).
///
/// Suitable for the most common conformance scenario: a monotonic
/// stream of generic detections where the test cares about sequence
/// numbering and not the sensor specifics.
pub fn beam_break_at_loop(seq: u64) -> Detection {
    Detection {
        detection_id: DetectionId::new(format!("det-{seq}")),
        detector_id: DetectorId::new("loop-1"),
        timing_point_id: TimingPointId::new("tp-start"),
        subject_id: None,
        detected_at_ns: FIXTURE_BASE_NS + seq * 1_000_000_000,
        detected_at_uncertainty_ns: None,
        received_at_ns: None,
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("gps-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: seq,
        sensor: SensorData::BeamBreak,
    }
}

/// Loop-style RFID read with a known subject and RSSI value. Use for
/// tests that exercise peak-signal selection, subject grouping, or
/// RSSI-driven crossing semantics.
///
/// `SensorData::LoopTransponder` is the OTK variant name regardless of
/// whether the underlying hardware is an active transponder (typical in
/// motorsport) or a passive RFID tag (typical in running and similar
/// foot races). Both report a subject identifier and, often, an RSSI
/// value; the fixture stays domain-neutral on `subject` so the same
/// constructor seeds either kind of test.
///
/// Detector "loop-1" at timing point "tp-start"; subject "subject-N"
/// for the given `subject`. Timestamp derived as for
/// [`beam_break_at_loop`].
pub fn loop_read_with_rssi(seq: u64, subject: u32, rssi_dbm: i16) -> Detection {
    Detection {
        detection_id: DetectionId::new(format!("det-{seq}")),
        detector_id: DetectorId::new("loop-1"),
        timing_point_id: TimingPointId::new("tp-start"),
        subject_id: Some(SubjectId::new(format!("subject-{subject}"))),
        detected_at_ns: FIXTURE_BASE_NS + seq * 1_000_000_000,
        detected_at_uncertainty_ns: Some(50_000),
        received_at_ns: None,
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("gps-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: seq,
        sensor: SensorData::LoopTransponder {
            rssi_dbm: Some(rssi_dbm),
            pulse_count: Some(3),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn beam_break_sequence_monotonic() {
        let a = beam_break_at_loop(0);
        let b = beam_break_at_loop(1);
        assert_eq!(a.sequence_number, 0);
        assert_eq!(b.sequence_number, 1);
        assert!(b.detected_at_ns > a.detected_at_ns);
    }

    #[test]
    fn loop_read_carries_subject_and_rssi() {
        let d = loop_read_with_rssi(7, 42, -55);
        assert_eq!(d.subject_id.as_ref().unwrap().as_str(), "subject-42");
        match d.sensor {
            SensorData::LoopTransponder {
                rssi_dbm: Some(r), ..
            } => assert_eq!(r, -55),
            other => panic!("expected LoopTransponder with rssi, got {other:?}"),
        }
    }
}
