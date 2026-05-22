//! Event Model conformance: CBOR encode/decode round-trips.
//!
//! Every [`OtkEvent`] variant must survive a `minicbor::to_vec` →
//! `minicbor::decode` round-trip with byte-stable re-encoding. The Wire Protocol
//! layer wraps these values in envelopes and ships them across every supported
//! transport binding; if a variant doesn't round-trip cleanly, every transport
//! that carries it is broken.

use event_model::{
    AdapterCapabilities, AdapterMetadataEvent, CrossingEvent, CrossingId, Detection, DetectionId,
    DetectorHealthEvent, DetectorHealthStatus, DetectorId, OtkEvent, SensorData, SourceAttestation,
    StreamDescriptor, StreamId, StreamKind, SubjectId, SyncState, TimebaseId, TimebaseStatusEvent,
    TimestampingMethod, TimingPointId,
};

fn roundtrip(event: &OtkEvent) {
    let bytes = minicbor::to_vec(event).expect("encode");
    let decoded: OtkEvent = minicbor::decode(&bytes).expect("decode");
    let re_encoded = minicbor::to_vec(&decoded).expect("re-encode");
    assert_eq!(bytes, re_encoded, "CBOR is not stable across re-encode");
}

#[test]
fn detection_loop_transponder_roundtrip() {
    let event = OtkEvent::Detection(Detection {
        detection_id: DetectionId::new("det-1"),
        detector_id: DetectorId::new("loop-1"),
        timing_point_id: TimingPointId::new("tp-start"),
        subject_id: Some(SubjectId::new("bib-42")),
        detected_at_ns: 1_700_000_000_000_000_000,
        detected_at_uncertainty_ns: Some(50_000),
        received_at_ns: Some(1_700_000_000_000_000_100),
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        timebase_id: TimebaseId::new("gps-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        sequence_number: 1,
        sensor: SensorData::LoopTransponder {
            rssi_dbm: Some(-60),
            pulse_count: Some(3),
        },
    });
    roundtrip(&event);
}

#[test]
fn detection_beam_break_roundtrip() {
    let event = OtkEvent::Detection(Detection {
        detection_id: DetectionId::new("det-2"),
        detector_id: DetectorId::new("beam-1"),
        timing_point_id: TimingPointId::new("tp-mid"),
        subject_id: None,
        detected_at_ns: 1_700_000_000_500_000_000,
        detected_at_uncertainty_ns: None,
        received_at_ns: None,
        timestamping_method: TimestampingMethod::FirmwareTimerRead,
        timebase_id: TimebaseId::new("ptp-gm-1"),
        source_attestation: SourceAttestation::OperatorAsserted,
        sequence_number: 7,
        sensor: SensorData::BeamBreak,
    });
    roundtrip(&event);
}

#[test]
fn detector_health_healthy_roundtrip() {
    let event = OtkEvent::DetectorHealth(DetectorHealthEvent {
        detector_id: DetectorId::new("loop-1"),
        reported_at_ns: 1_700_000_000_000_000_000,
        status: DetectorHealthStatus::Healthy,
        message: Some("ok".into()),
    });
    roundtrip(&event);
}

#[test]
fn detector_health_degraded_roundtrip() {
    let event = OtkEvent::DetectorHealth(DetectorHealthEvent {
        detector_id: DetectorId::new("loop-1"),
        reported_at_ns: 1_700_000_000_000_000_000,
        status: DetectorHealthStatus::Degraded {
            reason: "low SNR".into(),
        },
        message: None,
    });
    roundtrip(&event);
}

#[test]
fn timebase_status_roundtrip() {
    for state in [
        SyncState::Locked,
        SyncState::Holdover,
        SyncState::FreeRun,
        SyncState::Unsynchronized,
        SyncState::Unknown,
    ] {
        let event = OtkEvent::TimebaseStatus(TimebaseStatusEvent {
            timebase_id: TimebaseId::new("gps-1"),
            reported_at_ns: 1_700_000_000_000_000_000,
            sync_state: state,
            uncertainty_ns: Some(1_000),
            source_attestation: SourceAttestation::RuntimeDiscovered,
        });
        roundtrip(&event);
    }
}

#[test]
fn adapter_metadata_roundtrip() {
    let event = OtkEvent::AdapterMetadata(AdapterMetadataEvent {
        detector_id: DetectorId::new("loop-1"),
        timing_point_id: TimingPointId::new("tp-start"),
        timebase_id: TimebaseId::new("gps-1"),
        source_attestation: SourceAttestation::RuntimeDiscovered,
        declared_at_ns: 1_700_000_000_000_000_000,
        capabilities: AdapterCapabilities {
            streams: vec![StreamDescriptor {
                stream_id: StreamId::new("loop-1/detections"),
                kind: StreamKind::Detections,
                detector_id: Some(DetectorId::new("loop-1")),
                timing_point_id: Some(TimingPointId::new("tp-start")),
            }],
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            declared_resolution_ns: Some(1),
        },
    });
    roundtrip(&event);
}

#[test]
fn crossing_roundtrip() {
    let event = OtkEvent::Crossing(CrossingEvent {
        crossing_id: CrossingId::new("c-1"),
        timing_point_id: TimingPointId::new("tp-start"),
        subject_id: Some(SubjectId::new("bib-42")),
        crossed_at_ns: 1_700_000_000_000_000_000,
        crossed_at_uncertainty_ns: Some(100),
        timebase_id: TimebaseId::new("gps-1"),
        timestamping_method: TimestampingMethod::HardwareEventCapture,
        source_attestation: SourceAttestation::RuntimeDiscovered,
        detection_ids: vec![DetectionId::new("det-1"), DetectionId::new("det-2")],
    });
    roundtrip(&event);
}
