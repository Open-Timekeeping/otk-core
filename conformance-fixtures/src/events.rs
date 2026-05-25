//! [`OtkEvent`] wrappers and exhaustive variant samples.
//!
//! Two flavours:
//!
//! - Wrapping helpers ([`as_detection_event`], [`beam_break_event`],
//!   [`loop_transponder_event`]) for the common path of "build a
//!   detection, wrap it in `OtkEvent`."
//! - [`canon`]: one canonical example per [`OtkEvent`] variant, used by
//!   round-trip / encode-decode tests that need exhaustiveness over
//!   the event-model surface.

use event_model::{Detection, OtkEvent};

use crate::detections::{beam_break_at_loop, loop_transponder_with_rssi};

/// Wrap a [`Detection`] in [`OtkEvent::Detection`].
pub fn as_detection_event(d: Detection) -> OtkEvent {
    OtkEvent::Detection(d)
}

/// Convenience: build a beam-break detection via [`beam_break_at_loop`]
/// and wrap it in [`OtkEvent::Detection`].
pub fn beam_break_event(seq: u64) -> OtkEvent {
    as_detection_event(beam_break_at_loop(seq))
}

/// Convenience: build a loop-transponder detection via
/// [`loop_transponder_with_rssi`] and wrap it in
/// [`OtkEvent::Detection`].
pub fn loop_transponder_event(seq: u64, bib: u32, rssi_dbm: i16) -> OtkEvent {
    as_detection_event(loop_transponder_with_rssi(seq, bib, rssi_dbm))
}

/// One canonical example per [`OtkEvent`] variant.
///
/// Used by the event-model round-trip suite to exhaustively exercise
/// CBOR encode/decode against every public variant. The values are
/// deliberately diverse (different `subject_id` presence,
/// `SourceAttestation`s, optional uncertainty fields) so a serializer
/// that loses one variant or one optional field is caught at the
/// suite level.
pub mod canon {
    use event_model::{
        AdapterCapabilities, AdapterMetadataEvent, CrossingEvent, CrossingId, Detection,
        DetectionId, DetectorHealthEvent, DetectorHealthStatus, DetectorId, OtkEvent, SensorData,
        SourceAttestation, StreamDescriptor, StreamId, StreamKind, SubjectId, SyncState,
        TimebaseId, TimebaseStatusEvent, TimestampingMethod, TimingPointId,
    };

    /// Loop-transponder detection with full provenance (RSSI, pulse
    /// count, uncertainty, received timestamp, named subject).
    pub fn detection_loop_transponder() -> OtkEvent {
        OtkEvent::Detection(Detection {
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
        })
    }

    /// Anonymous beam-break detection (no subject, no uncertainty, no
    /// received timestamp). Exercises the optional-field-elided code
    /// paths.
    pub fn detection_beam_break() -> OtkEvent {
        OtkEvent::Detection(Detection {
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
        })
    }

    /// Detector reporting `Healthy` with a free-text status message.
    pub fn detector_health_healthy() -> OtkEvent {
        OtkEvent::DetectorHealth(DetectorHealthEvent {
            detector_id: DetectorId::new("loop-1"),
            reported_at_ns: 1_700_000_000_000_000_000,
            status: DetectorHealthStatus::Healthy,
            message: Some("ok".into()),
        })
    }

    /// Detector reporting `Degraded { reason }`, no free-text message.
    pub fn detector_health_degraded() -> OtkEvent {
        OtkEvent::DetectorHealth(DetectorHealthEvent {
            detector_id: DetectorId::new("loop-1"),
            reported_at_ns: 1_700_000_000_000_000_000,
            status: DetectorHealthStatus::Degraded {
                reason: "low SNR".into(),
            },
            message: None,
        })
    }

    /// Timebase status report at the given `SyncState`. Use
    /// [`timebase_status_all_states`] to iterate every variant.
    pub fn timebase_status(state: SyncState) -> OtkEvent {
        OtkEvent::TimebaseStatus(TimebaseStatusEvent {
            timebase_id: TimebaseId::new("gps-1"),
            reported_at_ns: 1_700_000_000_000_000_000,
            sync_state: state,
            uncertainty_ns: Some(1_000),
            source_attestation: SourceAttestation::RuntimeDiscovered,
        })
    }

    /// All five `SyncState` variants in a stable order. Useful for
    /// exhaustive iteration in tests that want to assert every state
    /// round-trips.
    pub fn timebase_status_all_states() -> [OtkEvent; 5] {
        [
            timebase_status(SyncState::Locked),
            timebase_status(SyncState::Holdover),
            timebase_status(SyncState::FreeRun),
            timebase_status(SyncState::Unsynchronized),
            timebase_status(SyncState::Unknown),
        ]
    }

    /// Adapter metadata announcing one detection stream from the
    /// loop-1 detector at tp-start.
    pub fn adapter_metadata() -> OtkEvent {
        OtkEvent::AdapterMetadata(AdapterMetadataEvent {
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
        })
    }

    /// Crossing derived from two detections (det-1, det-2), with
    /// uncertainty and a named subject.
    pub fn crossing() -> OtkEvent {
        OtkEvent::Crossing(CrossingEvent {
            crossing_id: CrossingId::new("c-1"),
            timing_point_id: TimingPointId::new("tp-start"),
            subject_id: Some(SubjectId::new("bib-42")),
            crossed_at_ns: 1_700_000_000_000_000_000,
            crossed_at_uncertainty_ns: Some(100),
            timebase_id: TimebaseId::new("gps-1"),
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            source_attestation: SourceAttestation::RuntimeDiscovered,
            detection_ids: vec![DetectionId::new("det-1"), DetectionId::new("det-2")],
        })
    }

    /// One example per `OtkEvent` variant, in a stable order. Adding a
    /// new variant should add an entry here; tests that iterate this
    /// will then exhaustively cover the new variant for free.
    pub fn one_of_each_variant() -> Vec<OtkEvent> {
        vec![
            detection_loop_transponder(),
            detection_beam_break(),
            detector_health_healthy(),
            detector_health_degraded(),
            timebase_status(SyncState::Locked),
            adapter_metadata(),
            crossing(),
        ]
    }
}
