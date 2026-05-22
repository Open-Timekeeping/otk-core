use event_model::{
    AdapterCapabilities, AdapterMetadataEvent, Detection, DetectionId, DetectorHealthEvent,
    DetectorHealthStatus, DetectorId, SensorData, SourceAttestation, StreamDescriptor, SubjectId,
    TimebaseId, TimestampingMethod, TimingPointId,
};

use crate::producer::time::now_ns;

/// Builder for `Detection` events with sensible defaults.
///
/// Defaults: `detection_id = "{detector_id}-{seq}"`, `received_at_ns = now_ns()`,
/// `timestamping_method = AdapterReceiveTime`, `source_attestation = OperatorAsserted`,
/// `sensor = LoopTransponder { rssi_dbm: None, pulse_count: None }`.
pub struct DetectionBuilder {
    detector_id: DetectorId,
    timing_point_id: TimingPointId,
    detected_at_ns: u64,
    sequence_number: u64,
    detection_id: Option<DetectionId>,
    subject_id: Option<SubjectId>,
    timebase_id: TimebaseId,
    timestamping_method: TimestampingMethod,
    source_attestation: SourceAttestation,
    sensor: SensorData,
    detected_at_uncertainty_ns: Option<u64>,
}

impl DetectionBuilder {
    pub fn new(
        detector_id: &DetectorId,
        timing_point_id: &TimingPointId,
        detected_at_ns: u64,
        sequence_number: u64,
    ) -> Self {
        Self {
            detector_id: detector_id.clone(),
            timing_point_id: timing_point_id.clone(),
            detected_at_ns,
            sequence_number,
            detection_id: None,
            subject_id: None,
            timebase_id: TimebaseId::new("local"),
            timestamping_method: TimestampingMethod::AdapterReceiveTime,
            source_attestation: SourceAttestation::OperatorAsserted,
            sensor: SensorData::LoopTransponder {
                rssi_dbm: None,
                pulse_count: None,
            },
            detected_at_uncertainty_ns: None,
        }
    }

    pub fn detection_id(mut self, id: DetectionId) -> Self {
        self.detection_id = Some(id);
        self
    }

    pub fn subject_id(mut self, id: SubjectId) -> Self {
        self.subject_id = Some(id);
        self
    }

    pub fn timebase_id(mut self, id: TimebaseId) -> Self {
        self.timebase_id = id;
        self
    }

    pub fn timestamping_method(mut self, method: TimestampingMethod) -> Self {
        self.timestamping_method = method;
        self
    }

    pub fn source_attestation(mut self, attestation: SourceAttestation) -> Self {
        self.source_attestation = attestation;
        self
    }

    pub fn sensor(mut self, sensor: SensorData) -> Self {
        self.sensor = sensor;
        self
    }

    pub fn uncertainty_ns(mut self, ns: u64) -> Self {
        self.detected_at_uncertainty_ns = Some(ns);
        self
    }

    pub fn build(self) -> Detection {
        let detection_id = self.detection_id.unwrap_or_else(|| {
            DetectionId::new(format!("{}-{}", self.detector_id.as_str(), self.sequence_number))
        });
        Detection {
            detection_id,
            detector_id: self.detector_id,
            timing_point_id: self.timing_point_id,
            subject_id: self.subject_id,
            detected_at_ns: self.detected_at_ns,
            detected_at_uncertainty_ns: self.detected_at_uncertainty_ns,
            received_at_ns: Some(now_ns()),
            timestamping_method: self.timestamping_method,
            timebase_id: self.timebase_id,
            source_attestation: self.source_attestation,
            sequence_number: self.sequence_number,
            sensor: self.sensor,
        }
    }
}

/// Builder for the mandatory first `AdapterMetadataEvent`.
///
/// Defaults: `timebase_id = "local"`, `AdapterReceiveTime`, `OperatorAsserted`, `streams = []`.
pub struct MetadataBuilder {
    detector_id: DetectorId,
    timing_point_id: TimingPointId,
    timebase_id: TimebaseId,
    source_attestation: SourceAttestation,
    timestamping_method: TimestampingMethod,
    declared_resolution_ns: Option<u64>,
    streams: Vec<StreamDescriptor>,
}

impl MetadataBuilder {
    pub fn new(detector_id: &DetectorId, timing_point_id: &TimingPointId) -> Self {
        Self {
            detector_id: detector_id.clone(),
            timing_point_id: timing_point_id.clone(),
            timebase_id: TimebaseId::new("local"),
            source_attestation: SourceAttestation::OperatorAsserted,
            timestamping_method: TimestampingMethod::AdapterReceiveTime,
            declared_resolution_ns: None,
            streams: vec![],
        }
    }

    pub fn timebase_id(mut self, id: TimebaseId) -> Self {
        self.timebase_id = id;
        self
    }

    pub fn source_attestation(mut self, attestation: SourceAttestation) -> Self {
        self.source_attestation = attestation;
        self
    }

    pub fn timestamping_method(mut self, method: TimestampingMethod) -> Self {
        self.timestamping_method = method;
        self
    }

    pub fn resolution_ns(mut self, ns: u64) -> Self {
        self.declared_resolution_ns = Some(ns);
        self
    }

    pub fn stream(mut self, descriptor: StreamDescriptor) -> Self {
        self.streams.push(descriptor);
        self
    }

    pub fn build(self) -> AdapterMetadataEvent {
        AdapterMetadataEvent {
            detector_id: self.detector_id,
            timing_point_id: self.timing_point_id,
            timebase_id: self.timebase_id,
            source_attestation: self.source_attestation,
            declared_at_ns: now_ns(),
            capabilities: AdapterCapabilities {
                streams: self.streams,
                timestamping_method: self.timestamping_method,
                declared_resolution_ns: self.declared_resolution_ns,
            },
        }
    }
}

/// Builder for `DetectorHealthEvent`.
pub struct HealthEventBuilder {
    detector_id: DetectorId,
    status: DetectorHealthStatus,
    message: Option<String>,
}

impl HealthEventBuilder {
    pub fn healthy(detector_id: &DetectorId) -> Self {
        Self {
            detector_id: detector_id.clone(),
            status: DetectorHealthStatus::Healthy,
            message: None,
        }
    }

    pub fn degraded(detector_id: &DetectorId, reason: impl Into<String>) -> Self {
        Self {
            detector_id: detector_id.clone(),
            status: DetectorHealthStatus::Degraded { reason: reason.into() },
            message: None,
        }
    }

    pub fn failed(detector_id: &DetectorId, reason: impl Into<String>) -> Self {
        Self {
            detector_id: detector_id.clone(),
            status: DetectorHealthStatus::Failed { reason: reason.into() },
            message: None,
        }
    }

    pub fn message(mut self, msg: impl Into<String>) -> Self {
        self.message = Some(msg.into());
        self
    }

    pub fn build(self) -> DetectorHealthEvent {
        DetectorHealthEvent {
            detector_id: self.detector_id,
            reported_at_ns: now_ns(),
            status: self.status,
            message: self.message,
        }
    }
}
