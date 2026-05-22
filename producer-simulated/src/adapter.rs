use async_trait::async_trait;
use otk_sdk::producer::{
    AdapterError, AdapterEvent, AdapterState, DetectionBuilder, DetectorAdapter,
    HealthEventBuilder, MetadataBuilder, SequenceCounter, now_ns,
};
use otk_sdk::event_model::{
    DetectorId, SourceAttestation, SubjectId, TimebaseId, TimestampingMethod, TimingPointId,
};

use crate::config::SimulatorConfig;

#[derive(Clone, Copy)]
enum Phase {
    PendingMetadata,
    PendingHealth,
    Generating { emitted: u64 },
    Done,
}

/// Simulated detector adapter.
///
/// Emits a mandatory `Metadata` event, a `Health(Healthy)` event, then
/// synthetic `Detection` events at the configured interval, cycling through
/// the declared timing points and subject IDs.
pub struct SimulatorAdapter {
    config: SimulatorConfig,
    state: AdapterState,
    phase: Phase,
    seq: SequenceCounter,
    detector_id: DetectorId,
    timebase_id: TimebaseId,
    timing_point_ids: Vec<TimingPointId>,
    subject_ids: Vec<SubjectId>,
}

impl SimulatorAdapter {
    pub fn new(config: SimulatorConfig) -> Self {
        let detector_id = DetectorId::new(&config.detector_id);
        let timebase_id = TimebaseId::new(&config.timebase_id);
        let timing_point_ids =
            config.timing_point_ids.iter().map(|s| TimingPointId::new(s)).collect();
        let subject_ids = config.subject_ids.iter().map(|s| SubjectId::new(s)).collect();
        Self {
            config,
            state: AdapterState::Initializing,
            phase: Phase::PendingMetadata,
            seq: SequenceCounter::new(),
            detector_id,
            timebase_id,
            timing_point_ids,
            subject_ids,
        }
    }

    fn primary_timing_point(&self) -> TimingPointId {
        self.timing_point_ids
            .first()
            .cloned()
            .unwrap_or_else(|| TimingPointId::new("tp-default"))
    }
}

#[async_trait]
impl DetectorAdapter for SimulatorAdapter {
    fn detector_id(&self) -> &DetectorId {
        &self.detector_id
    }

    fn state(&self) -> AdapterState {
        self.state
    }

    async fn start(&mut self) -> Result<(), AdapterError> {
        if self.timing_point_ids.is_empty() {
            return Err(AdapterError::Configuration(
                "timing_point_ids must not be empty".into(),
            ));
        }
        self.state = AdapterState::Running;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), AdapterError> {
        self.state = AdapterState::Stopped;
        Ok(())
    }

    async fn next_event(&mut self) -> Option<Result<AdapterEvent, AdapterError>> {
        match self.phase {
            Phase::PendingMetadata => {
                let tp = self.primary_timing_point();
                let event = MetadataBuilder::new(&self.detector_id, &tp)
                    .timebase_id(self.timebase_id.clone())
                    .timestamping_method(TimestampingMethod::AdapterReceiveTime)
                    .source_attestation(SourceAttestation::OperatorAsserted)
                    .build();
                self.phase = Phase::PendingHealth;
                Some(Ok(AdapterEvent::Metadata(event)))
            }

            Phase::PendingHealth => {
                let event = HealthEventBuilder::healthy(&self.detector_id).build();
                self.phase = Phase::Generating { emitted: 0 };
                Some(Ok(AdapterEvent::Health(event)))
            }

            Phase::Generating { emitted } => {
                if let Some(count) = self.config.count {
                    if emitted >= count {
                        self.phase = Phase::Done;
                        self.state = AdapterState::Stopped;
                        return None;
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(
                    self.config.detection_interval_ms,
                ))
                .await;

                let seq = self.seq.next();

                let tp_count = self.timing_point_ids.len().max(1);
                let tp = self
                    .timing_point_ids
                    .get((emitted as usize) % tp_count)
                    .cloned()
                    .unwrap_or_else(|| TimingPointId::new("tp-default"));

                let subject_id = if self.subject_ids.is_empty() {
                    None
                } else {
                    let idx = (emitted as usize) % self.subject_ids.len();
                    Some(self.subject_ids[idx].clone())
                };

                let mut builder =
                    DetectionBuilder::new(&self.detector_id, &tp, now_ns(), seq)
                        .timebase_id(self.timebase_id.clone());

                if let Some(sid) = subject_id {
                    builder = builder.subject_id(sid);
                }

                self.phase = Phase::Generating { emitted: emitted + 1 };
                Some(Ok(AdapterEvent::Detection(builder.build())))
            }

            Phase::Done => None,
        }
    }
}
