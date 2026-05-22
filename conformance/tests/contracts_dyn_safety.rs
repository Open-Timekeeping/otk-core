//! Trait contract conformance: object safety + lifecycle invariants.
//!
//! Verifies that the two top-level trait contracts in [`otk-contracts`]
//! ([`DetectorAdapter`] and [`Timebase`]) are dyn-safe (every adapter or
//! timebase implementation must be storable behind `Box<dyn ...>`), and that
//! a minimal implementation honors the contract's "first event must be
//! Metadata" rule.

use async_trait::async_trait;
use event_model::{
    timestamp::SourceAttestation, AdapterCapabilities, AdapterMetadataEvent, DetectorId,
    TimebaseId, TimebaseStatusEvent, TimestampingMethod, TimingPointId,
};
use otk_contracts::{
    AdapterError, AdapterEvent, AdapterState, DetectorAdapter, Timebase, TimebaseError,
    TimebaseEvent, TimebaseKind, TimebaseMetadataEvent, TimebaseState,
};

// ── Detector adapter ─────────────────────────────────────────────────────────

struct MinimalDetector {
    id: DetectorId,
    state: AdapterState,
    yielded_metadata: bool,
}

impl MinimalDetector {
    fn new() -> Self {
        Self {
            id: DetectorId::new("test-detector"),
            state: AdapterState::Initializing,
            yielded_metadata: false,
        }
    }
}

#[async_trait]
impl DetectorAdapter for MinimalDetector {
    fn detector_id(&self) -> &DetectorId {
        &self.id
    }
    fn state(&self) -> AdapterState {
        self.state
    }
    async fn start(&mut self) -> Result<(), AdapterError> {
        self.state = AdapterState::Running;
        Ok(())
    }
    async fn stop(&mut self) -> Result<(), AdapterError> {
        self.state = AdapterState::Stopped;
        Ok(())
    }
    async fn next_event(&mut self) -> Option<Result<AdapterEvent, AdapterError>> {
        if !self.yielded_metadata {
            self.yielded_metadata = true;
            return Some(Ok(AdapterEvent::Metadata(AdapterMetadataEvent {
                detector_id: self.id.clone(),
                timing_point_id: TimingPointId::new("tp-1"),
                timebase_id: TimebaseId::new("tb-1"),
                source_attestation: SourceAttestation::OperatorAsserted,
                declared_at_ns: 0,
                capabilities: AdapterCapabilities {
                    streams: vec![],
                    timestamping_method: TimestampingMethod::AdapterReceiveTime,
                    declared_resolution_ns: None,
                },
            })));
        }
        None
    }
}

#[tokio::test]
async fn detector_adapter_is_dyn_safe() {
    let mut adapter: Box<dyn DetectorAdapter> = Box::new(MinimalDetector::new());
    assert_eq!(adapter.state(), AdapterState::Initializing);
    adapter.start().await.unwrap();
    assert_eq!(adapter.state(), AdapterState::Running);
    adapter.stop().await.unwrap();
    assert_eq!(adapter.state(), AdapterState::Stopped);
}

#[tokio::test]
async fn detector_adapter_first_event_is_metadata() {
    let mut adapter = MinimalDetector::new();
    adapter.start().await.unwrap();
    let first = adapter
        .next_event()
        .await
        .expect("at least one event")
        .expect("ok");
    assert!(
        matches!(first, AdapterEvent::Metadata(_)),
        "adapter contract requires Metadata as the first event after start"
    );
}

// ── Timebase ─────────────────────────────────────────────────────────────────

struct MinimalTimebase {
    id: TimebaseId,
    state: TimebaseState,
    yielded_metadata: bool,
    yielded_status: bool,
}

impl MinimalTimebase {
    fn new() -> Self {
        Self {
            id: TimebaseId::new("test-tb"),
            state: TimebaseState::Initializing,
            yielded_metadata: false,
            yielded_status: false,
        }
    }
}

#[async_trait]
impl Timebase for MinimalTimebase {
    fn timebase_id(&self) -> &TimebaseId {
        &self.id
    }
    fn state(&self) -> TimebaseState {
        self.state
    }
    async fn start(&mut self) -> Result<(), TimebaseError> {
        self.state = TimebaseState::Running;
        Ok(())
    }
    async fn stop(&mut self) -> Result<(), TimebaseError> {
        self.state = TimebaseState::Stopped;
        Ok(())
    }
    async fn next_event(&mut self) -> Option<Result<TimebaseEvent, TimebaseError>> {
        if !self.yielded_metadata {
            self.yielded_metadata = true;
            return Some(Ok(TimebaseEvent::Metadata(TimebaseMetadataEvent {
                timebase_id: self.id.clone(),
                kind: TimebaseKind::Local,
                declared_uncertainty_ns: Some(1_000_000),
                source_attestation: SourceAttestation::OperatorAsserted,
                declared_at_ns: 0,
            })));
        }
        if !self.yielded_status {
            self.yielded_status = true;
            return Some(Ok(TimebaseEvent::Status(TimebaseStatusEvent {
                timebase_id: self.id.clone(),
                reported_at_ns: 0,
                sync_state: event_model::SyncState::Locked,
                uncertainty_ns: None,
                source_attestation: SourceAttestation::OperatorAsserted,
            })));
        }
        None
    }
}

#[tokio::test]
async fn timebase_is_dyn_safe() {
    let mut tb: Box<dyn Timebase> = Box::new(MinimalTimebase::new());
    assert_eq!(tb.state(), TimebaseState::Initializing);
    tb.start().await.unwrap();
    assert_eq!(tb.state(), TimebaseState::Running);
    tb.stop().await.unwrap();
    assert_eq!(tb.state(), TimebaseState::Stopped);
}

#[tokio::test]
async fn timebase_first_event_is_metadata() {
    let mut tb = MinimalTimebase::new();
    tb.start().await.unwrap();
    let first = tb
        .next_event()
        .await
        .expect("at least one event")
        .expect("ok");
    assert!(
        matches!(first, TimebaseEvent::Metadata(_)),
        "timebase contract requires Metadata as the first event after start"
    );
}
