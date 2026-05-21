use async_trait::async_trait;
use event_model::{AdapterMetadataEvent, Detection, DetectorHealthEvent, DetectorId};
use thiserror::Error;

/// Lifecycle state of a detector adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterState {
    Initializing,
    Running,
    Degraded,
    Stopped,
    Failed,
}

/// An event emitted by a detector adapter.
///
/// The first event after `DetectorAdapter::start` must be `Metadata` so the
/// timing node can register the adapter before any detections arrive.
#[derive(Debug, Clone)]
pub enum AdapterEvent {
    Detection(Detection),
    Health(DetectorHealthEvent),
    Metadata(AdapterMetadataEvent),
}

/// Errors that can occur during detector adapter operations.
///
/// The `Io` variant carries a `String` rather than `std::io::Error` so the
/// contract crate stays usable from `no_std + alloc` consumers (notably
/// firmware adapter implementations). `From<std::io::Error>` is provided
/// behind the default `std` feature for ergonomic `?`-propagation from
/// `std`-using adapters.
#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("device not found: {0}")]
    DeviceNotFound(String),
    #[error("device disconnected")]
    DeviceDisconnected,
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("I/O error: {0}")]
    Io(String),
}

#[cfg(feature = "std")]
impl From<std::io::Error> for AdapterError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

/// Map an `AdapterEvent` to the corresponding `OtkEvent` variant for publishing.
pub fn adapter_event_to_otk(event: AdapterEvent) -> event_model::OtkEvent {
    match event {
        AdapterEvent::Detection(d) => event_model::OtkEvent::Detection(d),
        AdapterEvent::Health(h) => event_model::OtkEvent::DetectorHealth(h),
        AdapterEvent::Metadata(m) => event_model::OtkEvent::AdapterMetadata(m),
    }
}

/// The universal detector adapter contract.
///
/// Implemented by every source of detector events: in-process plugins, external
/// producer processes, and embedded firmware.
///
/// # Lifecycle
///
/// 1. Call `start`; adapter opens device connections and transitions to `Running`.
/// 2. Loop on `next_event` to consume events as they arrive.
/// 3. The first event after `start` must be `AdapterEvent::Metadata`.
/// 4. Call `stop` when shutdown is desired; drain `next_event` until `None`.
#[async_trait]
pub trait DetectorAdapter: Send {
    fn detector_id(&self) -> &DetectorId;
    fn state(&self) -> AdapterState;
    async fn start(&mut self) -> Result<(), AdapterError>;
    async fn stop(&mut self) -> Result<(), AdapterError>;
    async fn next_event(&mut self) -> Option<Result<AdapterEvent, AdapterError>>;
}
