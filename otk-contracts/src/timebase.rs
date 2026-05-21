use async_trait::async_trait;
use event_model::timestamp::SourceAttestation;
use event_model::{TimebaseId, TimebaseStatusEvent};
use thiserror::Error;

/// Lifecycle state of a timebase implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimebaseState {
    Initializing,
    Running,
    Degraded,
    Stopped,
    Failed,
}

/// The sync mechanism a timebase uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimebaseKind {
    Gnss,
    Ptp,
    Ntp,
    Local,
    Other(String),
}

/// Capability and identity declaration published by a timebase at startup.
///
/// Must be the first event from `Timebase::next_event` after `start` returns.
#[derive(Debug, Clone)]
pub struct TimebaseMetadataEvent {
    pub timebase_id: TimebaseId,
    pub kind: TimebaseKind,
    pub declared_uncertainty_ns: Option<u64>,
    pub source_attestation: SourceAttestation,
    pub declared_at_ns: u64,
}

/// An event emitted by a timebase implementation.
#[derive(Debug, Clone)]
pub enum TimebaseEvent {
    Status(TimebaseStatusEvent),
    Metadata(TimebaseMetadataEvent),
}

/// Errors that can occur during timebase operations.
///
/// The `Io` variant carries a `String` rather than `std::io::Error` so the
/// contract crate stays usable from `no_std + alloc` consumers. See the
/// matching note on [`crate::AdapterError`].
#[derive(Debug, Error)]
pub enum TimebaseError {
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
impl From<std::io::Error> for TimebaseError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

/// The universal timebase contract.
///
/// Implemented by every clock-source implementation: in-process plugins,
/// standalone processes, or firmware.
///
/// # Lifecycle
///
/// 1. Call `start`; timebase opens its sync source and transitions to `Running`.
/// 2. Loop on `next_event` to consume status and metadata events.
/// 3. The first event after `start` must be `TimebaseEvent::Metadata`.
/// 4. Call `stop` when shutdown is desired; drain `next_event` until `None`.
#[async_trait]
pub trait Timebase: Send {
    fn timebase_id(&self) -> &TimebaseId;
    fn state(&self) -> TimebaseState;
    async fn start(&mut self) -> Result<(), TimebaseError>;
    async fn stop(&mut self) -> Result<(), TimebaseError>;
    async fn next_event(&mut self) -> Option<Result<TimebaseEvent, TimebaseError>>;
}
