use alloc::string::String;
use minicbor::{Decode, Encode};

use crate::ids::{DetectorId, TimebaseId};
use crate::timestamp::{SourceAttestation, SyncState};

/// Operational state of a detector adapter at a point in time.
#[derive(Debug, Clone, Encode, Decode)]
pub enum DetectorHealthStatus {
    #[n(0)]
    Healthy,

    /// Producing data but with degraded confidence or capability.
    #[n(1)]
    Degraded {
        #[n(0)]
        reason: String,
    },

    /// Not producing data; intervention required.
    #[n(2)]
    Failed {
        #[n(0)]
        reason: String,
    },
}

/// Periodic or state-change health report from a detector adapter.
/// First-class event; distinct from detection events.
#[derive(Debug, Clone, Encode, Decode)]
pub struct DetectorHealthEvent {
    #[n(0)]
    pub detector_id: DetectorId,

    /// When this health report was generated, in nanoseconds since Unix epoch.
    #[n(1)]
    pub reported_at_ns: u64,

    #[n(2)]
    pub status: DetectorHealthStatus,

    /// Human-readable detail for operators. Not for programmatic consumption.
    #[n(3)]
    pub message: Option<String>,
}

/// Periodic or state-change status report from a timebase implementation.
/// Reports the actual runtime sync state; operator config is a claim, this is the truth.
#[derive(Debug, Clone, Encode, Decode)]
pub struct TimebaseStatusEvent {
    #[n(0)]
    pub timebase_id: TimebaseId,

    /// When this status was observed, in nanoseconds since Unix epoch.
    #[n(1)]
    pub reported_at_ns: u64,

    #[n(2)]
    pub sync_state: SyncState,

    /// Current estimated timestamp uncertainty in nanoseconds. `None` means unknown.
    #[n(3)]
    pub uncertainty_ns: Option<u64>,

    #[n(4)]
    pub source_attestation: SourceAttestation,
}
