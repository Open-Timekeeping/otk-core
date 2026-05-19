use alloc::vec::Vec;
use minicbor::{Decode, Encode};

use crate::ids::{DetectorId, TimebaseId, TimingPointId};
use crate::stream::StreamDescriptor;
use crate::timestamp::{SourceAttestation, TimestampingMethod};

/// Declared capabilities of a detector adapter. Published in `AdapterMetadataEvent`
/// at startup and on configuration change.
#[derive(Debug, Clone, Encode, Decode)]
pub struct AdapterCapabilities {
    /// The streams this adapter publishes to, with their kinds and addresses.
    #[n(0)]
    pub streams: Vec<StreamDescriptor>,

    /// The timestamping method this adapter uses for `detected_at_ns`.
    #[n(1)]
    pub timestamping_method: TimestampingMethod,

    /// Nominal timestamp resolution in nanoseconds. Does not imply accuracy at this level;
    /// uncertainty is reported per event. `None` means resolution is not declared.
    #[n(2)]
    pub declared_resolution_ns: Option<u64>,
}

/// Registration and capability declaration event. Published by an adapter at startup
/// and whenever its configuration changes.
#[derive(Debug, Clone, Encode, Decode)]
pub struct AdapterMetadataEvent {
    #[n(0)]
    pub detector_id: DetectorId,

    #[n(1)]
    pub timing_point_id: TimingPointId,

    /// The timebase this adapter's timestamps are disciplined against.
    #[n(2)]
    pub timebase_id: TimebaseId,

    #[n(3)]
    pub source_attestation: SourceAttestation,

    /// When this declaration was made, in nanoseconds since Unix epoch.
    #[n(4)]
    pub declared_at_ns: u64,

    #[n(5)]
    pub capabilities: AdapterCapabilities,
}
