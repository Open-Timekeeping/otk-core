use alloc::vec::Vec;
use minicbor::{Decode, Encode};

use crate::ids::{CrossingId, DetectionId, SubjectId, TimebaseId, TimingPointId};
use crate::timestamp::{SourceAttestation, TimestampingMethod};

/// A derived timing event representing one passage of a subject across a timing point.
///
/// Produced by `timing-core` from one or more raw [`Detection`] events. Carried in
/// [`OtkEvent::Crossing`] in the event log alongside the source detections.
///
/// [`Detection`]: crate::Detection
/// [`OtkEvent::Crossing`]: crate::OtkEvent::Crossing
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CrossingEvent {
    #[n(0)]
    pub crossing_id: CrossingId,

    #[n(1)]
    pub timing_point_id: TimingPointId,

    /// The subject that crossed. `None` when the identity is unknown or not applicable.
    #[n(2)]
    pub subject_id: Option<SubjectId>,

    /// Timestamp of the crossing, in nanoseconds since the Unix epoch.
    #[n(3)]
    pub crossed_at_ns: u64,

    /// Timestamp uncertainty in nanoseconds. Widened to cover the full span of
    /// contributing detections when more than one detection is in the group.
    #[n(4)]
    pub crossed_at_uncertainty_ns: Option<u64>,

    /// The timebase the crossing timestamp is referenced to.
    #[n(5)]
    pub timebase_id: TimebaseId,

    /// How the crossing timestamp was produced (inherited from the peak detection).
    #[n(6)]
    pub timestamping_method: TimestampingMethod,

    /// Whether the timebase identity was discovered at runtime or asserted by config.
    #[n(7)]
    pub source_attestation: SourceAttestation,

    /// Detection IDs that contributed to this crossing, in arrival order.
    #[n(8)]
    pub detection_ids: Vec<DetectionId>,
}
