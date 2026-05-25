use event_model::{
    DetectionId, SourceAttestation, SubjectId, TimebaseId, TimestampingMethod, TimingPointId,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CrossingId(String);

impl CrossingId {
    pub(crate) fn new_random() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CrossingId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A derived timing event representing one passage of a subject across a timing point.
///
/// Produced by [`CrossingProcessor`] from one or more raw [`Detection`] events.
/// When multiple detections for the same subject at the same point arrive within the
/// configured grouping window, they are merged into a single crossing. The timestamp
/// is chosen by peak-signal selection (highest RSSI for loop detectors; earliest
/// timestamp otherwise).
///
/// [`CrossingProcessor`]: crate::CrossingProcessor
/// [`Detection`]: event_model::Detection
#[derive(Debug, Clone)]
pub struct Crossing {
    pub crossing_id: CrossingId,
    pub timing_point_id: TimingPointId,
    pub subject_id: Option<SubjectId>,
    /// Timestamp of the peak detection, in nanoseconds since the Unix epoch.
    pub crossed_at_ns: u64,
    /// Timestamp uncertainty in nanoseconds. Widened to cover the full span of
    /// contributing detections when more than one detection is in the group.
    pub crossed_at_uncertainty_ns: Option<u64>,
    pub timebase_id: TimebaseId,
    pub timestamping_method: TimestampingMethod,
    pub source_attestation: SourceAttestation,
    /// Detection IDs that contributed to this crossing, in arrival order.
    pub detection_ids: Vec<DetectionId>,
}
