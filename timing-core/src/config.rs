/// Configuration for [`CrossingProcessor`].
///
/// [`CrossingProcessor`]: crate::CrossingProcessor
#[derive(Debug, Clone, Copy)]
pub struct ProcessorConfig {
    /// Maximum time span (ns) over which detections for the same
    /// `(timing_point_id, subject_id)` pair are merged into a single crossing.
    /// Measured from the first detection in the group to the new arrival.
    /// Default: 2,000,000,000 ns (2 seconds).
    pub grouping_window_ns: u64,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            grouping_window_ns: 2_000_000_000,
        }
    }
}
