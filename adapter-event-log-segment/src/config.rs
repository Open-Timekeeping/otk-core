use std::path::PathBuf;

use port_out_event_log::RetentionPolicy;

/// Configuration for a [`SegmentLog`](crate::SegmentLog) instance.
#[derive(Debug, Clone)]
pub struct SegmentLogConfig {
    /// Directory where segment and index files are stored.
    pub dir: PathBuf,

    /// Roll the active segment when it exceeds this many bytes of record data.
    /// Default: 64 MiB.
    pub max_segment_bytes: u64,

    /// Roll the active segment when it has been open for this many seconds.
    /// Default: 3600 (1 hour).
    pub max_segment_age_secs: u64,

    /// fsync interval in milliseconds. `0` (default) means `sync_all()` is called
    /// once per `append()` invocation. Any nonzero value skips fsync, relying on
    /// OS write buffers; a background fsync task is a planned future addition.
    pub flush_interval_ms: u64,

    /// Retention policy applied after each segment roll.
    /// Default: `Indefinite`.
    ///
    /// Note: for `SizeBased` and `Hybrid` policies the `max_bytes` limit is
    /// enforced against segment data file sizes only. Companion `.idx` files
    /// are not counted. Actual on-disk usage can exceed `max_bytes` by up to
    /// the total index size for all retained segments.
    pub retention: RetentionPolicy,
}

impl Default for SegmentLogConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("otk-log"),
            max_segment_bytes: 64 * 1024 * 1024,
            max_segment_age_secs: 3600,
            flush_interval_ms: 0,
            retention: RetentionPolicy::Indefinite,
        }
    }
}
