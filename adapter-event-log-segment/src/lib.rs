//! Segment-file event log backend for Open Timekeeping.
//!
//! This crate implements [`port_out_event_log::EventLog`] using an append-only
//! sequence of fixed-budget segment files on local disk. It is the only
//! storage backend Open Timekeeping ships for v0; alternative backends can be
//! added later behind the same [`port_out_event_log::EventLog`] contract.
//!
//! # How it works
//!
//! Events are appended to an **active segment** file
//! (`{base_offset:020}.seg`). When the active segment reaches the configured
//! size or age limit it is **rolled**: the file is fsynced, a zero sentinel is
//! written, and an offset index (`{base_offset:020}.idx`) is written
//! atomically alongside it. The segment then becomes a **closed segment**;
//! subsequent appends go to a new active segment.
//!
//! Closed segments are immutable. Range reads use the offset index to seek
//! directly to the requested record. Live subscriptions read from disk and wait
//! on a [`tokio::sync::Notify`] signal that fires on every `append`.
//!
//! Crash recovery opens the active segment, scans it record-by-record
//! verifying CRC32, and truncates at the first corrupt or incomplete record.
//!
//! # Usage
//!
//! ```no_run
//! use adapter_event_log_segment::{SegmentLog, SegmentLogConfig};
//! use port_out_event_log::{EventLog, RetentionPolicy};
//! use std::path::PathBuf;
//!
//! # async fn example() -> Result<(), port_out_event_log::StorageError> {
//! let config = SegmentLogConfig {
//!     dir: PathBuf::from("/var/lib/otk-node/log"),
//!     retention: RetentionPolicy::TimeBased { max_age_secs: 86400 },
//!     ..Default::default()
//! };
//! let mut log = SegmentLog::open(config).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Deferred items
//!
//! - Background fsync task: setting `flush_interval_ms > 0` skips the
//!   per-`append` `sync_all` call, relying on OS write-back instead. There is
//!   no background flusher yet, so `flush_interval_ms > 0` trades durability
//!   for throughput without a timer-based safety net.
//! - Time-based index (deferred until `port_out_event_log` gains a timestamp-range
//!   read variant).
//! - Periodic retention enforcement (currently runs only after segment roll).

pub mod config;
pub mod log;

mod segment;
mod subscription;

pub use config::SegmentLogConfig;
pub use log::SegmentLog;
