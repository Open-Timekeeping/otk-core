//! OTK Storage API: the persistence contract every event log backend implements.
//!
//! This crate defines the boundary between the OTK runtime node and its storage
//! backends. Storage is pluggable: `timing-node` depends on this trait, not on
//! any particular backend. For v0, the only shipped backend is
//! `storage-segment-log`.
//!
//! # What a storage backend provides
//!
//! An **event log** is an append-only sequence of [`OtkEvent`] values, each
//! assigned a monotonic [`Offset`] by the backend. Consumers reconnect by
//! supplying the offset after the last one they successfully processed
//! (`last.checked_next().expect("offset exhausted")`); the log replays from
//! that point or returns
//! [`StorageError::RetentionExpired`] if the range has been evicted.
//!
//! # Key types
//!
//! - **[`EventLog`]**: the core persistence trait. Lifecycle: append events,
//!   read ranges, subscribe to live delivery, query bounds.
//! - **[`LogSubscription`]**: a live subscription returned by
//!   [`EventLog::subscribe`]. Poll [`next_entry`] until `None` (closed) or
//!   `Some(Err(_))` (terminal error); call [`LogSubscription::close`] after either.
//! - **[`LogEntry`]**: a stored event with its [`Offset`] and receipt timestamp.
//! - **[`Offset`]**: a monotonic `u64` position in the log.
//! - **[`RetentionPolicy`]**: how long the backend retains old entries.
//! - **[`StorageError`]**: error vocabulary, including the structured
//!   [`RetentionExpired`] variant for reads of evicted ranges.
//!
//! # Design
//!
//! **Poll-based subscriptions.** [`LogSubscription::next_entry`] follows the
//! same poll-until-`None` pattern as [`DetectorAdapter::next_event`] and
//! [`Timebase::next_event`] elsewhere in the OTK stack.
//!
//! **`&mut self` methods.** All [`EventLog`] and [`LogSubscription`] methods
//! take `&mut self`. The runtime node wraps the backend in a `Mutex` or similar
//! if it needs to share access across tasks.
//!
//! **std-only.** Async traits via `async-trait` require `std`.
//!
//! # Dependencies
//!
//! Depends on `event-model` for [`OtkEvent`] (the top-level event envelope).
//! No dependency on `transport-api`, `frame-codec`, or `wire-protocol`.
//!
//! [`DetectorAdapter::next_event`]: https://github.com/Open-Timekeeping/detector-adapter-api
//! [`Timebase::next_event`]: https://github.com/Open-Timekeeping/timebase-api
//! [`OtkEvent`]: event_model::OtkEvent
//! [`next_entry`]: LogSubscription::next_entry
//! [`RetentionExpired`]: StorageError::RetentionExpired

pub mod entry;
pub mod error;
pub mod log;
pub mod offset;
pub mod retention;

pub use entry::LogEntry;
pub use error::StorageError;
pub use log::{EventLog, LogSubscription};
pub use offset::Offset;
pub use retention::RetentionPolicy;
