//! Producer-side API for publishing events to a timing node.
//!
//! Core types:
//! - [`Producer`]: TCP connection to a timing node; publishes `OtkEvent` values.
//! - [`DetectorAdapter`]: trait contract for hardware/software event sources.
//! - [`Timebase`]: trait contract for clock-source implementations.
//! - [`DetectionBuilder`], [`MetadataBuilder`], [`HealthEventBuilder`]: builder helpers.
//! - [`SequenceCounter`]: atomic per-detector sequence counter.
//! - [`now_ns`]: current wall-clock nanoseconds.

pub mod adapter;
pub mod builder;
pub mod error;
pub mod producer;
pub mod seq;
pub mod timebase;
pub mod time;
pub mod transport;

pub use adapter::{adapter_event_to_otk, AdapterError, AdapterEvent, AdapterState, DetectorAdapter};
pub use builder::{DetectionBuilder, HealthEventBuilder, MetadataBuilder};
pub use error::ProducerError;
pub use producer::{Producer, ProducerConfig};
pub use seq::SequenceCounter;
pub use timebase::{Timebase, TimebaseError, TimebaseEvent, TimebaseKind, TimebaseMetadataEvent, TimebaseState};
pub use time::now_ns;
pub use transport::Transport;
