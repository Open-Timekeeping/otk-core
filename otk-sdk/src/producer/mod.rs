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
// The producer::producer naming is intentional: `Producer` is the headline
// type of this submodule. mod.rs re-exports it as `producer::Producer`, so
// callers never type `producer::producer::Producer`.
#[allow(clippy::module_inception)]
pub mod producer;
pub mod seq;
pub mod time;
pub mod timebase;
#[cfg(feature = "producer-tls")]
pub mod tls;
pub mod transport;

pub use adapter::{
    adapter_event_to_otk, AdapterError, AdapterEvent, AdapterState, DetectorAdapter,
};
pub use builder::{DetectionBuilder, HealthEventBuilder, MetadataBuilder};
pub use error::ProducerError;
pub use producer::{Producer, ProducerConfig};
pub use seq::SequenceCounter;
pub use time::now_ns;
pub use timebase::{
    Timebase, TimebaseError, TimebaseEvent, TimebaseKind, TimebaseMetadataEvent, TimebaseState,
};
pub use transport::Transport;

#[cfg(feature = "producer-tls")]
pub use transport::TlsClientConfig;
