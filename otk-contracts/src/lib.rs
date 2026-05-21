//! Open Timekeeping trait contracts.
//!
//! This crate is the dependency-light home for the universal trait contracts a
//! third-party implementer compiles against to claim OTK compatibility:
//!
//! - [`DetectorAdapter`]: every source of detector events (firmware, external
//!   process, in-process plugin, simulator, replay).
//! - [`Timebase`]: every clock-source implementation (GNSS, PTP, NTP, local).
//!
//! Dependencies are deliberately minimal (`event-model`, `async-trait`,
//! `thiserror`) so a vendor's adapter crate does not have to pull in
//! `tokio`/`minicbor`/`reqwest` to know what trait to implement.
//!
//! The `otk-sdk` crate re-exports these contracts and supplies the
//! producer/client/builder ergonomics around them.

pub mod adapter;
pub mod timebase;

pub use adapter::{adapter_event_to_otk, AdapterError, AdapterEvent, AdapterState, DetectorAdapter};
pub use timebase::{
    Timebase, TimebaseError, TimebaseEvent, TimebaseKind, TimebaseMetadataEvent, TimebaseState,
};
