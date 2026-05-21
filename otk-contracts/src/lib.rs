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
//! # Features
//!
//! - **`std`** (default): enables `From<std::io::Error>` conversions on the
//!   contract's error types for ergonomic `?`-propagation from `std`-using
//!   adapters. The error variants themselves carry `String`, not
//!   `std::io::Error`, so disabling the feature lets embedded firmware
//!   consumers construct `*::Io(String)` directly. The trait surface itself
//!   currently still requires `std` through `async-trait`'s `Box<dyn Future>`
//!   desugaring; full `no_std + alloc` support for the traits themselves is
//!   tracked in `spec/open-questions.md`.
//!
//! The `otk-sdk` crate re-exports these contracts and supplies the
//! producer/client/builder ergonomics around them.

pub mod adapter;
pub mod timebase;

pub use adapter::{adapter_event_to_otk, AdapterError, AdapterEvent, AdapterState, DetectorAdapter};
pub use timebase::{
    Timebase, TimebaseError, TimebaseEvent, TimebaseKind, TimebaseMetadataEvent, TimebaseState,
};
