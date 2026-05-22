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
//! # `no_std`
//!
//! The crate is `#![no_std]` with `extern crate alloc;` so firmware adapter
//! implementations can depend on it without a `std` runtime. `async_trait`'s
//! `Box<dyn Future + Send>` desugaring needs `alloc`, which firmware targets
//! already have via `alloc`.
//!
//! # Features
//!
//! - **`std`** (default): enables `From<std::io::Error>` conversions on the
//!   contract's error types for ergonomic `?`-propagation from `std`-using
//!   adapters. The error variants themselves carry `String`, so embedded
//!   consumers that disable the feature can construct `*::Io(String)`
//!   directly.
//!
//! The `otk-sdk` crate re-exports these contracts and supplies the
//! producer/client/builder ergonomics around them.

#![no_std]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

pub mod adapter;
pub mod timebase;

pub use adapter::{
    adapter_event_to_otk, AdapterError, AdapterEvent, AdapterState, DetectorAdapter,
};
pub use timebase::{
    Timebase, TimebaseError, TimebaseEvent, TimebaseKind, TimebaseMetadataEvent, TimebaseState,
};
