//! Canonical test inputs for the Open Timekeeping conformance suite.
//!
//! This crate is **data only**. It carries no harness, no assertions, no
//! tokio runtime. The companion [`conformance`] crate is the harness; an
//! adapter or runtime author can also depend on this crate directly to
//! exercise their own implementation against the same fixtures
//! `conformance` uses.
//!
//! ## Why a separate crate
//!
//! `conformance-fixtures` is the contract-shaped surface. Anyone wanting
//! a known-good corpus of canonical events, envelopes, and replay streams
//! can compile against this without inheriting `conformance`'s test
//! harness dependencies (async runtime, mocks, etc.). Implementations in
//! other languages (a future TypeScript SDK, firmware) can read the same
//! fixtures by porting the constructors or by snapshotting their
//! CBOR-encoded outputs as a binary corpus.
//!
//! ## Modules
//!
//! - [`detections`]: [`Detection`](event_model::Detection) constructors
//!   for common physical-layer scenarios (beam break, loop transponder
//!   with / without RSSI).
//! - [`events`]: [`OtkEvent`](event_model::OtkEvent) wrappers plus
//!   `canon::*` exhaustive examples (one of each variant) for round-trip
//!   and exhaustiveness tests.
//! - [`envelopes`]: [`OtkEnvelope`](otk_protocol::OtkEnvelope) builders
//!   for the wire-protocol layer (Connect, Connect-with-token, generic
//!   data envelopes).
//! - [`streams`]: small multi-event scenarios that exercise behaviour
//!   spanning more than one event (happy path, reconnect-with-replay).
//!
//! ## Scope at v0
//!
//! Today this crate ships a *starter* corpus, enough to deduplicate the
//! fixtures previously inlined across `conformance`'s test files. The
//! full corpus the README scopes (timebase degradation scenarios,
//! multi-detector races, pit-lane / start-finish topology fixtures)
//! lands incrementally; the public API of this crate is the entry point
//! for adding more.
//!
//! [`conformance`]: https://github.com/Open-Timekeeping/open-timekeeping/tree/main/conformance

pub mod detections;
pub mod envelopes;
pub mod events;
pub mod streams;
