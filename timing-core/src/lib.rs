//! Timing-domain engine, port contracts, and application services for OTK.
//!
//! `timing-core` is the hexagon. It owns:
//!
//! - **Domain** ([`domain`]): the timing-domain types and engines.
//!   [`Crossing`] (derived passage record), [`CrossingProcessor`]
//!   (detection-to-crossing grouping), [`SequenceGate`] (per-`(producer,
//!   detector)` sequence-number monotonicity with restart-resume via
//!   [`seed_from_log`] / [`seed_from_log_box`]). Other domain primitives
//!   (`Detection`, `SubjectId`, `TimingPointId`, etc.) live in the
//!   wire-schema crate `event-model` and are imported as-is until a future
//!   split separates wire from domain types.
//! - **Ports** ([`ports`]): the typed interfaces on the hexagon's edge.
//!   [`ports::inbound`] holds the ports the core implements
//!   ([`EventIngestPort`], [`EventQueryPort`]); [`ports::outbound`] holds
//!   the ports the core consumes ([`EventLog`], [`IngestMetrics`]). Adapter
//!   crates implement the outbound ports; the API layer depends on the
//!   inbound ports.
//! - **Application services** ([`services`]): [`EventIngestService`]
//!   stitches the domain together with the injected outbound ports and
//!   implements the read-only [`EventQueryPort`] the API layer depends on.
//!
//! # Adapter boundary
//!
//! Adapter crates (`adapter-event-log-segment`, `adapter-ingest-tcp`,
//! `adapter-ingest-unix-socket`) depend on `timing-core` but must reach
//! only into [`ports`]. Per-adapter `clippy.toml` files deny imports of
//! `timing_core::domain::*` and `timing_core::services::*` at CI; the
//! runtime composition root (`timing-node`) is the only crate that
//! constructs services and threads domain types.
//!
//! # Usage shape
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use timing_core::{
//!     ProcessorConfig, SequenceGate,
//!     ports::outbound::{EventLog, IngestMetrics, NoopIngestMetrics},
//!     services::EventIngestService,
//! };
//!
//! async fn wire_up(log: Box<dyn EventLog>) {
//!     let gate = Arc::new(SequenceGate::new());
//!     let metrics: Arc<dyn IngestMetrics> = Arc::new(NoopIngestMetrics);
//!     let service = EventIngestService::new(
//!         log,
//!         ProcessorConfig::default(),
//!         gate,
//!         metrics,
//!     );
//!     // ... hand `service` to ingest listeners and the API layer ...
//!     drop(service);
//! }
//! ```
//!
//! [`Crossing`]: domain::Crossing
//! [`CrossingProcessor`]: domain::CrossingProcessor
//! [`SequenceGate`]: domain::SequenceGate
//! [`seed_from_log`]: domain::seed_from_log
//! [`seed_from_log_box`]: domain::seed_from_log_box
//! [`EventIngestPort`]: ports::inbound::EventIngestPort
//! [`EventQueryPort`]: ports::inbound::EventQueryPort
//! [`EventLog`]: ports::outbound::EventLog
//! [`IngestMetrics`]: ports::outbound::IngestMetrics
//! [`EventIngestService`]: services::EventIngestService

pub mod domain;
pub mod ports;
pub mod services;

#[cfg(test)]
pub(crate) mod testing;

// Convenience re-exports at the crate root so common combinations stay
// terse. Anything an adapter is supposed to touch (i.e. only items in
// `ports::*`) should be reachable via `timing_core::Foo` here; the
// per-adapter clippy fence on `timing_core::domain::*` and
// `timing_core::services::*` covers the rest.
pub use domain::{
    seed_from_log, seed_from_log_box, Crossing, CrossingId, CrossingProcessor, GateDecision,
    ProcessorConfig, SequenceGate,
};
pub use ports::inbound::{
    EventEntry, EventIngestPort, EventPage, EventQueryPort, EventStream, IncomingEvent,
    IngestError, IngestSession, QueryError,
};
pub use ports::outbound::{
    EventLog, IngestMetrics, LogEntry, LogSubscription, NoopIngestMetrics, Offset, RetentionPolicy,
    StorageError,
};
pub use services::{AppendOutcome, EventIngestService};
