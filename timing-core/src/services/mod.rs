//! Application services.
//!
//! Services in this module orchestrate the domain (`crate::domain`) against
//! injected outbound ports (`crate::ports::outbound`) and expose their
//! public surface via inbound ports (`crate::ports::inbound`). Composition
//! roots (`timing-node` at v0; an offline analyzer or replay tool later)
//! build the adapters, construct one of these services, and route inbound
//! traffic to it.
//!
//! At v0 there is one service, [`EventIngestService`], which owns the
//! end-to-end peek/append/commit dance for incoming events.

pub mod event_ingest;

pub use event_ingest::{AppendOutcome, EventIngestService};
