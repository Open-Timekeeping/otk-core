//! Inbound port contract for OTK event ingestion.
//!
//! This crate defines the typed boundary between the timing node and its
//! transport adapters. The timing node calls `EventIngestPort::accept` to
//! obtain sessions; concrete adapters (e.g. `adapter-ingest-tcp`) implement
//! the traits and own all framing, CBOR decoding, and handshake mechanics.
//!
//! # Roles
//!
//! - `EventIngestPort`: server-side listener; accept sessions from producers.
//! - `IngestSession`: a single connected producer; poll `next_event` until done.
//! - `IngestError`: error vocabulary for both accept and session operations.

pub mod error;
pub mod port;
pub mod session;

pub use error::IngestError;
pub use port::EventIngestPort;
pub use session::IngestSession;
