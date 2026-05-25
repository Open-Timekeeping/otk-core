//! Inbound (driving) ports for `timing-core`.
//!
//! These are the interfaces external callers use to drive the core: ingest
//! sessions from transport adapters, and read-only queries from the API
//! layer. The core implements both; adapters and the API layer depend on
//! these traits, not on the application service's concrete type.

pub mod ingest;
pub mod query;

pub use ingest::{EventIngestPort, IncomingEvent, IngestError, IngestSession};
pub use query::{EventEntry, EventPage, EventQueryPort, EventStream, QueryError};
