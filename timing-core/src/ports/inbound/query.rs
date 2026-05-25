//! Inbound port: read-only query access to the event log.
//!
//! This module defines the typed boundary between the API layer (REST + SSE in
//! `timing-node`) and the application service that owns the event log
//! ([`crate::services::EventIngestService`] at v0). The API layer depends only
//! on [`EventQueryPort`]; it has no direct dependency on the storage adapter or
//! on the application service's concrete type.
//!
//! # Roles
//!
//! - [`EventQueryPort`]: read-only access to the event log. Used by `/api/v1/status`,
//!   `/api/v1/events`, and `/api/v1/events/stream`, and by readiness probes
//!   (`/readyz`) that need a quick storage-reachability check.
//! - [`QueryError`]: API-shaped error vocabulary. Storage backend errors are
//!   mapped to these variants at the service boundary so the API layer never
//!   sees implementation details of any particular event-log backend.
//! - [`EventEntry`] / [`EventPage`] / [`EventStream`]: the value types the port
//!   returns. `serde::Serialize` is derived so the API layer can `Json(page)`
//!   them directly.

use std::pin::Pin;

use event_model::OtkEvent;
use futures_util::Stream;
use serde::Serialize;

/// One persisted event paired with the offset it was assigned in the log.
#[derive(Debug, Serialize)]
pub struct EventEntry {
    pub offset: u64,
    pub event: OtkEvent,
}

/// A page of [`EventEntry`] returned by [`EventQueryPort::read_events`].
///
/// `latest_offset` is the highest offset currently persisted at the moment
/// the page was assembled. Callers can compare it against their cursor to
/// detect whether they have caught up.
#[derive(Debug, Serialize)]
pub struct EventPage {
    pub entries: Vec<EventEntry>,
    pub latest_offset: Option<u64>,
}

/// API-shaped error vocabulary for the query port.
///
/// The query port deliberately does not leak the storage backend's error type.
/// Backend errors are mapped to these variants at the application-service
/// boundary so the API layer can map them to HTTP status codes without
/// depending on any specific storage implementation.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// The requested offset is no longer available because the segment that
    /// held it has been evicted by the retention policy. `earliest_available`
    /// is the lowest offset the caller can re-anchor on, if known.
    #[error(
        "requested offset {requested} is not available (earliest_available={earliest_available:?})"
    )]
    RetentionExpired {
        requested: u64,
        earliest_available: Option<u64>,
    },
    /// The requested resource is not present. Not currently produced by any
    /// query operation; reserved for future per-entity lookups.
    #[error("not found")]
    NotFound,
    /// Storage backend error. The detail string is opaque to the API layer
    /// and is intended for logging server-side; HTTP responses should map
    /// this to a generic 5xx with no backend-specific text echoed to the
    /// client.
    #[error("internal error: {0}")]
    Internal(String),
}

/// A live stream of [`EventEntry`] from [`EventQueryPort::subscribe_events`].
///
/// Each item is one entry (or a terminal error). The stream ends when the
/// caller drops it or when the underlying subscription resolves to `None`
/// (e.g. the log was closed).
pub type EventStream = Pin<Box<dyn Stream<Item = Result<EventEntry, QueryError>> + Send>>;

/// Read-only query port for the event log.
///
/// Implemented at v0 by [`crate::services::EventIngestService`]. The API
/// layer holds an `Arc<dyn EventQueryPort>` and depends on nothing else from
/// the service module, so swapping the backing service (for example, an
/// offline read-only viewer) is a one-line wiring change at the composition
/// root.
#[async_trait::async_trait]
pub trait EventQueryPort: Send + Sync {
    /// Highest offset currently persisted, or `None` if the log is empty.
    async fn latest_offset(&self) -> Result<Option<u64>, QueryError>;

    /// Read up to `limit` entries starting at `from` (inclusive). Returns
    /// an [`EventPage`] including the highest offset visible at read time.
    async fn read_events(&self, from: u64, limit: usize) -> Result<EventPage, QueryError>;

    /// Subscribe to a live tail of the log starting at `from` (inclusive).
    /// The returned [`EventStream`] yields entries as they are appended and
    /// terminates when the caller drops it.
    async fn subscribe_events(&self, from: u64) -> Result<EventStream, QueryError>;
}
