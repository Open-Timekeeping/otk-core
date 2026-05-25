//! Outbound port: the persistence contract every event-log backend implements.
//!
//! This module defines the boundary between `timing-core` and its storage
//! backends. Storage is pluggable: [`crate::services::EventIngestService`]
//! depends on the [`EventLog`] trait, not on any particular backend. For v0,
//! the only shipped backend is `adapter-event-log-segment`.
//!
//! # What a storage backend provides
//!
//! An **event log** is an append-only sequence of [`OtkEvent`] values, each
//! assigned a monotonic [`Offset`] by the backend. Consumers reconnect by
//! supplying the offset after the last one they successfully processed
//! (`last.checked_next().expect("offset exhausted")`); the log replays from
//! that point or returns
//! [`StorageError::RetentionExpired`] if the range has been evicted.
//!
//! # Key types
//!
//! - [`EventLog`]: the core persistence trait. Lifecycle: append events,
//!   read ranges, subscribe to live delivery, query bounds.
//! - [`LogSubscription`]: a live subscription returned by [`EventLog::subscribe`].
//!   Poll [`LogSubscription::next_entry`] until `None` (closed) or `Some(Err(_))`
//!   (terminal error); call [`LogSubscription::close`] after either.
//! - [`LogEntry`]: a stored event with its [`Offset`] and receipt timestamp.
//! - [`Offset`]: a monotonic `u64` position in the log.
//! - [`RetentionPolicy`]: how long the backend retains old entries.
//! - [`StorageError`]: error vocabulary, including the structured
//!   [`StorageError::RetentionExpired`] variant for reads of evicted ranges.
//!
//! # Design
//!
//! **Poll-based subscriptions.** [`LogSubscription::next_entry`] follows the
//! same poll-until-`None` pattern as `DetectorAdapter::next_event` and
//! `Timebase::next_event` in `otk-contracts`.
//!
//! **`&mut self` methods.** All [`EventLog`] and [`LogSubscription`] methods
//! take `&mut self`. The runtime wraps the backend in a `Mutex` or similar
//! if it needs to share access across tasks.
//!
//! **std-only.** Async traits via `async-trait` require `std`.

use std::fmt;

use async_trait::async_trait;
use event_model::OtkEvent;

// ── Offset ────────────────────────────────────────────────────────────────

/// A monotonic position in the event log, assigned by the backend on append.
///
/// Offsets are strictly increasing: each appended event receives a higher
/// offset than all preceding events. The first event in a non-empty log has
/// offset 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(u64);

impl Offset {
    /// Construct an `Offset` from a raw `u64`.
    pub fn new(v: u64) -> Self {
        Self(v)
    }

    /// Return the underlying `u64`.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Return the next offset (`self + 1`), or `None` if `self` is `u64::MAX`.
    ///
    /// Use this when resuming after the last successfully processed offset to
    /// avoid unchecked arithmetic: `last.checked_next()` instead of
    /// `Offset::new(last.as_u64() + 1)`.
    pub fn checked_next(self) -> Option<Offset> {
        self.0.checked_add(1).map(Offset::new)
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── LogEntry ──────────────────────────────────────────────────────────────

/// A stored event together with its log position, receipt timestamp, and
/// the id of the producer that delivered (or triggered) it.
///
/// # `producer_id` semantics
///
/// `producer_id` records *which producer's process caused this entry to be
/// written*. For a `Detection` event arriving over the wire, that is the
/// producer that sent it. For a `Crossing` event synthesized server-side
/// by `timing-core` from one or more producer-supplied detections, the
/// `producer_id` is inherited from the originating detection's producer:
/// runtime-synthesized events are not given a synthetic producer id at
/// the storage layer (see the runtime metrics for that distinction).
///
/// This field is the source of truth used by the runtime's sequence gate
/// to rebuild its per-`(producer_id, detector_id)` high-water marks on
/// startup. Without it, restart-resume idempotence would not be possible.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// The position of this entry in the log.
    pub offset: Offset,

    /// Wall-clock time at which this entry was appended, in nanoseconds since
    /// the Unix epoch. Assigned by the backend on append.
    pub appended_at_ns: u64,

    /// The producer that delivered or triggered this entry. See the struct
    /// doc for the Detection-vs-Crossing semantics.
    pub producer_id: String,

    /// The canonical event.
    pub event: OtkEvent,
}

// ── RetentionPolicy ───────────────────────────────────────────────────────

/// Policy controlling how long the event log retains old entries.
///
/// Backends enforce this on the append path and during periodic compaction.
/// When an entry falls outside the retention window it is deleted and any
/// subsequent read that targets its offset returns
/// [`StorageError::RetentionExpired`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionPolicy {
    /// Keep all events indefinitely. Disk usage grows without bound.
    Indefinite,

    /// Retain events for at most this many seconds after they were appended.
    TimeBased { max_age_secs: u64 },

    /// Retain events up to approximately this many bytes of stored event data.
    ///
    /// The exact byte accounting is backend-defined (it typically covers
    /// serialized event payloads; index and filesystem overhead may or may not
    /// be included). Treat this as an advisory budget, not a hard guarantee.
    SizeBased { max_bytes: u64 },

    /// Enforce both a time limit and a size limit; whichever is exceeded first
    /// triggers eviction. `max_bytes` uses the same advisory byte accounting
    /// as [`Self::SizeBased`].
    Hybrid { max_age_secs: u64, max_bytes: u64 },
}

// ── StorageError ──────────────────────────────────────────────────────────

/// Errors that can occur during storage operations.
#[derive(Debug)]
pub enum StorageError {
    /// The requested range is outside the retained window.
    ///
    /// The consumer should re-establish its position at `earliest_available`
    /// (if `Some`) and accept that events before that offset are permanently
    /// unavailable. `None` means the retained window is empty: either all
    /// events have been evicted, or the backend cannot distinguish the
    /// requested offset from one beyond the historical high-water mark after
    /// full compaction.
    RetentionExpired {
        requested: Offset,
        earliest_available: Option<Offset>,
    },

    /// The caller passed invalid input (for example, an empty events slice to
    /// [`EventLog::append`]).
    InvalidInput(String),

    /// An underlying I/O error not covered by a more specific variant.
    /// Preserves the original [`std::io::Error`] as the error source.
    Io(std::io::Error),

    /// The log data is structurally corrupt and cannot be read.
    Corrupted(String),

    /// Invalid or missing configuration.
    Configuration(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RetentionExpired {
                requested,
                earliest_available: Some(ea),
            } => write!(
                f,
                "offset {requested} is before earliest retained offset {ea}"
            ),
            Self::RetentionExpired {
                requested,
                earliest_available: None,
            } => write!(
                f,
                "offset {requested} is not available: no retained events remain"
            ),
            Self::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Corrupted(msg) => write!(f, "log corruption: {msg}"),
            Self::Configuration(msg) => write!(f, "configuration error: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

// ── LogSubscription + EventLog traits ─────────────────────────────────────

/// A live subscription to the event log.
///
/// Created by [`EventLog::subscribe`]. Poll [`Self::next_entry`] until it returns
/// `None` (subscription closed) or `Some(Err(_))` (terminal error); both
/// states are terminal. After receiving either, call [`Self::close`] so the backend
/// can release internal resources. To terminate before a natural end state,
/// call [`Self::close`] eagerly instead of waiting for `next_entry` to return `None`.
#[async_trait]
pub trait LogSubscription: Send {
    /// Poll for the next log entry.
    ///
    /// Suspends until an entry is available or the subscription is closed.
    /// Returns `None` when the subscription has been closed and will produce
    /// no more entries.
    ///
    /// An error returned by this method is terminal: the subscription will
    /// produce no more entries. Callers should call [`Self::close`] after receiving
    /// an error. If retention advances past the next undelivered offset while
    /// the subscription is running, `next_entry` returns
    /// `Some(Err(StorageError::RetentionExpired { .. }))`; this is also terminal.
    async fn next_entry(&mut self) -> Option<Result<LogEntry, StorageError>>;

    /// Close this subscription.
    ///
    /// The subscription will produce no more entries after this returns.
    ///
    /// `close` is idempotent: calling it after the subscription has already
    /// ended (returned `None` or a terminal error) is valid and returns `Ok(())`.
    ///
    /// `close` must not be called while a `next_entry` future is pending. The
    /// `&mut self` receiver prevents concurrent calls; drop or abort the
    /// in-progress future first.
    async fn close(&mut self) -> Result<(), StorageError>;
}

/// The persistence contract for the OTK event log.
///
/// Implemented by every storage backend. The runtime appends events as
/// they arrive, serves range reads and live subscriptions to downstream
/// consumers, and enforces the configured retention policy.
///
/// # Lifecycle
///
/// 1. Append events as they arrive via [`Self::append`].
/// 2. Serve range reads for reconnecting consumers via [`Self::read_range`].
///    Returns [`StorageError::RetentionExpired`] if the requested range
///    has been evicted; the consumer must re-establish its position.
/// 3. Serve live subscriptions via [`Self::subscribe`]; the caller polls
///    [`LogSubscription::next_entry`] until it returns `None` or
///    `Some(Err(_))`, then calls [`LogSubscription::close`].
/// 4. Query current bounds via [`Self::latest_offset`] and [`Self::earliest_offset`].
#[async_trait]
pub trait EventLog: Send {
    /// Append one or more events. Returns the offset of the last appended event.
    ///
    /// `producer_id` is the producer that delivered (or triggered) every
    /// event in this batch. It is persisted alongside each event so the
    /// runtime can rebuild per-`(producer_id, detector_id)` state (notably
    /// the sequence-gate high-water marks) on restart. A batch is always
    /// single-producer in practice: the pipeline composes a detection and
    /// the crossings it triggered into one append for atomicity, and the
    /// crossings inherit the originating producer's id. See
    /// [`LogEntry::producer_id`] for the semantics.
    ///
    /// The events slice must not be empty; passing an empty slice returns
    /// [`StorageError::InvalidInput`]. `producer_id` must not be empty
    /// either; empty strings are rejected with `InvalidInput` because they
    /// would collapse every producer-less event together in the gate's
    /// keyspace and silently break replay detection.
    ///
    /// Append is atomic: either all events in the slice are committed with
    /// consecutive monotonically increasing offsets, or none are and an error
    /// is returned. Partial appends are not permitted. Offsets are assigned in
    /// input order: `events[0]` receives the lowest new offset and each
    /// subsequent event receives the next consecutive offset.
    ///
    /// The durability guarantee on success is backend-defined. Backends should
    /// document whether `Ok` implies fsync-level persistence or acceptance into
    /// OS buffers.
    async fn append(
        &mut self,
        producer_id: &str,
        events: &[OtkEvent],
    ) -> Result<Offset, StorageError>;

    /// Read entries in `[from, to)`. If `to` is `None`, reads through the
    /// latest available offset.
    ///
    /// `from` is the first offset to include. To resume after the last
    /// successfully processed entry at offset `N`, pass
    /// `N.checked_next().expect("offset exhausted")` (`u64` can hold 18
    /// quintillion events; exhaustion is impossible in practice).
    ///
    /// Returns [`StorageError::RetentionExpired`] if `from` is before the
    /// earliest retained offset. This check takes precedence: a call such as
    /// `read_range(Offset::new(5), Some(Offset::new(5)))` returns
    /// `RetentionExpired` if offset 5 has been evicted, even though
    /// `[5, 5)` would otherwise be empty.
    ///
    /// If `to <= from` and `from` is within the retained window, returns
    /// `Ok(vec![])`.
    ///
    /// If the log was never populated, returns `Ok(vec![])` for any `from`.
    /// If all entries have been evicted (the log was populated but retention
    /// removed everything), returns `RetentionExpired { earliest_available: None }`
    /// for any `from`. Backends are not required to distinguish offsets beyond
    /// the historical high-water mark from those below it after full compaction.
    ///
    /// If `from` is beyond the latest retained offset but the log is not empty,
    /// returns `Ok(vec![])`.
    ///
    /// Entries in the returned `Vec` are in ascending offset order.
    ///
    /// When `to` is `None` and the log contains many entries, all retained
    /// entries are materialized into memory. Callers replaying large logs should
    /// bound requests with an explicit `to`. A streaming or paginated variant is
    /// a planned addition (see `spec/open-questions.md`).
    async fn read_range(
        &mut self,
        from: Offset,
        to: Option<Offset>,
    ) -> Result<Vec<LogEntry>, StorageError>;

    /// The offset of the most recently appended event that is still within the
    /// retention window. `None` if no retained entries remain (either the log
    /// was never populated, or retention has evicted all entries).
    async fn latest_offset(&mut self) -> Result<Option<Offset>, StorageError>;

    /// The earliest offset still within the retention window. `None` if the log is empty.
    async fn earliest_offset(&mut self) -> Result<Option<Offset>, StorageError>;

    /// Subscribe to entries starting at `from` (inclusive).
    ///
    /// `from` is the first offset to deliver. To resume after the last
    /// successfully processed entry at offset `N`, pass
    /// `N.checked_next().expect("offset exhausted")` (`u64` can hold 18
    /// quintillion events; exhaustion is impossible in practice).
    ///
    /// Backfills from disk if `from` is behind the live tail, then switches to
    /// live delivery. If `from` is ahead of the latest appended offset, or if
    /// the log was never populated, the subscription waits for events at `from`
    /// to be appended before delivering anything; no backfill occurs. The
    /// returned subscription is polled with [`LogSubscription::next_entry`]
    /// until it returns `None` or `Some(Err(_))`.
    ///
    /// Entries are delivered in ascending offset order without gaps. In the
    /// backfill phase, entries are served from disk in order; in the live phase,
    /// each new event is delivered at the next consecutive offset.
    ///
    /// Returns [`StorageError::RetentionExpired`] if `from` is before the
    /// earliest retained offset, or if the log is fully evicted (populated but
    /// all entries removed by retention), returning
    /// `RetentionExpired { earliest_available: None }`.
    async fn subscribe(&mut self, from: Offset) -> Result<Box<dyn LogSubscription>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    // ── Offset ────────────────────────────────────────────────────────────

    #[test]
    fn offset_ordering() {
        assert!(Offset::new(0) < Offset::new(1));
        assert!(Offset::new(42) >= Offset::new(42));
        assert_eq!(Offset::new(7), Offset::new(7));
        assert_ne!(Offset::new(1), Offset::new(2));
    }

    #[test]
    fn offset_roundtrip() {
        let o = Offset::new(99);
        assert_eq!(o.as_u64(), 99);
        assert_eq!(Offset::new(o.as_u64()), o);
    }

    #[test]
    fn offset_display() {
        assert_eq!(Offset::new(0).to_string(), "0");
        assert_eq!(Offset::new(42).to_string(), "42");
        assert_eq!(Offset::new(u64::MAX).to_string(), u64::MAX.to_string());
    }

    #[test]
    fn checked_next_normal() {
        assert_eq!(Offset::new(0).checked_next(), Some(Offset::new(1)));
        assert_eq!(Offset::new(99).checked_next(), Some(Offset::new(100)));
    }

    #[test]
    fn checked_next_at_max() {
        assert_eq!(Offset::new(u64::MAX).checked_next(), None);
    }

    // ── StorageError ──────────────────────────────────────────────────────

    #[test]
    fn from_io_maps_to_io_variant() {
        let e = io::Error::new(io::ErrorKind::UnexpectedEof, "eof");
        let err = StorageError::from(e);
        assert!(matches!(err, StorageError::Io(_)));
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn io_display() {
        let e = StorageError::Io(io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe"));
        assert_eq!(e.to_string(), "I/O error: broken pipe");
    }

    #[test]
    fn retention_expired_with_earliest_display() {
        let e = StorageError::RetentionExpired {
            requested: Offset::new(5),
            earliest_available: Some(Offset::new(100)),
        };
        assert_eq!(
            e.to_string(),
            "offset 5 is before earliest retained offset 100"
        );
    }

    #[test]
    fn retention_expired_fully_evicted_display() {
        let e = StorageError::RetentionExpired {
            requested: Offset::new(5),
            earliest_available: None,
        };
        assert_eq!(
            e.to_string(),
            "offset 5 is not available: no retained events remain"
        );
    }

    #[test]
    fn invalid_input_display() {
        let e = StorageError::InvalidInput("events slice must not be empty".into());
        assert_eq!(
            e.to_string(),
            "invalid input: events slice must not be empty"
        );
    }

    #[test]
    fn corrupted_display() {
        let e = StorageError::Corrupted("torn write at offset 42".into());
        assert_eq!(e.to_string(), "log corruption: torn write at offset 42");
    }

    #[test]
    fn configuration_display() {
        let e = StorageError::Configuration("missing segment directory".into());
        assert_eq!(
            e.to_string(),
            "configuration error: missing segment directory"
        );
    }

    // ── RetentionPolicy ───────────────────────────────────────────────────

    #[test]
    fn retention_policy_equality() {
        assert_eq!(RetentionPolicy::Indefinite, RetentionPolicy::Indefinite);
        assert_ne!(
            RetentionPolicy::Indefinite,
            RetentionPolicy::TimeBased { max_age_secs: 3600 }
        );
        assert_eq!(
            RetentionPolicy::Hybrid {
                max_age_secs: 3600,
                max_bytes: 1_000_000
            },
            RetentionPolicy::Hybrid {
                max_age_secs: 3600,
                max_bytes: 1_000_000
            },
        );
        assert_ne!(
            RetentionPolicy::SizeBased { max_bytes: 100 },
            RetentionPolicy::SizeBased { max_bytes: 200 },
        );
    }

    #[test]
    fn retention_policy_variants_are_constructible() {
        let _ = [
            RetentionPolicy::Indefinite,
            RetentionPolicy::TimeBased {
                max_age_secs: 86400,
            },
            RetentionPolicy::SizeBased {
                max_bytes: 1_073_741_824,
            },
            RetentionPolicy::Hybrid {
                max_age_secs: 3600,
                max_bytes: 500_000_000,
            },
        ];
    }

    // ── dyn-safety of EventLog / LogSubscription ─────────────────────────

    struct MockLog;
    struct MockSub;

    #[async_trait]
    impl LogSubscription for MockSub {
        async fn next_entry(&mut self) -> Option<Result<LogEntry, StorageError>> {
            None
        }
        async fn close(&mut self) -> Result<(), StorageError> {
            Ok(())
        }
    }

    #[async_trait]
    impl EventLog for MockLog {
        async fn append(
            &mut self,
            _producer_id: &str,
            _events: &[OtkEvent],
        ) -> Result<Offset, StorageError> {
            Ok(Offset::new(0))
        }
        async fn read_range(
            &mut self,
            _from: Offset,
            _to: Option<Offset>,
        ) -> Result<Vec<LogEntry>, StorageError> {
            Ok(vec![])
        }
        async fn latest_offset(&mut self) -> Result<Option<Offset>, StorageError> {
            Ok(None)
        }
        async fn earliest_offset(&mut self) -> Result<Option<Offset>, StorageError> {
            Ok(None)
        }
        async fn subscribe(
            &mut self,
            _from: Offset,
        ) -> Result<Box<dyn LogSubscription>, StorageError> {
            Ok(Box::new(MockSub))
        }
    }

    #[tokio::test]
    async fn event_log_is_dyn_safe() {
        let mut log: Box<dyn EventLog> = Box::new(MockLog);
        assert!(log.latest_offset().await.unwrap().is_none());
        assert!(log.earliest_offset().await.unwrap().is_none());
        assert!(log
            .read_range(Offset::new(0), None)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn log_subscription_is_dyn_safe() {
        let mut log: Box<dyn EventLog> = Box::new(MockLog);
        let mut sub = log.subscribe(Offset::new(0)).await.unwrap();
        assert!(sub.next_entry().await.is_none());
        sub.close().await.unwrap();
    }
}
