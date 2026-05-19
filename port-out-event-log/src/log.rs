use async_trait::async_trait;
use event_model::OtkEvent;

use crate::entry::LogEntry;
use crate::error::StorageError;
use crate::offset::Offset;

/// A live subscription to the event log.
///
/// Created by [`EventLog::subscribe`]. Poll [`next_entry`] until it returns
/// `None` (subscription closed) or `Some(Err(_))` (terminal error); both
/// states are terminal. After receiving either, call [`close`] so the backend
/// can release internal resources. To terminate before a natural end state,
/// call [`close`] eagerly instead of waiting for `next_entry` to return `None`.
///
/// [`next_entry`]: LogSubscription::next_entry
/// [`close`]: LogSubscription::close
#[async_trait]
pub trait LogSubscription: Send {
    /// Poll for the next log entry.
    ///
    /// Suspends until an entry is available or the subscription is closed.
    /// Returns `None` when the subscription has been closed and will produce
    /// no more entries.
    ///
    /// An error returned by this method is terminal: the subscription will
    /// produce no more entries. Callers should call [`close`] after receiving
    /// an error. If retention advances past the next undelivered offset while
    /// the subscription is running, `next_entry` returns
    /// `Some(Err(StorageError::RetentionExpired { .. }))`; this is also terminal.
    ///
    /// [`close`]: LogSubscription::close
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
/// Implemented by every storage backend. The runtime node appends events as
/// they arrive, serves range reads and live subscriptions to downstream
/// consumers, and enforces the configured retention policy.
///
/// # Lifecycle
///
/// 1. Append events as they arrive via [`append`].
/// 2. Serve range reads for reconnecting consumers via [`read_range`].
///    Returns [`StorageError::RetentionExpired`] if the requested range
///    has been evicted; the consumer must re-establish its position.
/// 3. Serve live subscriptions via [`subscribe`]; the caller polls
///    [`LogSubscription::next_entry`] until it returns `None` or
///    `Some(Err(_))`, then calls [`LogSubscription::close`].
/// 4. Query current bounds via [`latest_offset`] and [`earliest_offset`].
///
/// [`append`]: EventLog::append
/// [`read_range`]: EventLog::read_range
/// [`subscribe`]: EventLog::subscribe
/// [`latest_offset`]: EventLog::latest_offset
/// [`earliest_offset`]: EventLog::earliest_offset
#[async_trait]
pub trait EventLog: Send {
    /// Append one or more events. Returns the offset of the last appended event.
    ///
    /// The events slice must not be empty; passing an empty slice returns
    /// [`StorageError::InvalidInput`].
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
    async fn append(&mut self, events: &[OtkEvent]) -> Result<Offset, StorageError>;

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
    /// a planned addition (see the open questions section in the README).
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
    async fn subscribe(
        &mut self,
        from: Offset,
    ) -> Result<Box<dyn LogSubscription>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

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
        async fn append(&mut self, _events: &[OtkEvent]) -> Result<Offset, StorageError> {
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
        assert!(log.read_range(Offset::new(0), None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn log_subscription_is_dyn_safe() {
        let mut log: Box<dyn EventLog> = Box::new(MockLog);
        let mut sub = log.subscribe(Offset::new(0)).await.unwrap();
        assert!(sub.next_entry().await.is_none());
        sub.close().await.unwrap();
    }
}
