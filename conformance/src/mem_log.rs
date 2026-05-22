//! In-memory reference implementation of [`EventLog`].
//!
//! Not intended for production; exists so the conformance suite can exercise
//! the `EventLog` contract without a real backend, and so adapter authors have
//! a known-good behavioural reference to diff against.

use std::pin::pin;
use std::sync::Arc;

use async_trait::async_trait;
use event_model::OtkEvent;
use port_out_event_log::{EventLog, LogEntry, LogSubscription, Offset, StorageError};
use tokio::sync::Notify;

struct Inner {
    entries: Vec<LogEntry>,
    /// Offsets strictly below this have been evicted by simulated retention.
    /// `None` = nothing evicted.
    earliest_retained: Option<u64>,
    notify: Arc<Notify>,
}

/// Compute `earliest_available` for a `RetentionExpired` error from a
/// retention boundary and the current `entries.len()`.
///
/// Returns `Some(Offset::new(earliest))` when the boundary points at a
/// retained entry, and `None` when it points past the last entry (the
/// fully-evicted case). Handles the `u64` → `usize` truncation that's
/// theoretically possible on 32-bit (or any non-64-bit) targets: if
/// `earliest` doesn't fit in `usize`, it can't be inside `entries`, so the
/// answer is unambiguously `None`.
fn earliest_available_offset(earliest: u64, entries_len: usize) -> Option<Offset> {
    match usize::try_from(earliest) {
        Ok(e) if e < entries_len => Some(Offset::new(earliest)),
        _ => None,
    }
}

/// An in-memory event log used by the conformance suite.
///
/// Implements [`EventLog`] with a `Vec`-backed buffer and an optional simulated
/// retention low-water mark so conformance tests can exercise the
/// [`StorageError::RetentionExpired`] paths without a real on-disk backend.
pub struct MemLog {
    inner: Arc<tokio::sync::Mutex<Inner>>,
}

impl MemLog {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(Inner {
                entries: Vec::new(),
                earliest_retained: None,
                notify: Arc::new(Notify::new()),
            })),
        }
    }

    /// Simulate retention eviction: every offset strictly below `boundary` is
    /// considered evicted. Subsequent reads or subscribes that target an
    /// evicted offset return [`StorageError::RetentionExpired`].
    ///
    /// In-flight subscriptions are also affected: a subscription whose
    /// cursor has fallen below the new boundary surfaces
    /// `RetentionExpired` on the next `next_entry()` call, matching the
    /// behaviour real backends (e.g. `adapter-event-log-segment`) exhibit
    /// when retention runs past an active reader. Subscribers ahead of the
    /// boundary continue normally.
    ///
    /// `boundary` is clamped to `min(boundary, next_offset)` where
    /// `next_offset = entries.len()`. Without this clamp, callers could
    /// move the retention boundary past the writable end of the log, after
    /// which every subsequent append would land at an offset below
    /// `earliest_retained` and be immediately "evicted" on read, effectively
    /// soft-bricking the log. Real backends can't reach this state because
    /// retention enforcement runs as a consequence of appending, not as an
    /// independent operation; the helper here is explicit so tests stay
    /// deterministic, and the clamp keeps it harmless.
    ///
    /// Test-only helper. Backends like the segment log enforce retention
    /// implicitly via segment deletion; the in-memory log here is explicit
    /// so tests are deterministic. After moving the boundary, the notify
    /// is rung so any parked subscribers wake to discover the change.
    pub async fn evict_below(&self, boundary: Offset) {
        let mut inner = self.inner.lock().await;
        let next_offset = inner.entries.len() as u64;
        let effective = boundary.as_u64().min(next_offset);
        inner.earliest_retained = Some(effective);
        inner.notify.notify_waiters();
    }

    /// Simulate complete eviction (every event removed by retention).
    ///
    /// Like [`evict_below`](Self::evict_below), this affects in-flight
    /// subscriptions: any subscription with an unread cursor surfaces
    /// `RetentionExpired { earliest_available: None }` on the next
    /// `next_entry()` call.
    pub async fn evict_all(&self) {
        let mut inner = self.inner.lock().await;
        let next = inner.entries.len() as u64;
        inner.earliest_retained = Some(next);
        inner.notify.notify_waiters();
    }
}

impl Default for MemLog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventLog for MemLog {
    async fn append(&mut self, events: &[OtkEvent]) -> Result<Offset, StorageError> {
        if events.is_empty() {
            return Err(StorageError::InvalidInput("events slice must not be empty".into()));
        }
        let mut inner = self.inner.lock().await;
        let start = inner.entries.len() as u64;
        for (i, ev) in events.iter().enumerate() {
            inner.entries.push(LogEntry {
                offset: Offset::new(start + i as u64),
                event: ev.clone(),
                appended_at_ns: now_ns(),
            });
        }
        let last = Offset::new(start + events.len() as u64 - 1);
        inner.notify.notify_waiters();
        Ok(last)
    }

    async fn read_range(
        &mut self,
        from: Offset,
        to: Option<Offset>,
    ) -> Result<Vec<LogEntry>, StorageError> {
        let inner = self.inner.lock().await;
        let from_u64 = from.as_u64();

        // Retention check takes precedence (per EventLog::read_range contract).
        if let Some(earliest) = inner.earliest_retained {
            if from_u64 < earliest {
                return Err(StorageError::RetentionExpired {
                    requested: from,
                    earliest_available: earliest_available_offset(earliest, inner.entries.len()),
                });
            }
        }

        // Convert u64 offsets to usize indices via try_from. If `from` doesn't
        // fit usize (only possible on a non-64-bit target with a very large
        // offset), it's definitionally past the end of any Vec we could hold,
        // so the range is empty. Same logic for `to`, except we clamp rather
        // than early-return so a finite `from..too-large-to` range still
        // returns the entries from `from` onward.
        let from_idx = match usize::try_from(from_u64) {
            Ok(i) => i,
            Err(_) => return Ok(vec![]),
        };
        if from_idx >= inner.entries.len() {
            return Ok(vec![]);
        }
        let to_idx = match to {
            None => inner.entries.len(),
            Some(o) => usize::try_from(o.as_u64()).unwrap_or(usize::MAX),
        };
        let to_idx = to_idx.min(inner.entries.len());
        if to_idx <= from_idx {
            return Ok(vec![]);
        }
        Ok(inner.entries[from_idx..to_idx].to_vec())
    }

    async fn latest_offset(&mut self) -> Result<Option<Offset>, StorageError> {
        let inner = self.inner.lock().await;
        if inner.entries.is_empty() {
            return Ok(None);
        }
        // Fully-evicted: earliest_retained points past the last entry.
        // earliest_available_offset() returns None for that case.
        if let Some(earliest) = inner.earliest_retained {
            if earliest_available_offset(earliest, inner.entries.len()).is_none() {
                return Ok(None);
            }
        }
        Ok(Some(Offset::new(inner.entries.len() as u64 - 1)))
    }

    async fn earliest_offset(&mut self) -> Result<Option<Offset>, StorageError> {
        let inner = self.inner.lock().await;
        if inner.entries.is_empty() {
            return Ok(None);
        }
        if let Some(earliest) = inner.earliest_retained {
            return Ok(earliest_available_offset(earliest, inner.entries.len()));
        }
        Ok(Some(Offset::new(0)))
    }

    async fn subscribe(
        &mut self,
        from: Offset,
    ) -> Result<Box<dyn LogSubscription>, StorageError> {
        let inner = self.inner.lock().await;
        let from_u64 = from.as_u64();
        if let Some(earliest) = inner.earliest_retained {
            if from_u64 < earliest {
                return Err(StorageError::RetentionExpired {
                    requested: from,
                    earliest_available: earliest_available_offset(earliest, inner.entries.len()),
                });
            }
        }
        drop(inner);
        // Convert `from` to a usize cursor. If `from` doesn't fit usize
        // (only possible on a non-64-bit target with a very large offset),
        // the subscription is asking to start past any position we could
        // ever serve. Per the EventLog::subscribe contract, "If `from` is
        // ahead of the latest appended offset ... the subscription waits
        // for events at `from` to be appended before delivering anything;
        // no backfill occurs." Saturating to usize::MAX gives exactly that:
        // cursor < entries.len() can never be true.
        let cursor = usize::try_from(from_u64).unwrap_or(usize::MAX);
        Ok(Box::new(MemSubscription {
            inner: Arc::clone(&self.inner),
            cursor,
            closed: false,
        }))
    }
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

struct MemSubscription {
    inner: Arc<tokio::sync::Mutex<Inner>>,
    cursor: usize,
    closed: bool,
}

#[async_trait]
impl LogSubscription for MemSubscription {
    async fn next_entry(&mut self) -> Option<Result<LogEntry, StorageError>> {
        if self.closed {
            return None;
        }
        loop {
            // Step 1: grab the notify Arc out from under the lock so we can
            // construct a Notified future after dropping the lock. The lock
            // is dropped here.
            let notify = {
                let inner = self.inner.lock().await;

                // Retention check before reading: if the cursor has fallen
                // behind a retention boundary set since this subscription
                // was created, surface RetentionExpired. Real backends drop
                // in-flight readers the same way when segment deletion
                // overtakes them.
                if let Some(earliest) = inner.earliest_retained {
                    if (self.cursor as u64) < earliest {
                        return Some(Err(StorageError::RetentionExpired {
                            requested: Offset::new(self.cursor as u64),
                            earliest_available: earliest_available_offset(
                                earliest,
                                inner.entries.len(),
                            ),
                        }));
                    }
                }

                if self.cursor < inner.entries.len() {
                    let entry = inner.entries[self.cursor].clone();
                    self.cursor += 1;
                    return Some(Ok(entry));
                }
                Arc::clone(&inner.notify)
            };

            // Step 2: register interest BEFORE re-checking under the lock.
            // tokio::sync::Notify::notify_waiters() only wakes waiters that
            // are already registered and stores no permit; without enable()
            // here, an append between Step 1's check and the .await below
            // would be lost and the subscription would block forever even
            // though new entries exist.
            //
            // After enable(), any subsequent notify_waiters() call wakes
            // this future even before it's polled to await.
            // Stable `std::pin::pin!` rather than `tokio::pin!` so this
            // crate doesn't need to enable tokio's `macros` feature just
            // for one pin call.
            let mut notified = pin!(notify.notified());
            notified.as_mut().enable();

            // Step 3: re-check state under the lock to catch any signal
            // (append OR eviction) that raced between Step 1 dropping the
            // lock and Step 2 registering interest. notify_waiters fired
            // from either append() or evict_*() in that window is not
            // remembered (we weren't registered yet), so we must re-check
            // both conditions synchronously here. If entries grew, loop
            // back to serve. If the retention boundary moved past our
            // cursor, surface RetentionExpired immediately rather than
            // awaiting a notification that may never come.
            {
                let inner = self.inner.lock().await;
                if let Some(earliest) = inner.earliest_retained {
                    if (self.cursor as u64) < earliest {
                        return Some(Err(StorageError::RetentionExpired {
                            requested: Offset::new(self.cursor as u64),
                            earliest_available: earliest_available_offset(
                                earliest,
                                inner.entries.len(),
                            ),
                        }));
                    }
                }
                if self.cursor < inner.entries.len() {
                    continue;
                }
            }
            if self.closed {
                return None;
            }

            // Step 4: now safe to await. Any notify_waiters() since Step 2
            // has either been consumed by re-check in Step 3 (loop again)
            // or is queued for this future and will wake it.
            notified.await;
            if self.closed {
                return None;
            }
        }
    }

    async fn close(&mut self) -> Result<(), StorageError> {
        self.closed = true;
        // Wake any sibling subscriptions that may be parked on notify so
        // shutdown paths don't leave them blocked. Cheap; no-op if no one
        // is waiting. (next_entry on THIS subscription can't be in flight
        // concurrently because the trait takes &mut self exclusively, so
        // this is purely defensive for other subscriptions watching the
        // same MemLog.)
        let inner = self.inner.lock().await;
        inner.notify.notify_waiters();
        Ok(())
    }
}
