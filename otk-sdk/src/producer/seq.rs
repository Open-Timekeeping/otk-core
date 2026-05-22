use std::sync::atomic::{AtomicU64, Ordering};

/// Per-detector monotonically incrementing sequence counter.
///
/// Thread-safe; share via `Arc<SequenceCounter>` if needed.
/// Each call to `next` returns a unique value starting from 0.
pub struct SequenceCounter(AtomicU64);

impl SequenceCounter {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn next(&self) -> u64 {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .expect("sequence counter overflow")
    }

    /// Returns the next value that will be returned by [`next`](Self::next),
    /// without advancing the counter.
    pub fn peek_next(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Default for SequenceCounter {
    fn default() -> Self {
        Self::new()
    }
}
