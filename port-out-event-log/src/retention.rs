/// Policy controlling how long the event log retains old entries.
///
/// Backends enforce this on the append path and during periodic compaction.
/// When an entry falls outside the retention window it is deleted and any
/// subsequent read that targets its offset returns
/// [`StorageError::RetentionExpired`].
///
/// [`StorageError::RetentionExpired`]: crate::StorageError::RetentionExpired
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
    /// as [`SizeBased`].
    Hybrid { max_age_secs: u64, max_bytes: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_policy_equality() {
        assert_eq!(RetentionPolicy::Indefinite, RetentionPolicy::Indefinite);
        assert_ne!(
            RetentionPolicy::Indefinite,
            RetentionPolicy::TimeBased { max_age_secs: 3600 }
        );
        assert_eq!(
            RetentionPolicy::Hybrid { max_age_secs: 3600, max_bytes: 1_000_000 },
            RetentionPolicy::Hybrid { max_age_secs: 3600, max_bytes: 1_000_000 },
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
            RetentionPolicy::TimeBased { max_age_secs: 86400 },
            RetentionPolicy::SizeBased { max_bytes: 1_073_741_824 },
            RetentionPolicy::Hybrid {
                max_age_secs: 3600,
                max_bytes: 500_000_000,
            },
        ];
    }
}
