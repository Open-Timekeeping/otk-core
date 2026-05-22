use std::fmt;

use crate::offset::Offset;

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
    ///
    /// [`EventLog::append`]: crate::EventLog::append
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

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
}
