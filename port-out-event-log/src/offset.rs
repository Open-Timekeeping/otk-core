use std::fmt;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
