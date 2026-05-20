use alloc::borrow::ToOwned;
use alloc::string::String;
use minicbor::{Decode, Encode};

macro_rules! string_id {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        #[cbor(transparent)]
        #[cfg_attr(feature = "serde", serde(transparent))]
        pub struct $name(#[n(0)] String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl core::fmt::Display for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_owned())
            }
        }
    };
}

string_id!(
    /// Identifies a specific detector device or adapter instance.
    DetectorId
);

string_id!(
    /// Identifies a physical timing point (start/finish line, sector boundary, checkpoint).
    TimingPointId
);

string_id!(
    /// Identifies the subject being timed (transponder, bib number, vehicle ID, etc.).
    SubjectId
);

string_id!(
    /// Identifies a timebase (upstream physical time reference).
    TimebaseId
);

string_id!(
    /// Uniquely identifies a single Detection event.
    DetectionId
);

string_id!(
    /// Uniquely identifies a single Crossing event.
    CrossingId
);

string_id!(
    /// Identifies a stream (topic) within the Timing Fabric.
    StreamId
);

string_id!(
    /// Identifies an operator who initiated a manual detection.
    OperatorId
);
