extern crate alloc;

use alloc::borrow::ToOwned;
use alloc::string::String;
use minicbor::{Decode, Encode};

macro_rules! string_id {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
        #[cbor(transparent)]
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
    /// Identifies the adapter process or firmware unit that is the source of messages in this session.
    ProducerId
);

string_id!(
    /// Links a request message to its response (e.g., Connect to ConnectAck/ConnectReject).
    CorrelationId
);
