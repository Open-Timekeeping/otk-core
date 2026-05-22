//! Re-exports of the timebase contract from [`otk_contracts`].
//!
//! See the [`adapter`] module for the rationale behind this split.
//!
//! [`adapter`]: super::adapter

pub use otk_contracts::timebase::{
    Timebase, TimebaseError, TimebaseEvent, TimebaseKind, TimebaseMetadataEvent, TimebaseState,
};
