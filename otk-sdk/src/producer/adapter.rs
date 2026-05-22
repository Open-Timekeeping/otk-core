//! Re-exports of the detector-adapter contract from [`otk_contracts`].
//!
//! The trait and supporting types live in [`otk_contracts`] so that
//! third-party adapter authors can implement against a dependency-light
//! contract crate, without pulling in the SDK's transport / encoder stack.
//! The SDK re-exports them here so existing consumers continue to work
//! unchanged.

pub use otk_contracts::adapter::{
    adapter_event_to_otk, AdapterError, AdapterEvent, AdapterState, DetectorAdapter,
};
