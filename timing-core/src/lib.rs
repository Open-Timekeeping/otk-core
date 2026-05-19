//! Timing-domain engine for OTK.
//!
//! This crate converts raw [`Detection`] events into [`Crossing`] events: the primary
//! derived timing record representing one passage of a subject across a timing point.
//!
//! # Domain scope
//!
//! `timing-core` knows about subjects, timing points, detections, and crossings. It does
//! not know about laps, races, flags, or any sport-specific structure. Those concerns
//! belong to application-layer code built on top of this library.
//!
//! # Usage
//!
//! ```rust,ignore
//! use timing_core::{CrossingProcessor, ProcessorConfig};
//!
//! let mut processor = CrossingProcessor::new(ProcessorConfig::default());
//!
//! // Feed detections as they arrive
//! for detection in incoming {
//!     for crossing in processor.push_detection(detection) {
//!         // handle committed crossing
//!     }
//! }
//!
//! // Commit any remaining groups at end of session
//! for crossing in processor.flush() {
//!     // handle final crossings
//! }
//! ```
//!
//! [`Detection`]: event_model::Detection

mod config;
mod crossing;
mod processor;

pub use config::ProcessorConfig;
pub use crossing::{Crossing, CrossingId};
pub use processor::CrossingProcessor;
