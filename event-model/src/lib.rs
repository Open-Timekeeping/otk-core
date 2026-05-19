//! Canonical Open Timekeeping event types and identifiers.
//!
//! This crate is the Event Model layer of the OTK Protocol stack. Every event flowing
//! through the Timing Fabric is described by a type defined here.
//!
//! # Design
//!
//! One event shape (`Detection`) is used for all timing observations regardless of
//! resolution level. The stream (topic) a `Detection` is published on carries the
//! semantic level:
//!
//! - raw stream: one event per sensor pulse
//! - detections stream: one event per passage, firmware or adapter processed
//! - processed stream: timing-core output, possibly consolidated across detectors
//!
//! Sensor-specific metadata is carried in the `SensorData` enum field on `Detection`.
//!
//! # Separation of mechanism and policy
//!
//! Events carry what happened and how trustworthy it is. This crate does not filter
//! or suppress events based on quality. Policy on acceptable timestamping methods,
//! sync states, or stream access is decided at the app, scorer, or operator layer.

#![no_std]
extern crate alloc;

pub mod detection;
pub mod health;
pub mod ids;
pub mod metadata;
pub mod stream;
pub mod timestamp;

pub use detection::{Detection, SensorData};
pub use health::{DetectorHealthEvent, DetectorHealthStatus, TimebaseStatusEvent};
pub use ids::{DetectionId, DetectorId, OperatorId, StreamId, SubjectId, TimebaseId, TimingPointId};
pub use metadata::{AdapterCapabilities, AdapterMetadataEvent};
pub use stream::{StreamDescriptor, StreamKind};
pub use timestamp::{SourceAttestation, SyncState, TimestampingMethod};

use minicbor::{Decode, Encode};

/// Top-level event envelope. Every event kind in the Timing Fabric is a variant here.
/// This is the type the wire-protocol layer wraps in its message envelope.
#[derive(Debug, Clone, Encode, Decode)]
pub enum OtkEvent {
    #[n(0)]
    Detection(#[n(0)] Detection),

    #[n(1)]
    DetectorHealth(#[n(0)] DetectorHealthEvent),

    #[n(2)]
    TimebaseStatus(#[n(0)] TimebaseStatusEvent),

    #[n(3)]
    AdapterMetadata(#[n(0)] AdapterMetadataEvent),
}
