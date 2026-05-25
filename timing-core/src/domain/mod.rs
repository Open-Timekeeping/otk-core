//! Domain layer: the hexagon's interior.
//!
//! Pure timing-domain logic. No I/O, no transport awareness, no port
//! implementations. Application services in [`crate::services`] orchestrate
//! these types against outbound ports the composition root injects.

pub mod crossing;
pub mod crossing_processor;
pub mod processor_config;
pub mod sequence_gate;

pub use crossing::{Crossing, CrossingId};
pub use crossing_processor::CrossingProcessor;
pub use processor_config::ProcessorConfig;
pub use sequence_gate::{seed_from_log, seed_from_log_box, GateDecision, SequenceGate};
