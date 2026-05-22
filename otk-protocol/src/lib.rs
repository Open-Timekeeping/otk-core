//! OTK Wire Protocol: message envelope and protocol-level semantics.
//!
//! This crate is the Wire Protocol layer of the OTK Protocol stack. It wraps
//! canonical Open Timekeeping events ([`event_model::OtkEvent`]) in an envelope
//! that carries protocol-level metadata: version, message type, producer identity,
//! stream routing, sequence numbers, and correlation IDs for request/response pairs.
//!
//! # Scope
//!
//! This crate owns:
//! - [`OtkEnvelope`]: the on-wire header wrapping every OTK message.
//! - [`MessageType`]: the full catalog of protocol-level message kinds.
//! - Handshake messages: [`Connect`], [`ConnectAck`], [`ConnectReject`].
//! - Keep-alive: [`Heartbeat`].
//! - Error reporting: [`ErrorMessage`], [`ErrorCode`].
//! - [`OtkMessage`]: the decoded, typed form of an envelope's payload.
//!
//! Frame encoding (length-prefixing, writing bytes to a link) is the concern of `adapter-ingest-tcp`
//! (server side) and `embedded-wire` (firmware side). Transport binding (sockets, serial, USB) is
//! the concern of `port-in-ingest` and its adapter implementations (`adapter-ingest-tcp`, etc.).
//!
//! # Protocol version
//!
//! The current version is [`PROTOCOL_VERSION`]. Producers advertise a `[min, max]` range
//! in [`Connect`]; the server picks the highest mutually supported version and confirms it
//! in [`ConnectAck`].
//!
//! # Acknowledgement model
//!
//! Silence means success. The server only sends messages on the event channel when something
//! is wrong ([`ErrorMessage`]). Per-event acks are not used; loss detection is the producer's
//! responsibility via sequence-number gap analysis. The handshake ([`Connect`] / [`ConnectAck`])
//! is the only mandatory request/response exchange.
//!
//! # Plugin path
//!
//! This protocol is for process-boundary communication only. Adapters compiled into the same
//! process as the timing-node use a Rust trait defined in `plugin-api` and produce
//! [`event_model::OtkEvent`] values directly, with no envelope overhead.

#![no_std]
extern crate alloc;

pub mod envelope;
pub mod error;
pub mod handshake;
pub mod heartbeat;
pub mod ids;
pub mod message;

pub use envelope::{MessageType, OtkEnvelope};
pub use error::{ErrorCode, ErrorMessage};
pub use handshake::{Connect, ConnectAck, ConnectReject, ConnectRejectReason};
pub use heartbeat::Heartbeat;
pub use ids::{CorrelationId, ProducerId};
pub use message::OtkMessage;

/// The OTK wire protocol version implemented by this crate.
pub const PROTOCOL_VERSION: u8 = 0;
