//! Transport-independent OTK ingest protocol state machine.
//!
//! This crate is the server-side, transport-agnostic core of OTK ingest:
//! given a stream of decoded [`OtkEnvelope`] values (produced by `frame-codec`
//! over any transport), it negotiates the handshake and dispatches subsequent
//! envelopes into a small set of inbound actions.
//!
//! Per-transport ingest adapters (`adapter-ingest-tcp`,
//! `adapter-ingest-unix-socket`, …) are reduced to: socket lifecycle, byte I/O,
//! frame-codec wiring, and translating the actions this crate yields into
//! [`port_in_ingest::IngestSession`] behavior. None of them re-implement the
//! handshake or message-type dispatch.
//!
//! # Lifecycle
//!
//! ```text
//! envelope --> ServerHandshake::process --> Accepted{ack, processor}
//!                                          \-> Rejected{reply, reason}
//!
//! envelope --> processor.process --> InboundAction::Event(otk_event)
//!                                  \-> InboundAction::Heartbeat   (ignore, keep reading)
//!                                  \-> InboundAction::Disconnect  (close cleanly)
//! ```
//!
//! Adapters send the handshake's reply envelope as the first outbound frame
//! and use the returned [`PostHandshakeProcessor`] to handle every envelope
//! that follows.
//!
//! [`OtkEnvelope`]: protocol::OtkEnvelope
//! [`port_in_ingest::IngestSession`]: https://docs.rs/port-in-ingest

pub mod error;
pub mod handshake;
pub mod processor;

pub use error::{HandshakeError, ProtocolError};
pub use handshake::{
    perform_server_handshake, perform_server_handshake_with_auth, AllowAll, ConnectAuthoriser,
    HandshakeOutcome,
};
pub use processor::{InboundAction, PostHandshakeProcessor};
