//! TCP ingest adapter for OTK.
//!
//! Implements `port-in-ingest` over plain TCP. This crate is intentionally
//! thin: it owns TCP socket lifecycle, accept loop, and per-session byte I/O,
//! and delegates every protocol concern upward.
//!
//! - **Framing** (length-prefix, oversize handling, partial-read buffering) is
//!   handled by [`frame_codec::StreamFrameDecoder`].
//! - **Handshake and post-handshake envelope dispatch** are handled by
//!   [`ingest_protocol`]: handshake negotiation, version/source validation,
//!   message-type dispatch into the small [`ingest_protocol::InboundAction`]
//!   vocabulary.
//!
//! Adding a new transport binding (Unix socket, USB CDC, …) is a new crate
//! that wraps a different I/O source around the same two upstream crates.

pub mod config;
mod port;
mod session;

pub use config::{TcpIngestConfig, DEFAULT_MAX_FRAME_BYTES};
pub use port::TcpIngestPort;
