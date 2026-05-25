//! Unix-socket ingest adapter for OTK.
//!
//! Implements `timing_core::ports::inbound` over a local AF_UNIX listener. Same protocol
//! and framing as TCP — bytes pumped through [`frame_codec::StreamFrameDecoder`]
//! and dispatched by [`ingest_protocol::PostHandshakeProcessor`] — only the
//! transport differs.
//!
//! Existence is the proof that the M2 refactor of `adapter-ingest-tcp` left
//! reusable layers behind: a second transport adapter is a few hundred lines
//! of socket lifecycle code, not a copy of the framing or handshake logic.
//!
//! # Platform support
//!
//! Compiled on Unix targets only. Tokio's `UnixListener` / `UnixStream` are
//! `#[cfg(unix)]`; on Windows the crate compiles to an empty stub so the
//! workspace builds, but the public types do not exist. Integration runs on
//! Linux / macOS.

#[cfg(unix)]
pub mod config;
#[cfg(unix)]
mod port;
#[cfg(unix)]
mod session;

#[cfg(unix)]
pub use config::UnixSocketIngestConfig;
#[cfg(unix)]
pub use port::UnixSocketIngestPort;
