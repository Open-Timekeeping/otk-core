//! OTK Frame Codec: encode and decode OTK messages as byte frames.
//!
//! This crate is the frame codec layer of the OTK protocol stack. It turns
//! [`OtkEnvelope`] values from [`protocol`] into byte frames ready for a
//! transport binding, and parses incoming bytes back into envelopes.
//!
//! # Two frame formats
//!
//! **Stream frames** (reliable transports: TCP, Unix socket):
//! A big-endian `u32` length prefix followed by the CBOR-encoded envelope.
//! No checksum: the transport provides integrity.
//!
//! ```text
//! +------------------+------------------------------+
//! |  length (u32 BE) |   CBOR OtkEnvelope bytes     |
//! +------------------+------------------------------+
//! ```
//!
//! **Serial frames** (unreliable transports: UART, RS-232, RS-485):
//! `COBS(cbor_bytes || CRC-16/CCITT-FALSE) || 0x00`.
//! COBS removes all zero bytes so `0x00` unambiguously marks end-of-frame.
//! CRC-16/CCITT-FALSE (polynomial 0x1021, init 0xFFFF) is appended in
//! big-endian byte order before COBS encoding to detect corruption.
//!
//! ```text
//! +--------------------------------------------------+-------+
//! |  COBS( CBOR OtkEnvelope || CRC-16/CCITT-FALSE) | 0x00  |
//! +--------------------------------------------------+-------+
//! ```
//!
//! # Usage
//!
//! Encoding a stream frame:
//! ```ignore
//! let frame = frame_codec::encode_stream(&envelope, frame_codec::DEFAULT_MAX_FRAME_SIZE)?;
//! transport.write_all(&frame)?;
//! ```
//!
//! Decoding stream frames incrementally:
//! ```ignore
//! let mut dec = frame_codec::StreamFrameDecoder::new(frame_codec::DEFAULT_MAX_FRAME_SIZE);
//! for result in dec.push(&incoming_bytes) {
//!     let envelope = result?;
//!     // handle envelope
//! }
//! ```
//!
//! # no_std
//!
//! This crate is `no_std` with `alloc`. Embedded firmware and `adapter-ingest-serial`
//! can both depend on it without pulling in the full server stack.
//!
//! [`OtkEnvelope`]: protocol::OtkEnvelope

#![no_std]
extern crate alloc;

pub mod error;
pub mod serial;
pub mod stream;

pub use error::FrameError;
pub use serial::{SerialFrameDecoder, crc16_ccitt_false, encode_serial};
pub use stream::{StreamFrameDecoder, encode_stream};
pub use protocol::OtkEnvelope;

/// Default maximum frame payload size in bytes.
///
/// Callers may use a smaller value for embedded targets with limited RAM.
/// Both the encoder functions and decoder constructors accept a `max_frame_size`
/// parameter.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 65_535;
