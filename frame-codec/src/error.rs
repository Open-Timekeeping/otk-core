use minicbor::decode;

/// Errors that can occur during frame encoding or decoding.
#[derive(Debug)]
pub enum FrameError {
    /// The encoded or declared frame payload length exceeds the configured maximum.
    /// `len` is `Some(exact)` when the length is known (encode path, stream length
    /// prefix); `None` when the true length is unknown (serial oversize detected
    /// mid-stream before the `0x00` delimiter).
    OversizeFrame { len: Option<usize>, max: usize },
    /// CRC-16/CCITT-FALSE mismatch on a serial frame; the frame is corrupt.
    CorruptFrame,
    /// COBS decoding failed; the frame boundary was lost.
    LostSync,
    /// CBOR encoding of the envelope failed.
    EncodeFailed,
    /// CBOR decoding of the frame payload failed: the frame was structurally
    /// valid (correct length / CRC) but its contents could not be parsed as
    /// an [`OtkEnvelope`].
    ///
    /// [`OtkEnvelope`]: protocol::OtkEnvelope
    DecodeFailed(decode::Error),
}

impl core::fmt::Display for FrameError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OversizeFrame { len: Some(len), max } => {
                write!(f, "frame too large: {len} bytes exceeds maximum of {max}")
            }
            Self::OversizeFrame { len: None, max } => {
                write!(f, "frame too large: exceeded maximum of {max} bytes before delimiter")
            }
            Self::CorruptFrame => f.write_str("corrupt frame: CRC-16/CCITT-FALSE mismatch"),
            Self::LostSync => f.write_str("lost sync: COBS decoding failed"),
            Self::EncodeFailed => f.write_str("CBOR encoding of envelope failed"),
            Self::DecodeFailed(e) => write!(f, "CBOR decoding of envelope failed: {e}"),
        }
    }
}

impl core::error::Error for FrameError {}
