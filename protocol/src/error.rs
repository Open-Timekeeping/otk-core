use alloc::string::String;
use minicbor::{Decode, Encode};

/// Error notification sent by the server when it cannot process a received envelope.
///
/// Under the error-only acknowledgement model, silence means success. The server only sends
/// this message when a specific problem is detected. The producer does not wait for per-event
/// acknowledgements; it monitors for `ErrorMessage` and detects loss via sequence-number gaps
/// in its own stream state.
#[derive(Debug, Clone, Encode, Decode)]
pub struct ErrorMessage {
    #[n(0)]
    pub code: ErrorCode,

    /// Human-readable description. `None` when the error code is self-explanatory.
    #[n(1)]
    pub message: Option<String>,

    /// Sequence number of the envelope that triggered this error, if applicable.
    /// Since sequence numbers are per-stream, the enclosing `OtkEnvelope.stream_id`
    /// identifies which stream this sequence belongs to when this field is `Some`.
    #[n(2)]
    pub related_sequence: Option<u64>,
}

/// Machine-readable error codes for [`ErrorMessage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum ErrorCode {
    /// The envelope header could not be CBOR-decoded.
    #[n(0)]
    MalformedEnvelope,
    /// The `stream_id` in the envelope is not registered with this server.
    #[n(1)]
    UnknownStream,
    /// The payload bytes could not be decoded as the expected type for this `message_type`.
    #[n(2)]
    PayloadDecodeFailed,
    /// The server detected a sequence-number gap for this stream.
    #[n(3)]
    SequenceGapDetected,
    /// The producer is not authorized to publish to the given stream.
    #[n(4)]
    Unauthorized,
    /// An unexpected internal error occurred on the server.
    #[n(5)]
    InternalError,
}
