use event_model::{OtkEvent, StreamDescriptor};
use otk_protocol::{
    ids::ProducerId, Connect, ConnectAck, ConnectReject, Heartbeat, MessageType, OtkEnvelope,
    PROTOCOL_VERSION,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::producer::error::ProducerError;
use crate::producer::time::now_ns;
use crate::producer::transport::Transport;

/// Marker trait so the producer can hold either a plain `TcpStream` or
/// a `TlsStream<TcpStream>` behind one `Box`. Producer traffic is low
/// per connection (one detector's worth of events), so the per-byte
/// vtable cost on the boxed stream is well below the noise floor; the
/// type-erasure pays for itself in API simplicity.
pub trait ProducerStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> ProducerStream for T {}

const MAX_FRAME_BYTES: u32 = 65_535;

/// Configuration for connecting to a timing node as a producer.
///
/// `#[non_exhaustive]`: construct via [`ProducerConfig::new`] (or
/// [`ProducerConfig::default`]) and chain `with_*` setters for any
/// non-default fields. New fields can then be added in minor releases
/// without breaking downstream code.
///
/// ```no_run
/// # use otk_sdk::producer::ProducerConfig;
/// let config = ProducerConfig::new("loop-adapter-1")
///     .with_max_frame_bytes(32_768)
///     .with_auth_token("my-shared-secret");
/// ```
///
/// `Debug` is implemented manually to redact `auth_token`, so debug-printing a
/// `ProducerConfig` (in logs, panic messages, telemetry, `tracing` events,
/// etc.) does not leak the credential. Whether the token is set is still
/// shown, since that's a useful debugging signal that doesn't disclose value.
#[derive(Clone)]
#[non_exhaustive]
pub struct ProducerConfig {
    /// Stable identifier for this producer (e.g. `"loop-adapter-1"`).
    pub producer_id: String,
    /// Streams this producer intends to publish. Sent during handshake.
    pub streams: Vec<StreamDescriptor>,
    /// Maximum frame payload size accepted from the server. Default and ceiling: 65535
    /// (the protocol maximum). Values above 65535 are silently clamped down. Must not
    /// be zero; `connect` returns `ProducerError::Config` if set to zero.
    pub max_frame_bytes: u32,
    /// Optional auth credential sent in the `Connect`. `None` for nodes that don't
    /// require auth (the default).
    pub auth_token: Option<String>,
}

impl ProducerConfig {
    /// Construct a config with the given producer id and defaults for the rest.
    pub fn new(producer_id: impl Into<String>) -> Self {
        Self {
            producer_id: producer_id.into(),
            ..Self::default()
        }
    }

    pub fn with_streams(mut self, streams: Vec<StreamDescriptor>) -> Self {
        self.streams = streams;
        self
    }

    pub fn with_max_frame_bytes(mut self, max_frame_bytes: u32) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    /// Set the auth token sent in the `Connect`.
    ///
    /// Use [`without_auth_token`](Self::without_auth_token) to clear it.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Explicitly clear the auth token (e.g. after copy-modifying an existing config).
    pub fn without_auth_token(mut self) -> Self {
        self.auth_token = None;
        self
    }
}

impl std::fmt::Debug for ProducerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerConfig")
            .field("producer_id", &self.producer_id)
            .field("streams", &self.streams)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            producer_id: "otk-producer".into(),
            streams: vec![],
            max_frame_bytes: MAX_FRAME_BYTES,
            auth_token: None,
        }
    }
}

/// A connected producer session with a timing node.
///
/// Created via `Producer::connect`. Drives `send_event` to publish `OtkEvent`
/// values; call `disconnect` for a graceful shutdown.
pub struct Producer {
    stream: Box<dyn ProducerStream>,
    producer_id: ProducerId,
    next_seq: u64,
}

impl Producer {
    /// Connect to the timing node at `transport` and complete the OTK handshake.
    pub async fn connect(
        transport: Transport,
        config: ProducerConfig,
    ) -> Result<Self, ProducerError> {
        if config.max_frame_bytes == 0 {
            return Err(ProducerError::Config(
                "max_frame_bytes must not be zero".into(),
            ));
        }
        let max_frame_bytes = config.max_frame_bytes;
        let mut stream: Box<dyn ProducerStream> = match transport {
            Transport::Tcp(addr) => Box::new(TcpStream::connect(addr).await?),
            #[cfg(feature = "producer-tls")]
            Transport::Tls { addr, config } => {
                let tls = crate::producer::tls::connect_tls(addr, &config).await?;
                Box::new(tls)
            }
        };

        let producer_id = ProducerId::from(config.producer_id.as_str());

        let connect = Connect {
            protocol_version_min: PROTOCOL_VERSION,
            protocol_version_max: PROTOCOL_VERSION,
            streams: config.streams,
            auth_token: config.auth_token,
        };
        let connect_payload = minicbor::to_vec(&connect)
            .map_err(|e| ProducerError::Encode(format!("Connect encode: {e}")))?;

        let env = make_envelope(&producer_id, MessageType::Connect, Some(connect_payload));
        send_frame(&mut stream, &encode_envelope(&env)?).await?;

        let response_bytes = recv_frame(&mut stream, max_frame_bytes.min(MAX_FRAME_BYTES)).await?;
        let response: OtkEnvelope = minicbor::decode(&response_bytes)
            .map_err(|e| ProducerError::Decode(format!("handshake response decode: {e}")))?;

        if response.protocol_version != PROTOCOL_VERSION {
            return Err(ProducerError::Handshake(format!(
                "server responded with protocol version {} but client offered only {PROTOCOL_VERSION}",
                response.protocol_version
            )));
        }

        match response.message_type {
            MessageType::ConnectAck => {
                let payload = response
                    .payload
                    .ok_or_else(|| ProducerError::Handshake("ConnectAck has no payload".into()))?;
                let ack: ConnectAck = minicbor::decode(&payload)
                    .map_err(|e| ProducerError::Decode(format!("ConnectAck decode: {e}")))?;
                if ack.negotiated_version != PROTOCOL_VERSION {
                    return Err(ProducerError::Handshake(format!(
                        "server negotiated version {} but client offered only {PROTOCOL_VERSION}",
                        ack.negotiated_version
                    )));
                }
                Ok(Self {
                    stream,
                    producer_id,
                    next_seq: 0,
                })
            }
            MessageType::ConnectReject => {
                let payload = response.payload.ok_or_else(|| {
                    ProducerError::Handshake("ConnectReject has no payload".into())
                })?;
                let reject: ConnectReject = minicbor::decode(&payload)
                    .map_err(|e| ProducerError::Decode(format!("ConnectReject decode: {e}")))?;
                Err(ProducerError::Rejected {
                    reason: reject.reason,
                    server_min: reject.supported_version_min,
                    server_max: reject.supported_version_max,
                })
            }
            other => Err(ProducerError::Handshake(format!(
                "expected ConnectAck or ConnectReject, got {other:?}"
            ))),
        }
    }

    /// Publish one event to the timing node. Sequence numbers are auto-assigned.
    pub async fn send_event(&mut self, event: OtkEvent) -> Result<(), ProducerError> {
        let seq = self.next_seq;
        let payload = minicbor::to_vec(&event)
            .map_err(|e| ProducerError::Encode(format!("OtkEvent encode: {e}")))?;
        let mut env = make_envelope(&self.producer_id, MessageType::Event, Some(payload));
        env.sequence_number = Some(seq);
        send_frame(&mut self.stream, &encode_envelope(&env)?).await?;
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .expect("sequence number overflow: u64 exhausted after 2^64 events");
        Ok(())
    }

    /// Send a keep-alive heartbeat.
    pub async fn send_heartbeat(&mut self) -> Result<(), ProducerError> {
        let hb = Heartbeat {
            sent_at_ns: now_ns(),
        };
        let payload = minicbor::to_vec(hb)
            .map_err(|e| ProducerError::Encode(format!("Heartbeat encode: {e}")))?;
        let env = make_envelope(&self.producer_id, MessageType::Heartbeat, Some(payload));
        send_frame(&mut self.stream, &encode_envelope(&env)?).await?;
        Ok(())
    }

    /// Send a graceful `Disconnect` and close the connection.
    pub async fn disconnect(mut self) -> Result<(), ProducerError> {
        let env = make_envelope(&self.producer_id, MessageType::Disconnect, None);
        send_frame(&mut self.stream, &encode_envelope(&env)?).await?;
        Ok(())
    }
}

// ── framing helpers ──────────────────────────────────────────────────────────

fn make_envelope(
    producer_id: &ProducerId,
    message_type: MessageType,
    payload: Option<Vec<u8>>,
) -> OtkEnvelope {
    OtkEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_type,
        source_id: producer_id.clone(),
        stream_id: None,
        sequence_number: None,
        correlation_id: None,
        payload,
        traceparent: current_traceparent(),
    }
}

/// Extract a W3C `traceparent` header value from the current
/// `tracing::Span` via the OTel bridge.
///
/// Returns `None` when no OpenTelemetry subscriber layer is installed
/// (the default empty context has an invalid span context), or when
/// the current span's trace/span ids would render to the all-zero
/// values the W3C spec forbids. In either case the envelope ships
/// with no traceparent and the runtime treats it as an unparented
/// event, matching v0 behaviour.
///
/// Called for every outgoing envelope (Event, Heartbeat, handshake,
/// Disconnect). The OTel bridge's `OpenTelemetrySpanExt::context()`
/// is fast (a hashmap lookup on the span's extensions), so the
/// per-envelope cost is small even with no subscriber configured.
fn current_traceparent() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let span = tracing::Span::current();
    let cx = span.context();
    let span_ref = cx.span();
    let span_ctx = span_ref.span_context();
    if !span_ctx.is_valid() {
        return None;
    }

    let trace_id = u128::from_be_bytes(span_ctx.trace_id().to_bytes());
    let span_id = u64::from_be_bytes(span_ctx.span_id().to_bytes());
    let flags = span_ctx.trace_flags().to_u8();
    otk_protocol::format_traceparent(trace_id, span_id, flags)
}

fn encode_envelope(envelope: &OtkEnvelope) -> Result<Vec<u8>, ProducerError> {
    minicbor::to_vec(envelope).map_err(|e| ProducerError::Encode(format!("envelope encode: {e}")))
}

async fn send_frame<S: AsyncWrite + Unpin + ?Sized>(
    stream: &mut S,
    payload: &[u8],
) -> Result<(), ProducerError> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        ProducerError::Encode(format!("frame too large to send: {} bytes", payload.len()))
    })?;
    if len > MAX_FRAME_BYTES {
        return Err(ProducerError::Encode(format!(
            "frame too large: {len} bytes exceeds the {MAX_FRAME_BYTES} byte protocol maximum"
        )));
    }
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    Ok(())
}

async fn recv_frame<S: AsyncRead + Unpin + ?Sized>(
    stream: &mut S,
    max_bytes: u32,
) -> Result<Vec<u8>, ProducerError> {
    let mut len_buf = [0u8; 4];
    let n = stream.read(&mut len_buf).await?;
    if n == 0 {
        return Err(ProducerError::Closed);
    }
    if n < 4 {
        stream
            .read_exact(&mut len_buf[n..])
            .await
            .map_err(eof_to_closed)?;
    }
    let payload_len = u32::from_be_bytes(len_buf) as usize;
    if payload_len > max_bytes as usize {
        return Err(ProducerError::Decode(format!(
            "frame too large: {payload_len} bytes (max {max_bytes})"
        )));
    }
    let mut payload = vec![0u8; payload_len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(eof_to_closed)?;
    Ok(payload)
}

fn eof_to_closed(e: std::io::Error) -> ProducerError {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        ProducerError::Closed
    } else {
        ProducerError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Without any tracing-opentelemetry layer installed, the current
    /// span's OTel context is empty. Extraction must return `None` so
    /// the envelope ships with no traceparent and the runtime treats
    /// the event as unparented (matching v0 behaviour).
    #[test]
    fn current_traceparent_is_none_without_otel_subscriber() {
        assert_eq!(current_traceparent(), None);
    }

    /// `make_envelope` populates `traceparent` from `current_traceparent`;
    /// when no OTel subscriber is installed, that pipes `None` through.
    /// Verifies the wiring without claiming the positive path here
    /// (that needs an OTel-aware subscriber; see the round-trip test
    /// covered at the runtime layer).
    #[test]
    fn make_envelope_propagates_none_traceparent_when_no_subscriber() {
        let env = make_envelope(
            &ProducerId::from("p-test"),
            MessageType::Heartbeat,
            Some(vec![1, 2, 3]),
        );
        assert_eq!(env.traceparent, None);
        assert_eq!(env.source_id.as_str(), "p-test");
    }
}
