use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use frame_codec::{encode_stream, FrameError, StreamFrameDecoder};
use ingest_protocol::{
    perform_server_handshake_with_auth, ConnectAuthoriser, HandshakeError, HandshakeOutcome,
    InboundAction, PostHandshakeProcessor, ProtocolError,
};
use otk_protocol::OtkEnvelope;
use timing_core::ports::inbound::{IncomingEvent, IngestError, IngestSession};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::config::TcpIngestConfig;

const READ_CHUNK: usize = 4096;

/// Session over an arbitrary byte stream. Concrete `S` is `TcpStream`
/// for plaintext listeners and `tokio_rustls::server::TlsStream<TcpStream>`
/// for the `tls`-feature listeners; the session code below doesn't care
/// which (handshake, framing, dispatch all work against any
/// `AsyncRead + AsyncWrite + Send + Unpin` stream).
pub(crate) struct TcpIngestSession<S> {
    stream: S,
    peer_addr: String,
    producer_id: String,
    decoder: StreamFrameDecoder,
    processor: PostHandshakeProcessor,
    /// Envelopes decoded by the handshake read that arrived in the same TCP
    /// chunk as the `Connect` frame and have not yet been dispatched.
    pending: VecDeque<OtkEnvelope>,
}

impl<S> TcpIngestSession<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin,
{
    /// Run the OTK server-side handshake. On success returns a session ready to
    /// produce events.
    pub(crate) async fn handshake(
        mut stream: S,
        peer_addr: String,
        config: Arc<TcpIngestConfig>,
        authoriser: Arc<dyn ConnectAuthoriser>,
    ) -> Result<Self, IngestError> {
        let max_frame_size = config.max_frame_bytes as usize;
        let mut decoder = StreamFrameDecoder::new(max_frame_size);
        let mut buf = vec![0u8; READ_CHUNK];

        // Read until the first complete envelope arrives.
        let mut pending: VecDeque<OtkEnvelope> = VecDeque::new();
        loop {
            let n = match tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await {
                Ok(0) => {
                    // Distinguish "peer disconnected before sending anything" (no
                    // bytes buffered in the decoder) from "peer sent a partial
                    // Connect and disconnected" (decoder still holds buffered
                    // bytes from the start of a frame).
                    if decoder.has_pending() {
                        return Err(IngestError::Decode(
                            "truncated Connect frame: EOF mid-frame during handshake".into(),
                        ));
                    }
                    return Err(IngestError::Handshake("EOF before Connect".into()));
                }
                Ok(n) => n,
                Err(e) => return Err(IngestError::Io(e)),
            };
            for result in decoder.push(&buf[..n]) {
                let envelope = result.map_err(frame_err_to_ingest)?;
                pending.push_back(envelope);
            }
            if !pending.is_empty() {
                break;
            }
        }

        let connect_env = pending.pop_front().expect("loop guarantees non-empty");
        match perform_server_handshake_with_auth(connect_env, authoriser.as_ref())
            .map_err(handshake_err_to_ingest)?
        {
            HandshakeOutcome::Accepted { reply, processor } => {
                send_envelope(&mut stream, &reply, max_frame_size).await?;
                let producer_id = processor.producer_id().to_string();
                Ok(Self {
                    stream,
                    peer_addr,
                    producer_id,
                    decoder,
                    processor,
                    pending,
                })
            }
            HandshakeOutcome::Rejected { reply, reason } => {
                let _ = send_envelope(&mut stream, &reply, max_frame_size).await;
                Err(IngestError::Handshake(format!("rejected: {reason:?}")))
            }
        }
    }

    /// Read more bytes and append any decoded envelopes to `self.pending`.
    /// Returns `Ok(false)` if the peer closed cleanly with the queue still empty.
    async fn fill_pending(&mut self) -> Result<bool, IngestError> {
        let mut buf = [0u8; READ_CHUNK];
        loop {
            let n = match tokio::io::AsyncReadExt::read(&mut self.stream, &mut buf).await {
                Ok(0) => {
                    // EOF. If the decoder is sitting on partial frame bytes (a
                    // truncated length prefix / payload, or unfinished oversize-
                    // skip), report a truncated-frame error rather than returning
                    // the clean-close "no more envelopes" signal. Producers that
                    // exit cleanly at a frame boundary land here with
                    // has_pending() == false.
                    if self.decoder.has_pending() {
                        return Err(IngestError::Decode(
                            "truncated frame: EOF before frame completed".into(),
                        ));
                    }
                    return Ok(false);
                }
                Ok(n) => n,
                Err(e) => return Err(IngestError::Io(e)),
            };
            for result in self.decoder.push(&buf[..n]) {
                let envelope = result.map_err(frame_err_to_ingest)?;
                self.pending.push_back(envelope);
            }
            if !self.pending.is_empty() {
                return Ok(true);
            }
        }
    }
}

#[async_trait]
impl<S> IngestSession for TcpIngestSession<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin,
{
    async fn next_event(&mut self) -> Result<Option<IncomingEvent>, IngestError> {
        loop {
            if self.pending.is_empty() && !self.fill_pending().await? {
                return Ok(None);
            }
            let envelope = self.pending.pop_front().expect("just filled or non-empty");
            match self
                .processor
                .process(envelope)
                .map_err(protocol_err_to_ingest)?
            {
                InboundAction::Event { event, traceparent } => {
                    return Ok(Some(IncomingEvent { event, traceparent }))
                }
                InboundAction::Heartbeat => continue,
                InboundAction::Disconnect => return Ok(None),
            }
        }
    }

    fn producer_id(&self) -> &str {
        &self.producer_id
    }

    fn peer_addr(&self) -> &str {
        &self.peer_addr
    }
}

async fn send_envelope<S>(
    stream: &mut S,
    envelope: &OtkEnvelope,
    max_frame_size: usize,
) -> Result<(), IngestError>
where
    S: AsyncWrite + Unpin,
{
    let frame = encode_stream(envelope, max_frame_size).map_err(frame_err_to_ingest)?;
    stream.write_all(&frame).await.map_err(IngestError::Io)?;
    Ok(())
}

fn frame_err_to_ingest(e: FrameError) -> IngestError {
    match e {
        FrameError::OversizeFrame { .. } => IngestError::Decode(e.to_string()),
        FrameError::DecodeFailed(_)
        | FrameError::CorruptFrame
        | FrameError::LostSync
        | FrameError::EncodeFailed => IngestError::Decode(e.to_string()),
    }
}

fn handshake_err_to_ingest(e: HandshakeError) -> IngestError {
    IngestError::Handshake(e.to_string())
}

fn protocol_err_to_ingest(e: ProtocolError) -> IngestError {
    IngestError::Decode(e.to_string())
}

#[cfg(test)]
mod tests {
    use event_model::{
        Detection, DetectionId, DetectorId, OtkEvent, SensorData, SourceAttestation, TimebaseId,
        TimestampingMethod, TimingPointId,
    };
    use frame_codec::encode_stream;
    use ingest_protocol::ConnectAuthoriser;
    use otk_protocol::{
        ids::ProducerId, Connect, ConnectRejectReason, MessageType, OtkEnvelope, PROTOCOL_VERSION,
    };
    use std::sync::Arc;
    use timing_core::ports::inbound::{EventIngestPort, IngestError};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use crate::config::TcpIngestConfig;
    use crate::TcpIngestPort;

    fn test_config() -> TcpIngestConfig {
        TcpIngestConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        }
    }

    async fn read_one_envelope(stream: &mut TcpStream) -> OtkEnvelope {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await.unwrap();
        minicbor::decode(&payload).unwrap()
    }

    async fn client_handshake(
        addr: std::net::SocketAddr,
        version_min: u8,
    ) -> (TcpStream, OtkEnvelope) {
        client_handshake_with_token(addr, version_min, None).await
    }

    async fn client_handshake_with_token(
        addr: std::net::SocketAddr,
        version_min: u8,
        auth_token: Option<&str>,
    ) -> (TcpStream, OtkEnvelope) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let connect = Connect {
            protocol_version_min: version_min,
            protocol_version_max: PROTOCOL_VERSION,
            streams: vec![],
            auth_token: auth_token.map(String::from),
        };
        let env = OtkEnvelope {
            protocol_version: PROTOCOL_VERSION,
            message_type: MessageType::Connect,
            source_id: ProducerId::from("test-producer"),
            stream_id: None,
            sequence_number: None,
            correlation_id: None,
            payload: Some(minicbor::to_vec(&connect).unwrap()),
            traceparent: None,
        };
        let frame = encode_stream(&env, 65_535).unwrap();
        stream.write_all(&frame).await.unwrap();
        let reply = read_one_envelope(&mut stream).await;
        (stream, reply)
    }

    /// Test authoriser that accepts exactly one token.
    struct OneTokenAuth(&'static str);

    impl ConnectAuthoriser for OneTokenAuth {
        fn authorise(
            &self,
            _producer_id: &ProducerId,
            token: Option<&str>,
        ) -> Result<(), ConnectRejectReason> {
            match token {
                Some(t) if t == self.0 => Ok(()),
                _ => Err(ConnectRejectReason::Unauthorized),
            }
        }
    }

    fn wrap_in_envelope(msg_type: MessageType, payload: Option<Vec<u8>>) -> OtkEnvelope {
        OtkEnvelope {
            protocol_version: PROTOCOL_VERSION,
            message_type: msg_type,
            source_id: ProducerId::from("test-producer"),
            stream_id: None,
            sequence_number: None,
            correlation_id: None,
            payload,
            traceparent: None,
        }
    }

    fn test_event() -> OtkEvent {
        OtkEvent::Detection(Detection {
            detection_id: DetectionId::new("det-1"),
            detector_id: DetectorId::new("sensor-1"),
            timing_point_id: TimingPointId::new("tp-a"),
            subject_id: None,
            detected_at_ns: 1_000_000_000,
            detected_at_uncertainty_ns: None,
            received_at_ns: None,
            timestamping_method: TimestampingMethod::HardwareEventCapture,
            timebase_id: TimebaseId::new("tb-1"),
            source_attestation: SourceAttestation::RuntimeDiscovered,
            sequence_number: 0,
            sensor: SensorData::BeamBreak,
        })
    }

    #[tokio::test]
    async fn handshake_success_returns_ack_and_session() {
        let port = TcpIngestPort::bind(test_config()).await.unwrap();
        let addr = port.local_addr().unwrap();

        let (session_result, (_, reply)) =
            tokio::join!(port.accept(), client_handshake(addr, PROTOCOL_VERSION));

        let session = session_result.unwrap();
        assert_eq!(reply.message_type, MessageType::ConnectAck);
        assert_eq!(session.producer_id(), "test-producer");
    }

    #[tokio::test]
    async fn handshake_version_min_too_high_sends_reject() {
        let port = TcpIngestPort::bind(test_config()).await.unwrap();
        let addr = port.local_addr().unwrap();

        let (accept_result, (_, reply)) =
            tokio::join!(port.accept(), client_handshake(addr, u8::MAX));

        assert!(accept_result.is_err());
        assert_eq!(reply.message_type, MessageType::ConnectReject);
    }

    #[tokio::test]
    async fn handshake_version_max_too_low_sends_reject() {
        let port = TcpIngestPort::bind(test_config()).await.unwrap();
        let addr = port.local_addr().unwrap();

        let (accept_result, (_, reply)) = tokio::join!(port.accept(), async {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let connect = Connect {
                protocol_version_min: 0,
                protocol_version_max: 0,
                streams: vec![],
                auth_token: None,
            };
            let env = OtkEnvelope {
                protocol_version: 0,
                message_type: MessageType::Connect,
                source_id: ProducerId::from("old-producer"),
                stream_id: None,
                sequence_number: None,
                correlation_id: None,
                payload: Some(minicbor::to_vec(&connect).unwrap()),
                traceparent: None,
            };
            let frame = encode_stream(&env, 65_535).unwrap();
            stream.write_all(&frame).await.unwrap();
            let reply = read_one_envelope(&mut stream).await;
            (stream, reply)
        });

        // The two branches handle the only two possible relations between
        // the producer's `protocol_version_max = 0` and the server's
        // PROTOCOL_VERSION: today PROTOCOL_VERSION > 0 so the reject arm
        // is the live one; if PROTOCOL_VERSION ever drops back to 0 (or
        // we introduce a 0-versioned legacy server build) the accept arm
        // is what we want. The clippy lint correctly notes the
        // currently-unreachable arm is "always false"; we're preserving
        // it intentionally as a forward-compat assertion.
        #[allow(clippy::absurd_extreme_comparisons)]
        if PROTOCOL_VERSION > 0 {
            assert!(accept_result.is_err());
            assert_eq!(reply.message_type, MessageType::ConnectReject);
        } else {
            // Producer max == server version (both 0); handshake must succeed.
            assert!(accept_result.is_ok());
            assert_eq!(reply.message_type, MessageType::ConnectAck);
        }
    }

    #[tokio::test]
    async fn heartbeat_consumed_transparently() {
        let port = TcpIngestPort::bind(test_config()).await.unwrap();
        let addr = port.local_addr().unwrap();

        let event = test_event();
        let event_env =
            wrap_in_envelope(MessageType::Event, Some(minicbor::to_vec(&event).unwrap()));
        // Per the OtkEnvelope contract, Heartbeat carries a CBOR-encoded payload.
        let hb = otk_protocol::Heartbeat { sent_at_ns: 0 };
        let heartbeat_env =
            wrap_in_envelope(MessageType::Heartbeat, Some(minicbor::to_vec(hb).unwrap()));

        let (session_result, _) = tokio::join!(port.accept(), async {
            let (mut stream, _) = client_handshake(addr, PROTOCOL_VERSION).await;
            stream
                .write_all(&encode_stream(&heartbeat_env, 65_535).unwrap())
                .await
                .unwrap();
            stream
                .write_all(&encode_stream(&event_env, 65_535).unwrap())
                .await
                .unwrap();
        });

        let mut session = session_result.unwrap();
        let received = session.next_event().await.unwrap().unwrap();
        assert!(matches!(received.event, OtkEvent::Detection(_)));
        assert_eq!(
            received.traceparent, None,
            "no traceparent set on the producer side"
        );
    }

    #[tokio::test]
    async fn disconnect_message_closes_session() {
        let port = TcpIngestPort::bind(test_config()).await.unwrap();
        let addr = port.local_addr().unwrap();
        let disconnect_env = wrap_in_envelope(MessageType::Disconnect, None);

        let (session_result, _) = tokio::join!(port.accept(), async {
            let (mut stream, _) = client_handshake(addr, PROTOCOL_VERSION).await;
            stream
                .write_all(&encode_stream(&disconnect_env, 65_535).unwrap())
                .await
                .unwrap();
        });

        let mut session = session_result.unwrap();
        assert!(session.next_event().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tcp_eof_closes_session() {
        let port = TcpIngestPort::bind(test_config()).await.unwrap();
        let addr = port.local_addr().unwrap();

        let (session_result, _) = tokio::join!(port.accept(), async {
            let (stream, _) = client_handshake(addr, PROTOCOL_VERSION).await;
            drop(stream);
        });

        let mut session = session_result.unwrap();
        assert!(session.next_event().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mid_frame_eof_is_an_error_not_clean_close() {
        // Producer sends a length prefix declaring more bytes than it actually
        // sends, then disconnects. The decoder buffers the partial frame; the
        // session must report the truncation, not return Ok(None).
        use tokio::io::AsyncWriteExt;

        let port = TcpIngestPort::bind(test_config()).await.unwrap();
        let addr = port.local_addr().unwrap();

        let (session_result, _) = tokio::join!(port.accept(), async {
            let (mut stream, _) = client_handshake(addr, PROTOCOL_VERSION).await;
            // Length prefix says 10 bytes; we send 0 of them, then close.
            stream.write_all(&10u32.to_be_bytes()).await.unwrap();
            drop(stream);
        });

        let mut session = session_result.unwrap();
        let result = session.next_event().await;
        assert!(
            matches!(result, Err(IngestError::Decode(_))),
            "mid-frame EOF must be a Decode error, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn truncated_connect_during_handshake_is_a_decode_error() {
        // Producer sends a length prefix promising N bytes of Connect, then
        // disconnects before sending them. The handshake loop must report
        // truncation, not "EOF before Connect".
        use tokio::io::AsyncWriteExt;

        let port = TcpIngestPort::bind(test_config()).await.unwrap();
        let addr = port.local_addr().unwrap();

        let (accept_result, _) = tokio::join!(port.accept(), async {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(&100u32.to_be_bytes()).await.unwrap();
            drop(stream);
        });

        match accept_result {
            Err(IngestError::Decode(msg)) => {
                assert!(
                    msg.contains("truncated") || msg.contains("EOF mid"),
                    "expected truncation message, got: {msg}"
                );
            }
            Err(other) => panic!("expected Decode error for truncated Connect, got: {other}"),
            Ok(_) => panic!("expected an error, got a session"),
        }
    }

    #[tokio::test]
    async fn bind_with_auth_rejects_missing_token() {
        // Server requires a token; producer sends Connect without one.
        let port = TcpIngestPort::bind_with_auth(test_config(), Arc::new(OneTokenAuth("secret")))
            .await
            .unwrap();
        let addr = port.local_addr().unwrap();

        let (accept_result, (_, reply)) = tokio::join!(
            port.accept(),
            client_handshake_with_token(addr, PROTOCOL_VERSION, None)
        );

        assert!(accept_result.is_err(), "missing token must reject accept()");
        assert_eq!(reply.message_type, MessageType::ConnectReject);
        let reject: otk_protocol::ConnectReject =
            minicbor::decode(reply.payload.as_deref().expect("reject payload"))
                .expect("decode reject");
        assert!(matches!(reject.reason, ConnectRejectReason::Unauthorized));
    }

    #[tokio::test]
    async fn bind_with_auth_rejects_wrong_token() {
        let port = TcpIngestPort::bind_with_auth(test_config(), Arc::new(OneTokenAuth("secret")))
            .await
            .unwrap();
        let addr = port.local_addr().unwrap();

        let (accept_result, (_, reply)) = tokio::join!(
            port.accept(),
            client_handshake_with_token(addr, PROTOCOL_VERSION, Some("nope"))
        );

        assert!(accept_result.is_err(), "wrong token must reject accept()");
        assert_eq!(reply.message_type, MessageType::ConnectReject);
        let reject: otk_protocol::ConnectReject =
            minicbor::decode(reply.payload.as_deref().expect("reject payload"))
                .expect("decode reject");
        assert!(matches!(reject.reason, ConnectRejectReason::Unauthorized));
    }

    #[tokio::test]
    async fn bind_with_auth_accepts_valid_token() {
        let port = TcpIngestPort::bind_with_auth(test_config(), Arc::new(OneTokenAuth("secret")))
            .await
            .unwrap();
        let addr = port.local_addr().unwrap();

        let (session_result, (_, reply)) = tokio::join!(
            port.accept(),
            client_handshake_with_token(addr, PROTOCOL_VERSION, Some("secret"))
        );

        let session = session_result.expect("valid token must produce a session");
        assert_eq!(reply.message_type, MessageType::ConnectAck);
        assert_eq!(session.producer_id(), "test-producer");
    }

    #[tokio::test]
    async fn frame_exceeding_max_bytes_is_rejected() {
        // Pick a server max small enough to exceed easily but large enough for the
        // Connect envelope to fit (the Connect payload is well under 256 bytes).
        let port = TcpIngestPort::bind(TcpIngestConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_frame_bytes: 256,
            ..Default::default()
        })
        .await
        .unwrap();
        let addr = port.local_addr().unwrap();

        let (session_result, _) = tokio::join!(port.accept(), async {
            let (mut stream, _) = client_handshake(addr, PROTOCOL_VERSION).await;
            // Declare a payload of 1_000 bytes (above the 256-byte server limit).
            // The decoder reports OversizeFrame as soon as it parses the header;
            // we still send the bytes so the connection doesn't get reset mid-write.
            stream.write_all(&1_000u32.to_be_bytes()).await.unwrap();
            let filler = vec![0u8; 1_000];
            stream.write_all(&filler).await.unwrap();
        });

        let mut session = session_result.unwrap();
        let result = session.next_event().await;
        assert!(result.is_err(), "oversized frame must return an error");
    }
}
