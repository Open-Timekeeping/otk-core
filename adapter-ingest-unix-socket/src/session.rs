use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use frame_codec::{encode_stream, FrameError, StreamFrameDecoder};
use ingest_protocol::{
    perform_server_handshake_with_auth, ConnectAuthoriser, HandshakeError, HandshakeOutcome,
    InboundAction, PostHandshakeProcessor, ProtocolError,
};
use otk_protocol::OtkEnvelope;
use port_in_ingest::{IncomingEvent, IngestError, IngestSession};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::config::UnixSocketIngestConfig;

const READ_CHUNK: usize = 4096;

pub(crate) struct UnixSocketIngestSession {
    stream: UnixStream,
    peer_addr: String,
    producer_id: String,
    decoder: StreamFrameDecoder,
    processor: PostHandshakeProcessor,
    pending: VecDeque<OtkEnvelope>,
}

impl UnixSocketIngestSession {
    pub(crate) async fn handshake(
        mut stream: UnixStream,
        peer_addr: String,
        config: Arc<UnixSocketIngestConfig>,
        authoriser: Arc<dyn ConnectAuthoriser>,
    ) -> Result<Self, IngestError> {
        let max_frame_size = config.max_frame_bytes as usize;
        let mut decoder = StreamFrameDecoder::new(max_frame_size);
        let mut buf = vec![0u8; READ_CHUNK];

        let mut pending: VecDeque<OtkEnvelope> = VecDeque::new();
        loop {
            let n = match stream.read(&mut buf).await {
                Ok(0) => return Err(IngestError::Handshake("EOF before Connect".into())),
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

    async fn fill_pending(&mut self) -> Result<bool, IngestError> {
        let mut buf = [0u8; READ_CHUNK];
        loop {
            let n = match self.stream.read(&mut buf).await {
                Ok(0) => return Ok(false),
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
impl IngestSession for UnixSocketIngestSession {
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

async fn send_envelope(
    stream: &mut UnixStream,
    envelope: &OtkEnvelope,
    max_frame_size: usize,
) -> Result<(), IngestError> {
    let frame = encode_stream(envelope, max_frame_size).map_err(frame_err_to_ingest)?;
    stream.write_all(&frame).await.map_err(IngestError::Io)?;
    Ok(())
}

fn frame_err_to_ingest(e: FrameError) -> IngestError {
    IngestError::Decode(e.to_string())
}

fn handshake_err_to_ingest(e: HandshakeError) -> IngestError {
    IngestError::Handshake(e.to_string())
}

fn protocol_err_to_ingest(e: ProtocolError) -> IngestError {
    IngestError::Decode(e.to_string())
}
