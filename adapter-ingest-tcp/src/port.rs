use std::sync::Arc;

use async_trait::async_trait;
use ingest_protocol::{AllowAll, ConnectAuthoriser};
use port_in_ingest::{EventIngestPort, IngestError, IngestSession};
use tokio::net::TcpListener;
use tokio::time::timeout;

use crate::config::TcpIngestConfig;
use crate::session::TcpIngestSession;

pub struct TcpIngestPort {
    listener: TcpListener,
    config: Arc<TcpIngestConfig>,
    authoriser: Arc<dyn ConnectAuthoriser>,
}

impl TcpIngestPort {
    /// Bind with the default [`AllowAll`] authoriser (development).
    pub async fn bind(config: TcpIngestConfig) -> Result<Self, IngestError> {
        Self::bind_with_auth(config, Arc::new(AllowAll)).await
    }

    /// Bind with an explicit authoriser. The runtime supplies a token-allow-list
    /// authoriser when [`crate::config::TcpIngestConfig`]'s deployment requires it.
    pub async fn bind_with_auth(
        config: TcpIngestConfig,
        authoriser: Arc<dyn ConnectAuthoriser>,
    ) -> Result<Self, IngestError> {
        let listener = TcpListener::bind(config.bind_addr).await?;
        Ok(Self {
            listener,
            config: Arc::new(config),
            authoriser,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
}

#[async_trait]
impl EventIngestPort for TcpIngestPort {
    async fn accept(&self) -> Result<Box<dyn IngestSession>, IngestError> {
        let (stream, peer) = self.listener.accept().await?;
        let session = timeout(
            self.config.handshake_timeout,
            TcpIngestSession::handshake(
                stream,
                peer.to_string(),
                self.config.clone(),
                Arc::clone(&self.authoriser),
            ),
        )
        .await
        .map_err(|_| IngestError::Handshake("handshake timed out".into()))??;
        Ok(Box::new(session))
    }
}
