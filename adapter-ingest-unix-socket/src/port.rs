use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ingest_protocol::{AllowAll, ConnectAuthoriser};
use port_in_ingest::{EventIngestPort, IngestError, IngestSession};
use tokio::net::{UnixListener, UnixStream};
use tokio::time::timeout;

use crate::config::UnixSocketIngestConfig;
use crate::session::UnixSocketIngestSession;

pub struct UnixSocketIngestPort {
    listener: UnixListener,
    config: Arc<UnixSocketIngestConfig>,
    authoriser: Arc<dyn ConnectAuthoriser>,
}

impl UnixSocketIngestPort {
    pub async fn bind(config: UnixSocketIngestConfig) -> Result<Self, IngestError> {
        Self::bind_with_auth(config, Arc::new(AllowAll)).await
    }

    pub async fn bind_with_auth(
        config: UnixSocketIngestConfig,
        authoriser: Arc<dyn ConnectAuthoriser>,
    ) -> Result<Self, IngestError> {
        // Reject obviously-broken config up front so failure is surfaced at
        // bind time, not later as a confusing handshake error.
        if config.max_frame_bytes == 0 {
            return Err(IngestError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_frame_bytes must be > 0",
            )));
        }
        if config.handshake_timeout == Duration::ZERO {
            return Err(IngestError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "handshake_timeout must be > 0",
            )));
        }

        // Clean up a stale socket file from a previous run, but only if it is
        // genuinely an AF_UNIX socket AND not actively bound by another
        // process. Refuse to delete regular files, directories, symlinks, or
        // device nodes at the configured path: a misconfigured `socket_path`
        // pointing at (say) `/etc/passwd` must never silently destroy that
        // file.
        //
        // The cleanup path can also race with another process removing the
        // socket file between any two of our filesystem ops. We treat
        // NotFound at any step as "clean slate, proceed to bind" rather
        // than an error.
        //
        // To avoid TOCTOU where another process swaps in a non-socket
        // between our type-check and our remove, we capture the (device,
        // inode) identity at the initial check and re-verify it
        // immediately before the unlink. If anything about the file's
        // identity has changed, we refuse to remove rather than risk
        // unlinking the wrong target.
        let mut needs_remove = false;
        let mut captured_identity: Option<(u64, u64)> = None;
        match tokio::fs::symlink_metadata(&config.socket_path).await {
            Ok(meta) => {
                if !meta.file_type().is_socket() {
                    let msg = format!(
                        "refusing to bind: {} exists and is not a Unix domain socket (file type: {:?}); aborting rather than risk deleting the wrong file",
                        config.socket_path.display(),
                        meta.file_type()
                    );
                    return Err(IngestError::Io(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        msg,
                    )));
                }

                // Liveness probe: try to connect. If something accepts the
                // connection, another process owns the socket and we must
                // not steal it (silent takeover would leave the original
                // listener running but unreachable). `force_rebind = true`
                // skips this check for intentional takeover (e.g. blue/green
                // deploys where the predecessor is being killed in lockstep).
                if config.force_rebind {
                    needs_remove = true;
                    captured_identity = Some((meta.dev(), meta.ino()));
                } else {
                    match UnixStream::connect(&config.socket_path).await {
                        Ok(_probe) => {
                            // Drop the probe stream immediately; we only
                            // needed to know whether someone was listening.
                            let msg = format!(
                                "refusing to bind: {} is already owned by an active listener; set force_rebind = true to take over intentionally",
                                config.socket_path.display()
                            );
                            return Err(IngestError::Io(io::Error::new(
                                io::ErrorKind::AddrInUse,
                                msg,
                            )));
                        }
                        Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                            // Stale socket file: previous owner is gone.
                            // Safe to remove and re-bind.
                            needs_remove = true;
                            captured_identity = Some((meta.dev(), meta.ino()));
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {
                            // Raced with another process removing the file
                            // between symlink_metadata and connect.
                            // needs_remove stays false: nothing to remove.
                        }
                        Err(e) => {
                            // Unexpected error (permission denied, etc.).
                            // Be conservative and refuse rather than risk
                            // misinterpreting the state. Preserve the
                            // original ErrorKind so callers can still
                            // distinguish e.g. PermissionDenied from
                            // ConnectionReset; the new message adds path
                            // context for ops triage but keeps the kind
                            // intact for programmatic matching.
                            let kind = e.kind();
                            return Err(IngestError::Io(io::Error::new(
                                kind,
                                format!(
                                    "could not probe existing socket at {}: {}; aborting bind",
                                    config.socket_path.display(),
                                    e
                                ),
                            )));
                        }
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Nothing at the path; nothing to clean up.
            }
            Err(e) => return Err(IngestError::Io(e)),
        }

        if needs_remove {
            // TOCTOU guard: re-stat immediately before unlinking and refuse
            // to remove if the file's identity has changed (different inode
            // or device, or no longer a socket). Without this, an attacker
            // (or a misbehaving cron job) could swap in a regular file
            // between our initial check and the unlink, and we'd destroy the
            // wrong target.
            //
            // If the file disappeared entirely (NotFound), `still_to_remove`
            // stays false and we skip the unlink — the bind below will
            // create the socket fresh either way.
            let still_to_remove = match captured_identity {
                Some((expected_dev, expected_ino)) => {
                    match tokio::fs::symlink_metadata(&config.socket_path).await {
                        Ok(now_meta) => {
                            if !now_meta.file_type().is_socket()
                                || now_meta.dev() != expected_dev
                                || now_meta.ino() != expected_ino
                            {
                                return Err(IngestError::Io(io::Error::new(
                                    io::ErrorKind::AlreadyExists,
                                    format!(
                                        "refusing to bind: {} was replaced between the initial check and the unlink (file type or inode changed); aborting rather than risk deleting the wrong file",
                                        config.socket_path.display()
                                    ),
                                )));
                            }
                            true
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => false,
                        Err(e) => return Err(IngestError::Io(e)),
                    }
                }
                None => true,
            };

            if still_to_remove {
                // Use the async remove so we don't block the runtime.
                // Tolerate a NotFound (another process unlinked the file
                // between our TOCTOU re-check and this call) — the bind path
                // below will create it fresh either way.
                match tokio::fs::remove_file(&config.socket_path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(IngestError::Io(e)),
                }
            }
        }

        let listener = UnixListener::bind(&config.socket_path).map_err(IngestError::Io)?;

        // Apply explicit permissions if configured. Without this, the socket
        // file's mode is determined by the process umask, which is typically
        // too permissive for an ingest endpoint. Production deployments
        // should set `socket_permissions` (e.g. `Some(0o660)` for owner+group
        // read/write only).
        if let Some(mode) = config.socket_permissions {
            let perms = std::fs::Permissions::from_mode(mode);
            tokio::fs::set_permissions(&config.socket_path, perms)
                .await
                .map_err(IngestError::Io)?;
        }

        Ok(Self {
            listener,
            config: Arc::new(config),
            authoriser,
        })
    }

    pub fn socket_path(&self) -> &std::path::Path {
        &self.config.socket_path
    }
}

#[async_trait]
impl EventIngestPort for UnixSocketIngestPort {
    async fn accept(&self) -> Result<Box<dyn IngestSession>, IngestError> {
        let (stream, peer) = self.listener.accept().await.map_err(IngestError::Io)?;
        // AF_UNIX clients are usually anonymous (no client-side bind), so
        // peer.pathname() is None for almost every real connection. When a
        // client did bind a pathname, surface it so per-connection logs can
        // distinguish peers; otherwise use a stable "unix:anonymous" marker.
        // The listener's own path is intentionally not in peer_addr — that's
        // the listener's identity (already tracked separately via the
        // listener_id label), not the peer's.
        let peer_addr = match peer.as_pathname() {
            Some(path) => format!("unix:{}", path.display()),
            None => "unix:anonymous".to_string(),
        };
        let session = timeout(
            self.config.handshake_timeout,
            UnixSocketIngestSession::handshake(
                stream,
                peer_addr,
                self.config.clone(),
                Arc::clone(&self.authoriser),
            ),
        )
        .await
        .map_err(|_| IngestError::Handshake("handshake timed out".into()))??;
        Ok(Box::new(session))
    }
}
