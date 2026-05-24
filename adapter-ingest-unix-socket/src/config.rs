use std::path::PathBuf;
use std::time::Duration;

/// Configuration for a Unix-socket ingest listener.
///
/// Validated at `bind` time: `max_frame_bytes == 0` and
/// `handshake_timeout == Duration::ZERO` are rejected up front rather than
/// surfacing later as confusing handshake failures.
#[derive(Debug, Clone)]
pub struct UnixSocketIngestConfig {
    /// Filesystem path the listener binds to. A stale socket file from a
    /// previous run is removed and re-created on `bind`, but only if it is
    /// genuinely an AF_UNIX socket AND not actively bound by another process
    /// (see [`force_rebind`](Self::force_rebind)). Non-socket entries always
    /// abort the bind to avoid clobbering the wrong file.
    pub socket_path: PathBuf,

    /// Maximum CBOR payload length per frame. **Enforced lower bound:
    /// `>= 1` (`0` is rejected at `bind` time).** Practical minimum is
    /// considerably higher: a Connect handshake serialised with a token
    /// and capabilities is typically several hundred bytes, and frames
    /// declaring more bytes than this cap are rejected mid-stream.
    /// Operators picking a value below ~1024 should expect every real
    /// handshake to fail.
    pub max_frame_bytes: u32,

    /// Maximum time allowed for the OTK handshake to complete after a
    /// connection is accepted. **Enforced lower bound: `> 0`
    /// (`Duration::ZERO` is rejected at `bind` time).** Practical
    /// minimum is round-trip latency plus handshake decode time;
    /// sub-millisecond values will time out instantly on real networks.
    pub handshake_timeout: Duration,

    /// Optional explicit permission bits to apply to the socket file after
    /// bind (e.g. `0o660` for owner+group read/write).
    ///
    /// `None` (default) means the socket's permissions are determined by the
    /// process umask, which is typically too permissive for an ingest
    /// endpoint. Set this field to a specific octal mode (e.g.
    /// `Some(0o660)`) for production deployments. The crate applies it via
    /// `tokio::fs::set_permissions` immediately after the listener binds, so
    /// the window during which the socket is reachable with the
    /// umask-derived mode is as narrow as possible.
    ///
    /// **Race window.** There is still an unavoidable gap between
    /// `UnixListener::bind` creating the filesystem entry (with the
    /// umask-derived mode) and the subsequent chmod completing. A process
    /// fast enough to `connect()` inside that window could be admitted under
    /// the looser permissions. For strict lock-down, combine
    /// `socket_permissions` with a restrictive process umask (so the
    /// pre-chmod mode is already conservative) and/or restrictive parent-
    /// directory permissions (so unauthorised processes can't reach the
    /// socket path at all).
    pub socket_permissions: Option<u32>,

    /// Whether to forcibly remove an existing AF_UNIX socket at
    /// [`socket_path`](Self::socket_path) even if another process appears to
    /// be actively bound to it.
    ///
    /// `false` (default): `bind` probes the existing socket with
    /// `UnixStream::connect`. If the connect succeeds, another process owns
    /// the socket and `bind` refuses with `IngestError::Io(AddrInUse, ...)`
    /// rather than silently kicking the live listener out. If the connect
    /// fails with `ConnectionRefused` (no one listening) the socket is
    /// considered stale and removed.
    ///
    /// `true`: skip the liveness probe and unconditionally remove the
    /// existing socket. Use only for intentional takeover scenarios (e.g.
    /// blue/green deploys where the old process is being killed in lockstep).
    pub force_rebind: bool,
}

impl Default for UnixSocketIngestConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from("/var/run/otk-node.sock"),
            max_frame_bytes: 65_535,
            handshake_timeout: Duration::from_secs(5),
            socket_permissions: None,
            force_rebind: false,
        }
    }
}
