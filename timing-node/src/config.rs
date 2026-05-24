use std::path::{Path, PathBuf};

/// One ingest listener entry in [`NodeConfig::listeners`].
///
/// One variant per supported transport binding. v0 ships TCP and AF_UNIX
/// (Unix-domain socket); USB-CDC and others will land as additional variants
/// without breaking existing configs.
///
/// The `unix-socket` variant only binds successfully on Unix targets; on
/// Windows it parses but `Node::new` returns an error if any unix-socket
/// listener is configured.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "transport", rename_all = "kebab-case")]
pub enum ListenerConfig {
    Tcp {
        /// Stable id, used in metrics and logs.
        #[serde(default = "default_tcp_id")]
        id: String,
        bind_addr: std::net::SocketAddr,
        #[serde(default = "default_max_frame_bytes")]
        max_frame_bytes: u32,
    },
    UnixSocket {
        #[serde(default = "default_unix_id")]
        id: String,
        socket_path: PathBuf,
        #[serde(default = "default_max_frame_bytes")]
        max_frame_bytes: u32,

        /// Octal permission bits to apply to the socket file after bind
        /// (e.g. `0o660` for owner+group read/write). `None` (the
        /// default) leaves the mode to the process umask, which is
        /// typically too permissive for an ingest endpoint. Set this
        /// for production deployments. See
        /// `adapter_ingest_unix_socket::UnixSocketIngestConfig::socket_permissions`
        /// for the race-window discussion. (Not linked: that crate is
        /// cfg(unix)-gated and absent from the dep graph on Windows.)
        ///
        /// TOML form: `socket_permissions = 0o660` (TOML accepts octal
        /// integer literals natively).
        #[serde(default)]
        socket_permissions: Option<u32>,

        /// If `true`, forcibly remove an existing AF_UNIX socket at
        /// [`socket_path`](Self::UnixSocket::socket_path) even if
        /// another process appears to own it. `false` (the default) is
        /// the safe behaviour: probe the existing socket with
        /// `UnixStream::connect`, refuse bind if a live listener
        /// responds (returns `AddrInUse`), remove it only if it's a
        /// stale entry from a crashed previous run. Set to `true` only
        /// for intentional takeover scenarios (e.g. blue/green deploys
        /// where the old process is being killed in lockstep).
        #[serde(default)]
        force_rebind: bool,
    },
}

fn default_tcp_id() -> String {
    "tcp-main".into()
}

fn default_unix_id() -> String {
    "unix-main".into()
}

fn default_max_frame_bytes() -> u32 {
    65_535
}

/// Authentication for ingest producers and API consumers.
///
/// When `producer_tokens` is empty, all producers are accepted unauthenticated
/// (development mode; a startup-time warning is logged). When non-empty, a
/// `Connect` whose `auth_token` is missing or not in the set is rejected with
/// `ConnectRejectReason::Unauthorized` from the `protocol` crate.
///
/// The same shape applies to `api_tokens` for the REST/SSE API.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct AuthConfig {
    pub producer_tokens: Vec<String>,
    pub api_tokens: Vec<String>,
}

/// CORS policy for the REST/SSE API.
///
/// When `allowed_origins` is empty, CORS is closed (no `Access-Control-Allow-Origin`
/// header is emitted) which means browsers will block cross-origin requests. The
/// special value `"*"` opens to all origins.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct ApiConfig {
    pub allowed_origins: Vec<String>,
}

/// Runtime configuration for `otk-node`.
///
/// Load from a TOML file with [`load_from_file`], or use [`Default`] for built-in defaults
/// suitable for local development.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct NodeConfig {
    /// Stable identifier for this node instance.
    pub node_id: String,
    /// Ingest listeners. Every entry feeds the same canonical ingest pipeline.
    pub listeners: Vec<ListenerConfig>,
    /// Address the REST/SSE API server binds to.
    pub api_addr: std::net::SocketAddr,
    /// Directory where the segment log stores its files. Created if absent.
    pub storage_dir: PathBuf,
    /// Authentication policy for producers and API consumers.
    pub auth: AuthConfig,
    /// API server configuration (CORS allow-list).
    pub api: ApiConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: "otk-node".into(),
            listeners: vec![ListenerConfig::Tcp {
                id: default_tcp_id(),
                bind_addr: "0.0.0.0:8463".parse().unwrap(),
                max_frame_bytes: default_max_frame_bytes(),
            }],
            api_addr: "0.0.0.0:8080".parse().unwrap(),
            storage_dir: PathBuf::from("data"),
            auth: AuthConfig::default(),
            api: ApiConfig::default(),
        }
    }
}

/// Load a [`NodeConfig`] from a TOML file.
pub fn load_from_file(path: &Path) -> Result<NodeConfig, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&text)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = NodeConfig::default();
        assert_eq!(cfg.node_id, "otk-node");
        assert_eq!(cfg.listeners.len(), 1);
        let ListenerConfig::Tcp {
            bind_addr,
            max_frame_bytes,
            ..
        } = &cfg.listeners[0]
        else {
            panic!("default listener should be TCP");
        };
        assert_eq!(bind_addr.port(), 8463);
        assert_eq!(*max_frame_bytes, 65_535);
    }

    #[test]
    fn single_tcp_listener_round_trip() {
        let toml_str = r#"
node_id   = "n"
api_addr  = "0.0.0.0:9090"
storage_dir = "data"

[[listeners]]
transport = "tcp"
id        = "tcp-main"
bind_addr = "0.0.0.0:7420"
max_frame_bytes = 1024
"#;
        let loaded: NodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(loaded.node_id, "n");
        assert_eq!(loaded.listeners.len(), 1);
        let ListenerConfig::Tcp {
            id,
            bind_addr,
            max_frame_bytes,
        } = &loaded.listeners[0]
        else {
            panic!("expected TCP listener");
        };
        assert_eq!(id, "tcp-main");
        assert_eq!(bind_addr.port(), 7420);
        assert_eq!(*max_frame_bytes, 1024);
    }

    #[test]
    fn multiple_tcp_listeners_parse() {
        let toml_str = r#"
node_id   = "n"
api_addr  = "0.0.0.0:9090"
storage_dir = "data"

[[listeners]]
transport = "tcp"
id        = "tcp-a"
bind_addr = "0.0.0.0:7420"

[[listeners]]
transport = "tcp"
id        = "tcp-b"
bind_addr = "0.0.0.0:7421"
"#;
        let loaded: NodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(loaded.listeners.len(), 2);
    }

    #[test]
    fn mixed_tcp_and_unix_listeners_parse() {
        let toml_str = r#"
node_id   = "n"
api_addr  = "0.0.0.0:9090"
storage_dir = "data"

[[listeners]]
transport = "tcp"
id        = "tcp-main"
bind_addr = "0.0.0.0:7420"

[[listeners]]
transport   = "unix-socket"
id          = "local-adapters"
socket_path = "/var/run/otk-node.sock"
"#;
        let loaded: NodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(loaded.listeners.len(), 2);
        match &loaded.listeners[1] {
            ListenerConfig::UnixSocket {
                id,
                socket_path,
                socket_permissions,
                force_rebind,
                ..
            } => {
                assert_eq!(id, "local-adapters");
                assert_eq!(socket_path, &PathBuf::from("/var/run/otk-node.sock"));
                // Defaults when fields are omitted.
                assert_eq!(*socket_permissions, None);
                assert!(!*force_rebind);
            }
            _ => panic!("expected UnixSocket variant"),
        }
    }

    #[test]
    fn unix_listener_with_permissions_and_force_rebind_parses() {
        // TOML accepts `0o660` as an octal integer literal natively.
        let toml_str = r#"
node_id   = "n"
api_addr  = "0.0.0.0:9090"
storage_dir = "data"

[[listeners]]
transport          = "unix-socket"
id                 = "local-adapters"
socket_path        = "/var/run/otk-node.sock"
socket_permissions = 0o660
force_rebind       = true
"#;
        let loaded: NodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(loaded.listeners.len(), 1);
        match &loaded.listeners[0] {
            ListenerConfig::UnixSocket {
                socket_permissions,
                force_rebind,
                ..
            } => {
                assert_eq!(*socket_permissions, Some(0o660));
                assert!(*force_rebind);
            }
            _ => panic!("expected UnixSocket variant"),
        }
    }
}
