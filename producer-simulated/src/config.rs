use std::{net::SocketAddr, path::Path, path::PathBuf};

/// Configuration for the simulator adapter.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct SimulatorConfig {
    /// Stable identifier for this detector instance.
    pub detector_id: String,
    /// Timing points to cycle detections across. Validated non-empty at adapter start.
    pub timing_point_ids: Vec<String>,
    /// Subject IDs to cycle through. Empty means no subject_id on detections.
    pub subject_ids: Vec<String>,
    /// Timebase identifier to declare in metadata and stamp on detections.
    pub timebase_id: String,
    /// Milliseconds between generated detections.
    pub detection_interval_ms: u64,
    /// Total detections to emit. `None` runs indefinitely.
    pub count: Option<u64>,
    /// Address of the timing-node ingest listener.
    pub node_addr: SocketAddr,
    /// Producer identity sent in the OTK handshake.
    pub producer_id: String,
    /// Optional shared-secret token sent in `Connect.auth_token`. When
    /// the node is configured with a non-empty `auth.producer_tokens`,
    /// the simulator's token must match one of them or the handshake
    /// is rejected. Omitted = no token sent (works against a node
    /// with an empty allow-list).
    pub auth_token: Option<String>,
    /// Optional TLS configuration. `Some(...)` upgrades the producer
    /// connection to TLS using `otk-sdk`'s `Transport::Tls`; the
    /// node must have a `[listeners.tls]` block on the matching
    /// listener. `None` (the default) uses plain TCP.
    pub tls: Option<TlsClientConfig>,
}

/// TLS client config for [`SimulatorConfig::tls`]. Mirrors
/// `otk_sdk::producer::TlsClientConfig` with serde-friendly types,
/// loaded once at simulator startup.
///
/// TOML form:
///
/// ```toml
/// [tls]
/// trust_roots = "/etc/otk/server-ca.pem"
/// server_name = "otk-node.lan"
/// # For mTLS, set both:
/// client_cert = "/etc/otk/sim-cert.pem"
/// client_key  = "/etc/otk/sim-key.pem"
/// ```
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TlsClientConfig {
    pub trust_roots: PathBuf,
    pub server_name: String,
    #[serde(default)]
    pub client_cert: Option<PathBuf>,
    #[serde(default)]
    pub client_key: Option<PathBuf>,
}

impl Default for SimulatorConfig {
    fn default() -> Self {
        Self {
            detector_id: "sim-detector-1".into(),
            timing_point_ids: vec!["tp-start".into()],
            subject_ids: vec![],
            timebase_id: "local".into(),
            detection_interval_ms: 1_000,
            count: None,
            node_addr: "127.0.0.1:8463".parse().unwrap(),
            producer_id: "otk-simulator".into(),
            auth_token: None,
            tls: None,
        }
    }
}

pub fn load_from_file(path: &Path) -> Result<SimulatorConfig, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&text)?)
}
