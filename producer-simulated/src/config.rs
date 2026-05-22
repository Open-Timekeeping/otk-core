use std::{net::SocketAddr, path::Path};

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
        }
    }
}

pub fn load_from_file(path: &Path) -> Result<SimulatorConfig, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&text)?)
}
