use otk_sdk::producer::{ProducerConfig, TlsClientConfig as SdkTlsClientConfig, Transport};
use producer_simulated::{config::load_from_file, runner, SimulatorAdapter, SimulatorConfig};
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let sim_config = parse_config();
    // Build the transport from the config. `tls` present → TLS, else plain TCP.
    // Doing the TLS-config translation here keeps the producer-simulated
    // config struct serde-friendly and otk-sdk's TlsClientConfig out of
    // the public TOML surface.
    let transport = match sim_config.tls.as_ref() {
        Some(tls) => Transport::Tls {
            addr: sim_config.node_addr,
            config: SdkTlsClientConfig {
                trust_roots: tls.trust_roots.clone(),
                server_name: tls.server_name.clone(),
                client_cert: tls.client_cert.clone(),
                client_key: tls.client_key.clone(),
            },
        },
        None => Transport::Tcp(sim_config.node_addr),
    };
    let mut producer_config = ProducerConfig::new(sim_config.producer_id.clone());
    if let Some(token) = sim_config.auth_token.as_ref() {
        producer_config = producer_config.with_auth_token(token);
    }

    let tls_mode = if sim_config.tls.is_some() {
        if sim_config
            .tls
            .as_ref()
            .map(|t| t.client_cert.is_some())
            .unwrap_or(false)
        {
            "mtls"
        } else {
            "tls"
        }
    } else {
        "tcp"
    };
    info!(
        detector_id = %sim_config.detector_id,
        node_addr = %sim_config.node_addr,
        transport = tls_mode,
        count = ?sim_config.count,
        interval_ms = sim_config.detection_interval_ms,
        "starting simulator"
    );

    let adapter = SimulatorAdapter::new(sim_config);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut handle = tokio::spawn(runner::run(
        adapter,
        transport,
        producer_config,
        shutdown_rx,
    ));

    tokio::select! {
        result = &mut handle => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("task error: {e}");
                    std::process::exit(1);
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("interrupted, shutting down");
            let _ = shutdown_tx.send(true);
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => eprintln!("error during shutdown: {e}"),
                Err(e) => eprintln!("task error: {e}"),
            }
        }
    }
}

fn parse_config() -> SimulatorConfig {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--config") => {
            let path = args.next().unwrap_or_else(|| {
                eprintln!("--config requires a path argument");
                std::process::exit(1);
            });
            load_from_file(std::path::Path::new(&path)).unwrap_or_else(|e| {
                eprintln!("failed to load config: {e}");
                std::process::exit(1);
            })
        }
        Some(arg) => {
            eprintln!("unknown argument: {arg}");
            eprintln!("usage: otk-simulator [--config <path>]");
            std::process::exit(1);
        }
        None => SimulatorConfig::default(),
    }
}
