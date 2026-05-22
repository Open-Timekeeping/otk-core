use otk_sdk::producer::{ProducerConfig, Transport};
use producer_simulated::{config::load_from_file, runner, SimulatorAdapter, SimulatorConfig};
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let sim_config = parse_config();
    let transport = Transport::Tcp(sim_config.node_addr);
    let producer_config = ProducerConfig::new(sim_config.producer_id.clone());

    info!(
        detector_id = %sim_config.detector_id,
        node_addr = %sim_config.node_addr,
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
