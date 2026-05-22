use timing_node::{load_from_file, run, NodeConfig};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = parse_config();
    run(config).await;
}

fn parse_config() -> NodeConfig {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            if let Some(path) = args.next() {
                match load_from_file(std::path::Path::new(&path)) {
                    Ok(cfg) => return cfg,
                    Err(e) => {
                        eprintln!("error loading config from {path}: {e}");
                        std::process::exit(1);
                    }
                }
            } else {
                eprintln!("--config requires a path argument");
                std::process::exit(1);
            }
        } else {
            eprintln!("unknown argument: {arg}");
            eprintln!("usage: otk-node [--config <path>]");
            std::process::exit(1);
        }
    }
    NodeConfig::default()
}
