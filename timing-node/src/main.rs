use std::path::PathBuf;

use timing_node::{run, run_from_config_path, NodeConfig};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Distinguish "loaded from a file on disk" from "running with
    // built-in defaults". The file-path case enables config hot-
    // reload; the default-config case skips the watcher (there's no
    // file to watch).
    match parse_config_source() {
        ConfigSource::Default => run(NodeConfig::default()).await,
        ConfigSource::File(path) => run_from_config_path(path).await,
    }
}

enum ConfigSource {
    Default,
    File(PathBuf),
}

const USAGE: &str = "\
usage: otk-node [OPTIONS]

Open Timekeeping runtime node.

Options:
  -c, --config <PATH>   Path to a TOML config file.
                        If omitted, NodeConfig::default() is used
                        (one TCP listener on 0.0.0.0:8463, storage in ./data).
  -h, --help            Print this help text and exit (status 0).
  -V, --version         Print the package version and exit (status 0).

Environment:
  RUST_LOG              tracing-subscriber filter (default: \"info\").
";

// `while let` rather than `if let`: every current branch either returns
// or exits, so today the loop only runs one iteration (clippy::never_loop
// correctly notices) - but the shape is what we want as soon as a flag
// takes more than one positional arg or repeats. Allowing the lint at
// the fn level keeps the future extension obvious instead of churning
// the structure.
#[allow(clippy::never_loop)]
fn parse_config_source() -> ConfigSource {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "-V" | "--version" => {
                println!("otk-node {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            "-c" | "--config" => match args.next() {
                Some(path) => return ConfigSource::File(PathBuf::from(path)),
                None => {
                    eprintln!("--config requires a path argument");
                    eprintln!();
                    eprintln!("{USAGE}");
                    std::process::exit(1);
                }
            },
            _ => {
                eprintln!("unknown argument: {arg}");
                eprintln!();
                eprintln!("{USAGE}");
                std::process::exit(1);
            }
        }
    }
    ConfigSource::Default
}
