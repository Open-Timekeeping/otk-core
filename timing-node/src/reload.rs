//! Config-file hot-reload: watch the TOML on disk, apply auth-token
//! changes atomically without dropping active connections, warn on
//! restart-required changes.
//!
//! # Reloadable vs restart-required
//!
//! | Field                          | Reloadable? |
//! |--------------------------------|-------------|
//! | `auth.producer_tokens`         | yes         |
//! | `auth.api_tokens`              | yes         |
//! | `node_id`                      | no (identity) |
//! | `storage_dir`                  | no (log already open) |
//! | `listeners` (any change)       | no (sockets already bound) |
//! | `api_addr`                     | no (server already bound) |
//! | `api.allowed_origins` (CORS)   | no (axum layer built once) |
//!
//! Restart-required fields are not enforced by config validation
//! against the running instance; the watcher just logs a `warn!` so
//! the operator knows their edit didn't take effect for those fields
//! and is then free to schedule a restart.
//!
//! # Why a debouncer
//!
//! Raw `notify` events fire multiple times per logical save (open →
//! truncate → write → rename on most editors). `notify-debouncer-mini`
//! coalesces these into one event per ~500ms window so we don't
//! re-parse and re-apply on every intermediate filesystem event.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use notify::RecursiveMode;
use notify_debouncer_mini::new_debouncer;
use tracing::{debug, info, warn};

use crate::auth::AuthState;
use crate::config::{load_from_file, NodeConfig};

/// Polling interval for the debouncer. Half a second is well above the
/// editor save-flurry but well below human edit-and-test latency.
const DEBOUNCE_INTERVAL: Duration = Duration::from_millis(500);

/// Spawn a background task that watches `config_path` and applies
/// hot-reloadable changes to the shared [`AuthState`]. Returns a
/// `JoinHandle` so `run_with_shutdown` can await the watcher on its
/// way down.
///
/// `initial` is the parsed config at startup; the watcher diffs each
/// reload against it (well, against the previous successfully-loaded
/// config) so the `warn!` messages name exactly what changed.
///
/// On any reload error (file disappears, TOML parse fails, IO error)
/// the watcher logs the error and keeps watching: a transient bad
/// edit shouldn't take the running node down with it.
pub fn spawn_config_watcher(
    config_path: PathBuf,
    auth: Arc<AuthState>,
    initial: NodeConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // The notify watcher uses a std mpsc channel; we bridge to
        // tokio via a try_recv loop driven by an interval. The
        // debouncer thread itself is a std thread spawned by the
        // crate; we don't manage its lifetime directly, it dies when
        // the debouncer is dropped at task exit.
        let (tx, rx) = std::sync::mpsc::channel();
        let mut debouncer = match new_debouncer(DEBOUNCE_INTERVAL, tx) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    error = %e,
                    "could not start config-file watcher; hot-reload is disabled for this process"
                );
                return;
            }
        };

        // Watch the file's parent dir, not the file directly: many
        // editors do atomic-write-via-rename, which `notify` reports
        // as the original file being deleted. Watching the directory
        // catches the rename target too.
        let watch_target = config_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        if let Err(e) = debouncer
            .watcher()
            .watch(&watch_target, RecursiveMode::NonRecursive)
        {
            warn!(
                path = %watch_target.display(),
                error = %e,
                "could not watch config directory; hot-reload disabled for this process"
            );
            return;
        }
        info!(
            config = %config_path.display(),
            watching = %watch_target.display(),
            "config hot-reload watcher started"
        );

        let mut last = initial;
        let mut poll = tokio::time::interval(Duration::from_millis(250));
        // Skip the missed-tick burst on resume.
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    debug!("config watcher: shutdown signalled");
                    break;
                }
                _ = poll.tick() => {
                    // Drain every debounced event the watcher
                    // produced since the last tick. We don't care
                    // which event triggered the change; we re-read
                    // and diff every time the debouncer sees activity.
                    let mut any = false;
                    while let Ok(events) = rx.try_recv() {
                        match events {
                            Ok(evs) => {
                                if evs.iter().any(|e| e.path == config_path) {
                                    any = true;
                                }
                            }
                            Err(err) => {
                                warn!(error = %err, "config watcher event error");
                            }
                        }
                    }
                    if any {
                        match try_apply_reload(&config_path, &auth, &last) {
                            Ok(new_cfg) => last = new_cfg,
                            Err(e) => {
                                warn!(error = %e, "config reload failed; keeping previous in-memory state");
                            }
                        }
                    }
                }
            }
        }
        info!("config hot-reload watcher stopped");
    })
}

/// Re-parse the config file, apply hot-reloadable changes to `auth`,
/// and emit warns for any restart-required differences. Returns the
/// newly-parsed config so the caller can use it as the next diff
/// baseline.
fn try_apply_reload(
    path: &Path,
    auth: &AuthState,
    prev: &NodeConfig,
) -> Result<NodeConfig, Box<dyn std::error::Error>> {
    let new_cfg = load_from_file(path)?;

    // Reloadable: producer + API tokens. Apply unconditionally; the
    // ArcSwap call is cheap even when the value is unchanged.
    if new_cfg.auth.producer_tokens != prev.auth.producer_tokens {
        let prev_len = prev.auth.producer_tokens.len();
        let new_len = new_cfg.auth.producer_tokens.len();
        auth.set_producer_tokens(new_cfg.auth.producer_tokens.clone());
        info!(
            previous_count = prev_len,
            current_count = new_len,
            "hot-reload: producer_tokens rotated"
        );
    }
    if new_cfg.auth.api_tokens != prev.auth.api_tokens {
        let prev_len = prev.auth.api_tokens.len();
        let new_len = new_cfg.auth.api_tokens.len();
        auth.set_api_tokens(new_cfg.auth.api_tokens.clone());
        info!(
            previous_count = prev_len,
            current_count = new_len,
            "hot-reload: api_tokens rotated"
        );
    }

    // Restart-required: log warns naming what the operator changed
    // but couldn't reload. The new file IS the source of truth on
    // next start; the running node just hasn't picked these up.
    if new_cfg.node_id != prev.node_id {
        warn!(
            previous = %prev.node_id,
            current = %new_cfg.node_id,
            "config change to `node_id` requires a restart; running instance keeps the old value"
        );
    }
    if new_cfg.storage_dir != prev.storage_dir {
        warn!(
            previous = %prev.storage_dir.display(),
            current = %new_cfg.storage_dir.display(),
            "config change to `storage_dir` requires a restart; log stays open at the old path"
        );
    }
    if new_cfg.api_addr != prev.api_addr {
        warn!(
            previous = %prev.api_addr,
            current = %new_cfg.api_addr,
            "config change to `api_addr` requires a restart; API server stays bound to the old address"
        );
    }
    if new_cfg.listeners.len() != prev.listeners.len() {
        warn!(
            previous_count = prev.listeners.len(),
            current_count = new_cfg.listeners.len(),
            "config change to `listeners` (count) requires a restart; ingest sockets stay bound to the old set"
        );
    }
    if new_cfg.api.allowed_origins != prev.api.allowed_origins {
        warn!(
            previous = ?prev.api.allowed_origins,
            current = ?new_cfg.api.allowed_origins,
            "config change to `api.allowed_origins` requires a restart; CORS layer stays at the old allow-list"
        );
    }

    Ok(new_cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::TempDir;

    fn write_config(path: &Path, producer_tokens: &[&str], api_tokens: &[&str]) {
        let mut s = String::from(
            "node_id = \"test\"\napi_addr = \"127.0.0.1:0\"\nstorage_dir = \"./data\"\n\n",
        );
        s.push_str("[[listeners]]\ntransport = \"tcp\"\nid = \"tcp-main\"\nbind_addr = \"127.0.0.1:0\"\n\n");
        s.push_str("[auth]\n");
        s.push_str(&format!(
            "producer_tokens = {:?}\napi_tokens = {:?}\n",
            producer_tokens, api_tokens
        ));
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(s.as_bytes()).unwrap();
        f.sync_all().unwrap();
    }

    #[tokio::test]
    async fn reload_rotates_producer_tokens_when_file_changes() {
        let tmp = TempDir::new().unwrap();
        let cfg_path = tmp.path().join("otk-node.toml");
        write_config(&cfg_path, &["old-producer"], &[]);

        let initial = load_from_file(&cfg_path).unwrap();
        let auth = Arc::new(AuthState::new(
            initial.auth.producer_tokens.clone(),
            initial.auth.api_tokens.clone(),
        ));

        // Sanity: pre-reload state.
        assert_eq!(
            auth.current_producer_tokens().as_slice(),
            &["old-producer".to_string()]
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle =
            spawn_config_watcher(cfg_path.clone(), Arc::clone(&auth), initial, shutdown_rx);

        // Editor save: rewrite the file with a new token. Sleep a
        // beat so the watcher's debouncer is past its initial settle
        // time before the change lands.
        tokio::time::sleep(Duration::from_millis(100)).await;
        write_config(&cfg_path, &["new-producer", "second"], &[]);

        // Poll the auth state for the new tokens; the watcher's
        // debounce window + tokio poll interval bound the wait below
        // a second on a quiet system. Use a generous total budget so
        // a slow CI run isn't flaky.
        let mut got = auth.current_producer_tokens();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while got.as_slice() != ["new-producer".to_string(), "second".to_string()].as_slice()
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            got = auth.current_producer_tokens();
        }
        assert_eq!(
            got.as_slice(),
            &["new-producer".to_string(), "second".to_string()],
            "watcher should have rotated producer_tokens within the poll budget"
        );

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    }

    #[tokio::test]
    async fn reload_does_not_fail_on_invalid_toml() {
        // A bad edit lands; the watcher must log + carry on, not
        // crash. We assert the auth state is unchanged after a bad
        // write, then a subsequent good write applies normally.
        let tmp = TempDir::new().unwrap();
        let cfg_path = tmp.path().join("otk-node.toml");
        write_config(&cfg_path, &["original"], &[]);
        let initial = load_from_file(&cfg_path).unwrap();
        let auth = Arc::new(AuthState::new(initial.auth.producer_tokens.clone(), vec![]));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle =
            spawn_config_watcher(cfg_path.clone(), Arc::clone(&auth), initial, shutdown_rx);

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Bad edit: not valid TOML.
        {
            let mut f = std::fs::File::create(&cfg_path).unwrap();
            f.write_all(b"this is = = = not toml [[\n").unwrap();
            f.sync_all().unwrap();
        }

        // Give the watcher a chance to react and log.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        // State unchanged.
        assert_eq!(
            auth.current_producer_tokens().as_slice(),
            &["original".to_string()]
        );

        // Good edit: should apply.
        write_config(&cfg_path, &["recovered"], &[]);
        let mut got = auth.current_producer_tokens();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while got.as_slice() != ["recovered".to_string()].as_slice()
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            got = auth.current_producer_tokens();
        }
        assert_eq!(got.as_slice(), &["recovered".to_string()]);

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    }
}
