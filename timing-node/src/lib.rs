//! Open Timekeeping runtime node library.
//!
//! Exposes [`Node`] and [`NodeConfig`] so that integration tests and embedders can
//! construct and drive a node programmatically without going through the CLI.

mod api;
mod auth;
mod config;
mod ingest;
mod metrics;
mod pipeline;
mod ports;
mod sequence_gate;
mod trace_context;

pub use config::{load_from_file, ApiConfig, AuthConfig, ListenerConfig, NodeConfig};
pub use metrics::Metrics;
pub use pipeline::{AppendOutcome, NodePipeline};
pub use ports::{EventEntry, EventPage, EventQueryPort, QueryError};
pub use sequence_gate::{GateDecision, SequenceGate};

use std::sync::Arc;

use adapter_event_log_segment::{SegmentLog, SegmentLogConfig};
use adapter_ingest_tcp::{TcpIngestConfig, TcpIngestPort};
use api::AppState;
use auth::build_producer_authoriser;
use ingest::run_listener;
use port_in_ingest::EventIngestPort;
use tracing::{debug, info, warn};

/// Bound address of one ingest listener; per-transport.
#[derive(Debug, Clone)]
enum BoundListenerAddr {
    Tcp(std::net::SocketAddr),
    #[cfg(unix)]
    UnixSocket(std::path::PathBuf),
}

impl std::fmt::Display for BoundListenerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(a) => write!(f, "tcp://{a}"),
            #[cfg(unix)]
            Self::UnixSocket(p) => write!(f, "unix:{}", p.display()),
        }
    }
}

/// One bound ingest listener, ready to be run.
struct BoundIngestListener {
    id: String,
    addr: BoundListenerAddr,
    port: Box<dyn EventIngestPort>,
}

/// The OTK runtime node.
pub struct Node {
    ingest: Vec<BoundIngestListener>,
    api_listener: tokio::net::TcpListener,
    pipeline: Arc<NodePipeline>,
    metrics: Arc<Metrics>,
    api_tokens: Vec<String>,
    allowed_origins: Vec<String>,
    node_id: String,
}

impl Node {
    pub async fn new(config: NodeConfig) -> Result<Self, Box<dyn std::error::Error>> {
        if config.listeners.is_empty() {
            return Err("no ingest listeners configured".into());
        }

        if config.auth.producer_tokens.is_empty() {
            warn!("auth.producer_tokens is empty: all producer Connects will be accepted unauthenticated");
        }
        if config.auth.api_tokens.is_empty() {
            warn!("auth.api_tokens is empty: API requests will be accepted unauthenticated");
        }

        // Listener ids are used as metrics labels (otk_ingest_sessions_*)
        // and as the lookup key for listener_tcp_addr / listener_unix_path.
        // Duplicates would merge metrics series and make those accessors
        // ambiguous, so reject duplicates up front with a clear error
        // naming both the conflicting id and its index in the listeners
        // list.
        let mut seen_ids: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for (index, listener) in config.listeners.iter().enumerate() {
            let id = match listener {
                ListenerConfig::Tcp { id, .. } => id.as_str(),
                #[cfg(unix)]
                ListenerConfig::UnixSocket { id, .. } => id.as_str(),
                #[cfg(not(unix))]
                ListenerConfig::UnixSocket { id, .. } => id.as_str(),
            };
            if let Some(&first) = seen_ids.get(id) {
                return Err(format!(
                    "duplicate listener id {id:?}: appears at listeners[{first}] and listeners[{index}]; \
                     each listener must have a unique id (used in metrics labels and listener_*_addr lookups)"
                )
                .into());
            }
            seen_ids.insert(id, index);
        }

        let authoriser = build_producer_authoriser(&config.auth.producer_tokens);

        let mut ingest = Vec::with_capacity(config.listeners.len());
        for listener in &config.listeners {
            match listener {
                ListenerConfig::Tcp {
                    id,
                    bind_addr,
                    max_frame_bytes,
                } => {
                    let ingest_config = TcpIngestConfig {
                        bind_addr: *bind_addr,
                        max_frame_bytes: *max_frame_bytes,
                        handshake_timeout: std::time::Duration::from_secs(5),
                    };
                    let port =
                        TcpIngestPort::bind_with_auth(ingest_config, Arc::clone(&authoriser))
                            .await?;
                    let addr = port.local_addr()?;
                    ingest.push(BoundIngestListener {
                        id: id.clone(),
                        addr: BoundListenerAddr::Tcp(addr),
                        port: Box::new(port),
                    });
                }
                #[cfg(unix)]
                ListenerConfig::UnixSocket {
                    id,
                    socket_path,
                    max_frame_bytes,
                    socket_permissions,
                    force_rebind,
                } => {
                    let cfg = adapter_ingest_unix_socket::UnixSocketIngestConfig {
                        socket_path: socket_path.clone(),
                        max_frame_bytes: *max_frame_bytes,
                        handshake_timeout: std::time::Duration::from_secs(5),
                        socket_permissions: *socket_permissions,
                        force_rebind: *force_rebind,
                    };
                    let port = adapter_ingest_unix_socket::UnixSocketIngestPort::bind_with_auth(
                        cfg,
                        Arc::clone(&authoriser),
                    )
                    .await?;
                    let path = port.socket_path().to_path_buf();
                    ingest.push(BoundIngestListener {
                        id: id.clone(),
                        addr: BoundListenerAddr::UnixSocket(path),
                        port: Box::new(port),
                    });
                }
                #[cfg(not(unix))]
                ListenerConfig::UnixSocket { id, .. } => {
                    return Err(format!(
                        "listener {id:?}: unix-socket transport is only supported on Unix targets"
                    )
                    .into());
                }
            }
        }

        let api_listener = tokio::net::TcpListener::bind(config.api_addr).await?;

        let log_config = SegmentLogConfig {
            dir: config.storage_dir.clone(),
            ..SegmentLogConfig::default()
        };
        let log = SegmentLog::open(log_config).await?;
        let metrics = Arc::new(Metrics::new());
        let pipeline = Arc::new(NodePipeline::new(Box::new(log), Arc::clone(&metrics)));

        Ok(Self {
            ingest,
            api_listener,
            pipeline,
            metrics,
            api_tokens: config.auth.api_tokens,
            allowed_origins: config.api.allowed_origins,
            node_id: config.node_id,
        })
    }

    /// Returns the address of the first configured **TCP** listener,
    /// regardless of position in the listener list. Convenience for tests
    /// and operators that run at least one TCP listener.
    ///
    /// # Panics
    ///
    /// Panics if **no TCP listener is configured at all**, including
    /// the case where the node is configured with one or more
    /// Unix-socket listeners but no TCP listener. The earlier
    /// implementation panicked any time the *first* configured listener
    /// happened to be a Unix-socket listener; that's been fixed, but a
    /// Unix-only config still has no `SocketAddr` to return and will
    /// trip this `expect`. For mixed-listener or Unix-only deployments,
    /// use [`Node::listener_tcp_addr`] (returns `Option<SocketAddr>`)
    /// or `Node::listener_unix_path` (Unix-only; cfg(unix)) keyed by `id`.
    // `find_map` is the semantically right call here: on Unix, the
    // `BoundListenerAddr::UnixSocket(_) => None` arm exists and we want
    // to skip Unix-socket listeners while looking for the first TCP
    // listener. On non-Unix targets the cfg gate removes that arm
    // entirely, leaving only the Tcp arm, and clippy notices that the
    // remaining match always returns Some - hence the lint. The lint is
    // correct on Windows in isolation but the cross-platform form has to
    // stay `find_map` to compile on Unix.
    #[allow(clippy::unnecessary_find_map)]
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.ingest
            .iter()
            .find_map(|l| match &l.addr {
                BoundListenerAddr::Tcp(a) => Some(*a),
                #[cfg(unix)]
                BoundListenerAddr::UnixSocket(_) => None,
            })
            .expect(
                "local_addr() requires at least one TCP listener; \
                 use listener_tcp_addr(id) or listener_unix_path(id) for mixed configs",
            )
    }

    // and_then (rather than map) because on Unix the inner match has a
    // None-returning arm. cargo clippy --fix on Windows (where the cfg
    // gate removes the Unix arm) rewrote this to .map(...) returning a
    // bare SocketAddr; that change doesn't compile on Linux. The right
    // cross-platform form is and_then with the Tcp arm returning Some.
    #[allow(clippy::bind_instead_of_map)]
    pub fn listener_tcp_addr(&self, id: &str) -> Option<std::net::SocketAddr> {
        self.ingest
            .iter()
            .find(|l| l.id == id)
            .and_then(|l| match &l.addr {
                BoundListenerAddr::Tcp(a) => Some(*a),
                #[cfg(unix)]
                BoundListenerAddr::UnixSocket(_) => None,
            })
    }

    #[cfg(unix)]
    pub fn listener_unix_path(&self, id: &str) -> Option<&std::path::Path> {
        self.ingest
            .iter()
            .find(|l| l.id == id)
            .and_then(|l| match &l.addr {
                BoundListenerAddr::UnixSocket(p) => Some(p.as_path()),
                BoundListenerAddr::Tcp(_) => None,
            })
    }

    pub fn api_addr(&self) -> std::net::SocketAddr {
        self.api_listener
            .local_addr()
            .expect("api listener has a local addr")
    }

    /// Run the node until either Ctrl-C arrives or a spawned task
    /// terminates early.
    ///
    /// Returns `Ok(())` on a clean Ctrl-C shutdown where every task
    /// drained without panic or error, and `Err(ShutdownError)` if
    /// any spawned task panicked, returned an error, exited
    /// unexpectedly before the shutdown signal, or failed to drain
    /// within `SHUTDOWN_DEADLINE` (a private constant, 10 s). The caller is expected to
    /// translate `Err` into a non-zero process exit so supervisors
    /// (systemd, Kubernetes, etc.) restart the node. Just logging
    /// and returning `Ok` would let the node exit with status 0
    /// after a panic, which would suppress automatic restart on
    /// every supervisor we care about.
    ///
    /// The drain is bounded by `SHUTDOWN_DEADLINE` so a long-lived
    /// HTTP connection (notably a still-open SSE subscription on
    /// `/api/v1/events/stream`, which `axum::serve(...).
    /// with_graceful_shutdown(...)` cannot itself terminate) cannot
    /// pin the API task indefinitely and hang `systemctl stop`.
    /// Tasks still running at the deadline are aborted via
    /// `JoinSet::shutdown` and the function returns
    /// `Err(ShutdownError::DrainTimedOut)`.
    pub async fn run_until_shutdown(self) -> Result<(), ShutdownError> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (ingest_tasks, api_task) = self.spawn_tasks(shutdown_rx);

        // Wrap each spawned task in a small JoinSet entry that pairs it
        // with a friendly name, so the `select!` below can react to ANY
        // of them ending before the shutdown signal arrives. The previous
        // version only selected on `ctrl_c()`: if (say) the API server
        // died mid-flight because of a port-bind race after restart, the
        // node kept "running" in a half-broken state and the operator
        // only saw the failure once they hit Ctrl-C themselves. Now an
        // early task exit triggers shutdown for the rest and the
        // original cause is logged at error/warn so it shows up in the
        // journal.
        //
        // # AbortHandle bookkeeping
        //
        // We ALSO capture the underlying tasks' `AbortHandle` before
        // moving the `JoinHandle` into the wrapper. The wrapper's body
        // just awaits the inner `JoinHandle` to forward the join result,
        // so calling `JoinSet::shutdown().await` aborts the WRAPPER but
        // dropping a `JoinHandle` does NOT cancel the underlying task.
        // On the drain-timeout path we therefore have to abort the real
        // ingest/API tasks via their captured `AbortHandle`s. Without
        // this, a timed-out shutdown would orphan the API server / ingest
        // listener: they'd keep accepting work after `run_until_shutdown`
        // returned, with no way to reach them.
        let mut tasks: tokio::task::JoinSet<(String, Result<(), tokio::task::JoinError>)> =
            tokio::task::JoinSet::new();
        let mut underlying_aborts: Vec<(String, tokio::task::AbortHandle)> = Vec::new();
        for (i, t) in ingest_tasks.into_iter().enumerate() {
            let name = format!("ingest[{i}]");
            underlying_aborts.push((name.clone(), t.abort_handle()));
            tasks.spawn(async move {
                let res = t.await;
                (name, res)
            });
        }
        let api_name = "api".to_string();
        underlying_aborts.push((api_name.clone(), api_task.abort_handle()));
        tasks.spawn(async move {
            let res = api_task.await;
            (api_name, res)
        });

        // Track whether anything went wrong so we can return Err at the
        // end. `first_failure` records the earliest failure for the
        // returned error; subsequent failures still log at error level.
        let mut first_failure: Option<ShutdownError> = None;

        let exit_reason: &'static str = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received; shutting down");
                "ctrl_c"
            }
            Some(joined) = tasks.join_next() => {
                let (level, err) = classify_unexpected_exit(joined);
                report_classified_exit(level, &err);
                first_failure.get_or_insert(err);
                "task_exit"
            }
        };

        let _ = shutdown_tx.send(true);

        // Drain whatever is still running, bounded by SHUTDOWN_DEADLINE.
        // `shutdown_tx` has already triggered both the ingest listeners
        // (which check `shutdown_rx` in their accept loop) and the API
        // server (which has `with_graceful_shutdown`). Most of the time
        // they settle within milliseconds, but `axum::serve(...).
        // with_graceful_shutdown(...)` only stops accepting NEW
        // connections; it waits for in-flight ones to finish. A
        // long-lived SSE subscription on `/api/v1/events/stream` has
        // no natural finish, so without an upper bound here a single
        // open SSE client could pin the API task and prevent shutdown
        // indefinitely. That stalls Ctrl-C, blocks systemd's stop
        // step, and looks like a hang to operators.
        //
        // After the deadline expires, abort every remaining task via
        // `JoinSet::shutdown()` (which calls `abort()` on each handle
        // and then awaits them) and surface a `ShutdownError::DrainTimedOut`
        // so the process exits non-zero. Cancellations (the normal
        // shutdown path) log at info; panics and errors log at error
        // AND are captured for the returned Result.
        let drain_outcome = {
            // Borrow `tasks` (not move) so we can still call
            // `tasks.shutdown()` and `tasks.len()` on the timeout path.
            // The borrow is bounded by this scope; the `timeout` future
            // drops its inner future when it returns, which releases the
            // borrow before we reach the `match` below.
            let drain = async {
                while let Some(joined) = tasks.join_next().await {
                    match joined {
                        Ok((name, Ok(()))) => {
                            debug!(task = %name, "task drained cleanly");
                        }
                        Ok((name, Err(e))) if e.is_cancelled() => {
                            info!(task = %name, "task cancelled at shutdown");
                        }
                        Ok((name, Err(e))) if e.is_panic() => {
                            let cause = join_error_cause(e);
                            tracing::error!(task = %name, cause = %cause, "task panicked during shutdown drain");
                            first_failure
                                .get_or_insert(ShutdownError::TaskPanicked { name, cause });
                        }
                        Ok((name, Err(e))) => {
                            let cause = join_error_cause(e);
                            tracing::error!(task = %name, cause = %cause, "task ended with error during shutdown drain");
                            first_failure.get_or_insert(ShutdownError::TaskFailed { name, cause });
                        }
                        Err(e) => {
                            let cause = join_error_cause(e);
                            tracing::error!(cause = %cause, "join-set monitor task panicked");
                            first_failure.get_or_insert(ShutdownError::MonitorPanicked { cause });
                        }
                    }
                }
            };
            tokio::time::timeout(SHUTDOWN_DEADLINE, drain).await
        };

        match drain_outcome {
            Ok(()) => {
                info!(reason = exit_reason, "otk-node stopped");
            }
            Err(_elapsed) => {
                // Drain didn't complete in time. Abort the REAL ingest/API
                // tasks (not the wrapper tasks in the JoinSet, which only
                // forward the join result). Without this, calling
                // `tasks.shutdown()` would abort the wrappers and drop
                // their `JoinHandle`s, but a dropped `JoinHandle` does NOT
                // cancel its underlying task: the API server would keep
                // running orphaned after we return. Aborting the underlying
                // handles is what actually stops the work.
                tracing::error!(
                    deadline_secs = SHUTDOWN_DEADLINE.as_secs(),
                    remaining = tasks.len(),
                    "shutdown drain timed out; aborting remaining tasks"
                );
                for (name, h) in &underlying_aborts {
                    if !h.is_finished() {
                        tracing::warn!(task = %name, "aborting underlying task after drain timeout");
                        h.abort();
                    }
                }
                // Aborting the underlying tasks will make each wrapper's
                // `await` resolve to `Err(JoinError::is_cancelled())`,
                // letting `tasks.shutdown().await` finish promptly without
                // leaving any orphaned join entries behind.
                tasks.shutdown().await;
                first_failure.get_or_insert(ShutdownError::DrainTimedOut {
                    deadline_secs: SHUTDOWN_DEADLINE.as_secs(),
                });
                info!(reason = exit_reason, "otk-node stopped (drain timed out)");
            }
        }
        match first_failure {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }

    /// Spawn the ingest and API tasks against an externally-supplied
    /// shutdown receiver and return their `JoinHandle`s. Used by tests
    /// that need to drive the node from inside `#[tokio::test]` and
    /// reach the bound addresses before sending traffic.
    ///
    /// Synchronous (not `async`) because the underlying `spawn_tasks`
    /// only calls `tokio::spawn`, which itself is sync. Leaving this
    /// `async` for symmetry with `run_until_shutdown` would have built
    /// a one-shot state machine for no reason and tripped
    /// `clippy::unused_async`.
    pub fn run_with_shutdown(
        self,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> (
        Vec<tokio::task::JoinHandle<()>>,
        tokio::task::JoinHandle<()>,
    ) {
        self.spawn_tasks(shutdown_rx)
    }

    fn spawn_tasks(
        self,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> (
        Vec<tokio::task::JoinHandle<()>>,
        tokio::task::JoinHandle<()>,
    ) {
        // Destructure `self` up front rather than relying on the compiler's
        // disjoint-capture rules to let `async move { ... self.api_listener
        // ... }` co-exist with a later `self.ingest.into_iter()`. The
        // disjoint-capture behaviour is correct today, but it's a subtle
        // language feature: a future edit that incidentally references any
        // other field of `self` inside the API task (for example, in an
        // error-path log line) would silently capture the whole struct and
        // break the per-field moves below with a confusing "use of moved
        // value: `self`" error. Naming each piece makes the move explicit
        // and survives such edits.
        let Node {
            ingest,
            api_listener,
            pipeline,
            metrics,
            api_tokens,
            allowed_origins,
            node_id,
        } = self;

        let ingest_summary: Vec<(String, String)> = ingest
            .iter()
            .map(|l| (l.id.clone(), l.addr.to_string()))
            .collect();
        let api_addr = api_listener
            .local_addr()
            .expect("api listener has a local addr");
        info!(
            api_addr = %api_addr,
            node_id = %node_id,
            listeners = ?ingest_summary,
            "otk-node starting"
        );

        // Use a typed `let` binding to drive the `Arc<NodePipeline>` ->
        // `Arc<dyn EventQueryPort>` conversion via Rust's unsized-coercion
        // rules rather than an `as` cast. Functionally equivalent here
        // (the `as` form does happen to compile via the same coercion),
        // but the explicit binding makes the trait-object conversion
        // visible to the reader and keeps it on the standard,
        // statically-checked coercion path.
        //
        // Note: `Arc::clone(&pipeline)` directly into `Arc<dyn ...>` does
        // NOT compile because turbofish-style cloning fixes the type as
        // `Arc<NodePipeline>` before any coercion site sees it. Going
        // via `pipeline.clone()` works because the let-binding is the
        // coercion site and the right-hand side is just an `Arc<NodePipeline>`
        // that the binding converts.
        let query: Arc<dyn EventQueryPort> = pipeline.clone();
        let api_state = AppState {
            node_id: Arc::<str>::from(node_id.as_str()),
            query,
            metrics: Arc::clone(&metrics),
            api_tokens: Arc::new(api_tokens),
        };
        let api_router = api::router(api_state, &allowed_origins);
        let mut api_shutdown_rx = shutdown_rx.clone();
        let api_task = tokio::spawn(async move {
            // Propagate `axum::serve` failures by panicking inside the
            // task. The previous code used `unwrap_or_else(|e|
            // tracing::error!(...))` which logged the error but had the
            // task resolve `Ok(())`, so `run_until_shutdown`'s
            // `JoinHandle` match couldn't tell a clean shutdown apart
            // from an API server that died unexpectedly. Panicking here
            // means `JoinError::is_panic()` catches it at join time and
            // the operator sees an error-level log naming the API task.
            if let Err(e) = axum::serve(api_listener, api_router)
                .with_graceful_shutdown(async move {
                    api_shutdown_rx.changed().await.ok();
                })
                .await
            {
                tracing::error!(error = %e, "API server stopped with error");
                panic!("API server failure: {e}");
            }
        });

        let ingest_tasks: Vec<_> = ingest
            .into_iter()
            .map(|listener| {
                let pipeline = Arc::clone(&pipeline);
                let metrics = Arc::clone(&metrics);
                let shutdown_rx = shutdown_rx.clone();
                let id = listener.id.clone();
                tokio::spawn(async move {
                    info!(listener_id = %id, addr = %listener.addr, "ingest listener spawned");
                    run_listener(listener.port, id, pipeline, metrics, shutdown_rx).await;
                })
            })
            .collect();

        (ingest_tasks, api_task)
    }
}

/// Outcome of `Node::run_until_shutdown` reported back to the caller.
///
/// Only failure modes worth surfacing to a process supervisor are
/// represented; clean Ctrl-C shutdowns return `Ok(())`. The supervisor
/// (systemd, Kubernetes, etc.) keys off the process exit code, and
/// `run()` translates each variant into `exit(1)` so the supervisor
/// restarts the node rather than treating the crash as a clean stop.
///
/// The panic / error variants carry a `cause` string captured at the
/// point of detection so the diagnostic survives across the `run()` →
/// `exit(1)` boundary, even when the operator only has the final
/// fatal line in the journal and not the per-task warn/error logs.
/// The panic payload, when downcastable to `&str` / `String`, is
/// preferred over the synthesized `JoinError::Display` text.
#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    #[error("task `{name}` panicked: {cause}")]
    TaskPanicked { name: String, cause: String },
    #[error("task `{name}` ended with an error: {cause}")]
    TaskFailed { name: String, cause: String },
    #[error("task `{name}` exited before shutdown signal")]
    TaskExitedEarly { name: String },
    #[error("join-set monitor task panicked: {cause}")]
    MonitorPanicked { cause: String },
    #[error("shutdown drain exceeded {deadline_secs}s deadline; remaining tasks were aborted")]
    DrainTimedOut { deadline_secs: u64 },
}

/// Upper bound on how long `run_until_shutdown` waits for spawned tasks
/// to settle after the shutdown signal fires. Any task still running
/// past this deadline (typically a long-lived SSE subscription on
/// `/api/v1/events/stream` that `axum`'s graceful-shutdown wraps but
/// can't itself end) is forcibly aborted via `JoinSet::shutdown`.
///
/// 10s is comfortably longer than a clean drain (which finishes in
/// milliseconds once `shutdown_tx` fires) but short enough that a
/// hung shutdown is caught well inside systemd's default
/// `TimeoutStopSec=90s`, leaving the supervisor plenty of headroom
/// to send SIGKILL if we ever miss our own deadline too.
const SHUTDOWN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// Best-effort human-readable cause string from a `JoinError`.
///
/// `JoinError::into_panic()` returns the original `Box<dyn Any + Send>`
/// payload, which is typically a `String` or `&'static str` produced
/// by `panic!`. We downcast in both shapes; if both fail, fall back to
/// the `JoinError`'s own `Display` (which says something like "task
/// `<id>` panicked"). For non-panic errors, just use `Display`.
fn join_error_cause(e: tokio::task::JoinError) -> String {
    if e.is_panic() {
        // `into_panic()` consumes the error to recover the payload.
        let payload = e.into_panic();
        if let Some(s) = payload.downcast_ref::<&'static str>() {
            return (*s).to_string();
        }
        if let Some(s) = payload.downcast_ref::<String>() {
            return s.clone();
        }
        return "panic payload not a string".to_string();
    }
    e.to_string()
}

/// Convert one early-exit observation from the monitor `JoinSet` into a
/// log level and a typed `ShutdownError`.
///
/// The outer `Result` is from the wrapper task we spawned around the
/// real `JoinHandle`; the inner is the real task's join result. We
/// distinguish panic / cancelled / clean-but-unexpected so the operator
/// can read the journal and tell apart "the API server crashed" from
/// "ingest accepted a clean disconnect" from "something panicked in the
/// wrapper itself", AND so the right `ShutdownError` variant (with the
/// original panic payload preserved) bubbles up to drive a non-zero
/// process exit.
fn classify_unexpected_exit(
    joined: Result<(String, Result<(), tokio::task::JoinError>), tokio::task::JoinError>,
) -> (tracing::Level, ShutdownError) {
    match joined {
        Ok((name, Ok(()))) => (
            tracing::Level::WARN,
            ShutdownError::TaskExitedEarly { name },
        ),
        Ok((name, Err(e))) if e.is_panic() => {
            let cause = join_error_cause(e);
            (
                tracing::Level::ERROR,
                ShutdownError::TaskPanicked { name, cause },
            )
        }
        Ok((name, Err(e))) => {
            let cause = join_error_cause(e);
            (
                tracing::Level::ERROR,
                ShutdownError::TaskFailed { name, cause },
            )
        }
        Err(e) => {
            let cause = join_error_cause(e);
            (
                tracing::Level::ERROR,
                ShutdownError::MonitorPanicked { cause },
            )
        }
    }
}

fn report_classified_exit(level: tracing::Level, err: &ShutdownError) {
    // `err.Display` now includes the underlying cause string, so the
    // operator gets the panic message inline in the journal without
    // needing to chain through `error.source()`.
    match level {
        tracing::Level::ERROR => {
            tracing::error!(error = %err, "spawned task exited before shutdown signal");
        }
        tracing::Level::WARN => {
            tracing::warn!(error = %err, "spawned task exited before shutdown signal");
        }
        _ => {
            tracing::info!(error = %err, "spawned task exited before shutdown signal");
        }
    }
}

/// CLI entry point. Builds the node, runs it, and translates any
/// `ShutdownError` into a non-zero process exit so systemd / Kubernetes
/// / launchd restart the node on panic instead of seeing a clean exit.
///
/// Previously this awaited `run_until_shutdown` for its side effects
/// only: a panicked API task would log at error and the process would
/// still exit 0, suppressing every supervisor's automatic-restart
/// behaviour. Now panics, errors, and unexpected early exits all
/// surface as `exit(1)`.
pub async fn run(config: NodeConfig) {
    let node = match Node::new(config).await {
        Ok(n) => n,
        Err(e) => {
            eprintln!("fatal: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = node.run_until_shutdown().await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}
