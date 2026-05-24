use std::sync::Arc;

use port_in_ingest::{EventIngestPort, IngestError, IngestSession};
use tracing::{debug, error, info, Instrument};

use crate::metrics::Metrics;
use crate::pipeline::NodePipeline;
use crate::trace_context::apply_traceparent;

/// RAII guard that decrements `ingest_sessions_active` for a listener when
/// dropped, regardless of how the session task ended (clean return, error,
/// panic, abort). Without this guard, a session task panicking would leave
/// the gauge stuck at +1 indefinitely.
struct ActiveSessionGuard {
    metrics: Arc<Metrics>,
    listener_id: String,
}

impl Drop for ActiveSessionGuard {
    fn drop(&mut self) {
        self.metrics
            .ingest_sessions_active
            .dec(&[("listener_id", &self.listener_id)]);
    }
}

/// Accept loop. Spawns a task per producer session; drains all active tasks on shutdown.
///
/// `listener_id` is the per-listener label used in the `listener_id` Prometheus
/// label on session-count metrics.
pub async fn run_listener(
    port: Box<dyn EventIngestPort>,
    listener_id: String,
    pipeline: Arc<NodePipeline>,
    metrics: Arc<Metrics>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    info!(listener = %listener_id, "ingest listener ready");
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            result = port.accept() => {
                match result {
                    Ok(session) => {
                        let pipeline = Arc::clone(&pipeline);
                        let metrics = Arc::clone(&metrics);
                        let listener_id = listener_id.clone();
                        metrics.ingest_sessions_total.incr(&[("listener_id", &listener_id)]);
                        metrics.ingest_sessions_active.inc(&[("listener_id", &listener_id)]);
                        // Construct the guard BEFORE `tasks.spawn(...)` and
                        // move it into the future. If the guard were created
                        // inside the `async move` block it would only
                        // materialise on first poll, so a JoinSet that gets
                        // dropped (e.g. shutdown drain timed out and aborted
                        // every task) before the task ever ran would leak
                        // the gauge increment we already made above. Owning
                        // the guard outside the future ties the increment
                        // and the matching decrement to the same scope.
                        let active = ActiveSessionGuard {
                            metrics: Arc::clone(&metrics),
                            listener_id: listener_id.clone(),
                        };
                        tasks.spawn(async move {
                            // RAII: `active` is dropped on any exit path
                            // (clean return, error, panic) and now also on
                            // abort-before-first-poll because it's owned by
                            // the future itself.
                            let _active = active;
                            handle_session(session, pipeline).await;
                        });
                    }
                    Err(IngestError::Closed) => {
                        info!(listener = %listener_id, "ingest port closed");
                        break;
                    }
                    Err(e) => {
                        error!(listener = %listener_id, error = %e, "accept error");
                    }
                }
            }
            // Reap finished session tasks during the accept loop so a
            // long-running node with many short-lived producer sessions
            // doesn't accumulate `JoinSet` entries forever and leak
            // memory between shutdowns. `JoinSet::join_next()` resolves
            // to `None` immediately when the set is empty (not Pending),
            // so this arm relies on the `Some(res) = ...` pattern guard
            // to disable itself in that case rather than parking. When
            // the set is empty the arm is skipped and `select!` waits
            // on the other arms only, so an idle listener doesn't spin
            // here and accept isn't starved. Once a session finishes,
            // the next iteration's join_next() returns Some(res) and
            // the slot is drained. Panics still surface (here, at error
            // level) just like they would at shutdown drain.
            Some(res) = tasks.join_next() => {
                match res {
                    Ok(()) => {
                        debug!(listener = %listener_id, "session task finished cleanly");
                    }
                    Err(e) if e.is_cancelled() => {
                        // We don't normally cancel sessions outside of
                        // shutdown, but log it at debug if it ever happens
                        // mid-accept-loop rather than silently dropping it.
                        debug!(listener = %listener_id, "session task cancelled mid-accept-loop");
                    }
                    Err(e) => {
                        error!(listener = %listener_id, error = %e, "session task panicked");
                    }
                }
            }
            _ = shutdown.changed() => {
                info!(listener = %listener_id, "shutdown signal; stopping accept loop");
                break;
            }
        }
    }
    // Drain spawned session tasks. Surface panics (JoinError::is_panic())
    // and cancellations distinctly so an operator can tell a buggy session
    // handler from a normal shutdown cancellation. Wrap the whole drain in
    // a 5-second timeout so a hung session can't block shutdown forever.
    let drain = async {
        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {
                    debug!(listener = %listener_id, "session cancelled at shutdown");
                }
                Err(e) => {
                    error!(listener = %listener_id, error = %e, "session task panicked");
                }
            }
        }
    };
    if tokio::time::timeout(std::time::Duration::from_secs(5), drain)
        .await
        .is_err()
    {
        tracing::warn!(listener = %listener_id, "shutdown: drain timed out; forcing sessions closed");
    }
}

async fn handle_session(mut session: Box<dyn IngestSession>, pipeline: Arc<NodePipeline>) {
    let producer_id = session.producer_id().to_string();
    let peer_addr = session.peer_addr().to_string();
    info!(producer = %producer_id, peer = %peer_addr, "session started");

    loop {
        match session.next_event().await {
            Ok(Some(incoming)) => {
                // Create a per-event span. When the producer supplied a
                // valid W3C traceparent, parent this span on the
                // producer's remote span context via the OTel bridge so
                // logs stitch across the wire in any OpenTelemetry-
                // aware backend. When no traceparent arrived (or no
                // OTel SDK is configured at runtime), the parent stays
                // empty and the span becomes a local root.
                //
                // `traceparent` is recorded as a span field even when
                // the OTel parent set is a no-op, so plain `tracing`
                // log consumers can still see the correlation id.
                let event_span = tracing::info_span!(
                    "ingest_event",
                    producer = %producer_id,
                    peer = %peer_addr,
                    traceparent = incoming.traceparent.as_deref().unwrap_or("none"),
                );
                if let Some(tp) = incoming.traceparent.as_deref() {
                    apply_traceparent(&event_span, tp);
                }
                let producer_id_for_async = producer_id.clone();
                let peer_addr_for_async = peer_addr.clone();
                let pipeline_for_async = Arc::clone(&pipeline);
                let result = async move {
                    pipeline_for_async
                        .append_event(&producer_id_for_async, incoming.event)
                        .await
                        .map_err(|e| (peer_addr_for_async, e))
                }
                .instrument(event_span)
                .await;
                if let Err((peer, e)) = result {
                    error!(peer = %peer, error = %e, "storage error");
                    break;
                }
            }
            Ok(None) => {
                debug!(peer = %peer_addr, "session ended cleanly");
                break;
            }
            Err(e) => {
                error!(peer = %peer_addr, error = %e, "session error");
                break;
            }
        }
    }

    info!(peer = %peer_addr, "connection closed");
}
