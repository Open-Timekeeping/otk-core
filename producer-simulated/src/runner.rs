use otk_sdk::producer::{adapter_event_to_otk, DetectorAdapter, Producer, ProducerConfig, Transport};
use tracing::{error, info, warn};

/// Run a detector adapter to completion, publishing all events to a timing node.
///
/// Calls `start()`, connects to the node, then loops on `next_event()`, mapping
/// each `AdapterEvent` to an `OtkEvent` and sending it via `Producer`. Disconnects
/// gracefully when the adapter returns `None` or the shutdown channel fires. On
/// error the function stops the adapter and returns the error without disconnecting.
pub async fn run<A: DetectorAdapter>(
    mut adapter: A,
    transport: Transport,
    producer_config: ProducerConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    adapter.start().await?;

    let mut producer = match Producer::connect(transport, producer_config).await {
        Ok(p) => p,
        Err(e) => {
            adapter.stop().await.unwrap_or_else(|e| warn!("stop error: {e}"));
            return Err(e.into());
        }
    };
    info!(
        detector_id = adapter.detector_id().as_str(),
        "connected to timing node"
    );

    loop {
        tokio::select! {
            event = adapter.next_event() => {
                match event {
                    Some(Ok(ev)) => {
                        let otk_event = adapter_event_to_otk(ev);
                        if let Err(e) = producer.send_event(otk_event).await {
                            error!("send error: {e}");
                            adapter.stop().await.unwrap_or_else(|e| warn!("stop error: {e}"));
                            return Err(e.into());
                        }
                    }
                    Some(Err(e)) => {
                        error!("adapter error: {e}");
                        adapter.stop().await.unwrap_or_else(|e| warn!("stop error: {e}"));
                        return Err(e.into());
                    }
                    None => break,
                }
            }
            result = shutdown.changed() => {
                match result {
                    Ok(()) => {
                        if *shutdown.borrow() {
                            info!("shutdown signal received");
                            break;
                        }
                    }
                    Err(_) => {
                        info!("shutdown channel closed, shutting down");
                        break;
                    }
                }
            }
        }
    }

    adapter.stop().await.unwrap_or_else(|e| warn!("stop error: {e}"));
    producer.disconnect().await?;
    info!("disconnected");
    Ok(())
}
