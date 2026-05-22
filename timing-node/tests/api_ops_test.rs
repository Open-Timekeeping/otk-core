//! Integration tests for the operational surface added in M7:
//! `/healthz`, `/readyz`, `/metrics`, and the API bearer-token middleware.

use reqwest::Client;
use timing_node::{ApiConfig, AuthConfig, ListenerConfig, Node, NodeConfig};

/// HTTP client used for every integration assertion in this file.
/// Carries a request timeout so a node that fails to bind or fails to
/// respond can't hang the test indefinitely; `cargo test`'s parallel
/// runner would otherwise hold the whole suite open until the OS
/// killed the process. 5 s is well beyond any legitimate localhost
/// response time but short enough that a failing test fails
/// deterministically.
fn timeout_client() -> Client {
    Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("reqwest Client builds with timeout")
}

/// Signal shutdown and drain the spawned tasks with a timeout so background
/// workers don't outlive a test under cargo's parallel runner. Asserts on
/// both the timeout AND the join result so a panic in a background task
/// fails the test instead of being silently dropped on the floor. Matches
/// the strict pattern used in tests/ingest_test.rs.
async fn shutdown_and_drain(
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    ingest_tasks: Vec<tokio::task::JoinHandle<()>>,
    api_task: tokio::task::JoinHandle<()>,
) {
    let _ = shutdown_tx.send(true);
    for t in ingest_tasks {
        tokio::time::timeout(tokio::time::Duration::from_secs(5), t)
            .await
            .expect("listener shutdown timed out")
            .expect("listener task panicked");
    }
    tokio::time::timeout(tokio::time::Duration::from_secs(5), api_task)
        .await
        .expect("api shutdown timed out")
        .expect("api task panicked");
}

async fn node_with(auth: AuthConfig, api: ApiConfig) -> (Node, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let config = NodeConfig {
        node_id: "test-node".into(),
        listeners: vec![ListenerConfig::Tcp {
            id: "tcp-main".into(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_frame_bytes: 65_535,
        }],
        api_addr: "127.0.0.1:0".parse().unwrap(),
        storage_dir: tmp.path().to_path_buf(),
        auth,
        api,
    };
    let node = Node::new(config).await.unwrap();
    (node, tmp)
}

#[tokio::test]
async fn healthz_and_readyz_return_ok_unauthenticated() {
    let (node, _tmp) = node_with(AuthConfig::default(), ApiConfig::default()).await;
    let api_addr = node.api_addr();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (ingest, api) = node.run_with_shutdown(shutdown_rx);

    let client = timeout_client();
    let healthz = client
        .get(format!("http://{api_addr}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(healthz.status(), reqwest::StatusCode::OK);

    let readyz = client
        .get(format!("http://{api_addr}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(readyz.status(), reqwest::StatusCode::OK);

    shutdown_and_drain(shutdown_tx, ingest, api).await;
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_text() {
    let (node, _tmp) = node_with(AuthConfig::default(), ApiConfig::default()).await;
    let api_addr = node.api_addr();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (ingest, api) = node.run_with_shutdown(shutdown_rx);

    let body = timeout_client()
        .get(format!("http://{api_addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(body.contains("# TYPE otk_events_appended_total counter"));
    assert!(body.contains("# TYPE otk_ingest_sessions_active gauge"));
    assert!(body.contains("otk_events_appended_total 0"));

    shutdown_and_drain(shutdown_tx, ingest, api).await;
}

#[tokio::test]
async fn api_requires_bearer_token_when_configured() {
    let auth = AuthConfig {
        producer_tokens: vec![],
        api_tokens: vec!["super-secret".into()],
    };
    let (node, _tmp) = node_with(auth, ApiConfig::default()).await;
    let api_addr = node.api_addr();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (ingest, api) = node.run_with_shutdown(shutdown_rx);

    let client = timeout_client();

    // No header → 401.
    let no_token = client
        .get(format!("http://{api_addr}/api/v1/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(no_token.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Wrong token → 401.
    let wrong = client
        .get(format!("http://{api_addr}/api/v1/status"))
        .bearer_auth("nope")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Correct token → 200.
    let ok = client
        .get(format!("http://{api_addr}/api/v1/status"))
        .bearer_auth("super-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), reqwest::StatusCode::OK);

    // /healthz and /metrics are unauthenticated regardless.
    let public = client
        .get(format!("http://{api_addr}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(public.status(), reqwest::StatusCode::OK);

    shutdown_and_drain(shutdown_tx, ingest, api).await;
}
