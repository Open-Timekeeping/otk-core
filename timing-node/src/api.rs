use std::sync::Arc;

use axum::{
    extract::{Query, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::get,
    Json, Router,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};

use crate::metrics::Metrics;
use crate::ports::{EventPage, EventQueryPort, QueryError};

/// State shared with every request handler and middleware.
///
/// `Clone` is cheap: every field is either an `Arc` (`query`, `metrics`,
/// `api_tokens`) or an `Arc<str>` (`node_id`). Axum clones this state for
/// each request and again per middleware layer, so per-clone allocation
/// matters at request rates.
///
/// `allowed_origins` was intentionally removed from this struct: it's only
/// consumed once by `router()` to build the `CorsLayer`, never read per-
/// request, so keeping it here would have cloned a `Vec<String>` per
/// request for nothing.
#[derive(Clone)]
pub struct AppState {
    pub node_id: Arc<str>,
    pub query: Arc<dyn EventQueryPort>,
    pub metrics: Arc<Metrics>,
    /// API bearer tokens; empty = no auth required.
    pub api_tokens: Arc<Vec<String>>,
}

pub fn router(state: AppState, allowed_origins: &[String]) -> Router {
    let cors = build_cors(allowed_origins);

    // Health and metrics are unauthenticated; ops tooling needs them.
    let public = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_endpoint));

    // /api/v1/* sits behind the bearer-token middleware.
    let api = Router::new()
        .route("/api/v1/status", get(status))
        .route("/api/v1/events", get(events))
        .route("/api/v1/events/stream", get(events_stream))
        .route_layer(from_fn_with_state(state.clone(), require_api_token));

    public.merge(api).layer(cors).with_state(state)
}

fn build_cors(allowed_origins: &[String]) -> CorsLayer {
    // Allowed request headers. `/api/v1/*` is auth-gated, so any browser
    // call that needs auth will send `Authorization: Bearer ...`, which
    // is a non-simple header and triggers a CORS preflight. Without
    // `Authorization` on `Access-Control-Allow-Headers`, the browser
    // rejects the preflight and never sends the real request, so the
    // origin allow-list alone wasn't enough. `Content-Type` is included
    // so the same router still works once we add JSON-body endpoints.
    let layer = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]);
    if allowed_origins.iter().any(|o| o == "*") {
        layer.allow_origin(Any)
    } else if allowed_origins.is_empty() {
        // No CORS header emitted; browsers will block cross-origin requests.
        layer
    } else {
        // Parse each origin; log any that fail rather than silently dropping
        // them, otherwise a typo in `[api] allowed_origins` would disable
        // CORS for that origin with no operator-visible signal.
        let mut parsed: Vec<HeaderValue> = Vec::with_capacity(allowed_origins.len());
        for origin in allowed_origins {
            match HeaderValue::from_str(origin) {
                Ok(v) => parsed.push(v),
                Err(e) => tracing::warn!(
                    origin = %origin,
                    error = %e,
                    "ignoring invalid CORS origin in api.allowed_origins"
                ),
            }
        }
        layer.allow_origin(parsed)
    }
}

async fn require_api_token(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if state.api_tokens.is_empty() {
        return next.run(request).await;
    }
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(parse_bearer_token);
    // Walk every configured token regardless of an earlier match.
    // `iter().any(...)` would short-circuit on the first hit, and even
    // though each per-token comparison is constant-time, the
    // short-circuit itself reveals which configured token matched (and
    // therefore approximately its index) via response timing when more
    // than one API token is configured. OR-ing the results with a fold
    // forces the loop to do the same amount of work for any supplied
    // token. The `t.is_some()` guard is fine to short-circuit on: it
    // reflects the *supplied* header shape, which the attacker already
    // controls.
    let token_ok = match supplied {
        None => false,
        Some(t) => {
            let supplied_bytes = t.as_bytes();
            let mut hit = false;
            for allowed in state.api_tokens.iter() {
                // Bitwise OR (not `||`) so the right-hand side is always
                // evaluated, preventing short-circuit timing.
                hit |= constant_time_eq(allowed.as_bytes(), supplied_bytes);
            }
            hit
        }
    };
    if token_ok {
        next.run(request).await
    } else {
        // RFC 6750 §3: a 401 response from a Bearer-protected resource
        // MUST include a WWW-Authenticate header carrying the Bearer
        // challenge so the client (or an intermediary proxy) knows how
        // to authenticate.
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer realm=\"otk-node\"")],
            "missing or invalid bearer token",
        )
            .into_response()
    }
}

/// Parse an `Authorization: Bearer <token>` header value, returning
/// the token if (and only if) the scheme is `Bearer` (case-insensitive).
///
/// Spec considerations:
/// - **Scheme case** (RFC 6750 §2.1): the auth scheme is
///   case-insensitive. We `eq_ignore_ascii_case("bearer")` rather than
///   the prior `strip_prefix("Bearer ")` form, which rejected
///   conforming clients that sent `bearer` / `BEARER`.
/// - **Separator** (RFC 6750 BNF says `1*SP`, RFC 7230 §3.2.3 defines
///   `OWS = *( SP / HTAB )`): clients and intermediaries in the wild
///   do emit `Bearer\t<token>` or `Bearer  <token>`. The plain
///   `split_once(' ')` we used before rejected both, which the
///   caller saw as a 401 with no obvious cause. We now split on either
///   ASCII space or tab and absorb runs of either.
/// - **Surrounding whitespace**: the entire header value is trimmed
///   of `SP`/`HTAB` first so `"  Bearer  abc "` parses the same as
///   `"Bearer abc"`.
///
/// Returns `None` for missing scheme, wrong scheme, or empty token
/// after trimming.
fn parse_bearer_token(raw: &str) -> Option<&str> {
    let is_http_ws = |c: char| c == ' ' || c == '\t';
    let trimmed = raw.trim_matches(is_http_ws);
    let (scheme, rest) = trimmed.split_once(is_http_ws)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = rest.trim_matches(is_http_ws);
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

/// Byte-slice equality that does not short-circuit on byte mismatches.
///
/// What this protects against: plain `==` (and naive memcmp) returns the
/// instant it sees a mismatching byte, leaking the length of the matching
/// prefix one byte at a time. An attacker who can repeatedly probe the
/// endpoint can byte-by-byte recover a short shared secret. This routine
/// XOR-accumulates every byte before returning, so an equal-length wrong
/// guess takes the same time as an equal-length right guess regardless
/// of where the mismatch sits.
///
/// What this does not protect against: a length mismatch returns early.
/// An attacker can therefore learn the configured token's length by
/// probing different lengths. In this threat model the configured token
/// length is treated as not-secret (it is fixed at boot, the operator
/// controls it, and any token issued by the operator should carry enough
/// entropy at any plausible length that length is not the relevant
/// secret). If that assumption ever changes, switch to a constant-time
/// crate (`subtle::ConstantTimeEq`) and pad both inputs to a fixed
/// maximum.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn readyz(State(state): State<AppState>) -> StatusCode {
    // Today: ready iff the query port responds. When the pipeline gains explicit
    // readiness signals (storage open, listeners bound), tighten this check.
    match state.query.latest_offset().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn metrics_endpoint(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}

/// JSON shape returned by `GET /api/v1/status`. Owned `String` rather than
/// a borrow because the response is moved into `axum::Json` for
/// serialisation and we cannot tie its lifetime to the request-scoped
/// `AppState` clone without fighting the extractor's `'static` bound.
/// Status is low-QPS (operator UI / occasional health probe), so the
/// per-call allocation is not on a hot path.
#[derive(Serialize)]
struct StatusResponse {
    node_id: String,
    latest_offset: Option<u64>,
}

async fn status(State(state): State<AppState>) -> Result<Json<StatusResponse>, StatusCode> {
    // A query-port error here means storage is unhealthy. Return 503 so
    // clients can distinguish "node up, no events appended yet" (200 with
    // latest_offset: null) from "node can't talk to storage" (503).
    // Previously the error was swallowed and both cases returned 200.
    match state.query.latest_offset().await {
        Ok(latest_offset) => Ok(Json(StatusResponse {
            node_id: state.node_id.to_string(),
            latest_offset,
        })),
        Err(e) => {
            tracing::warn!(error = %e, "status endpoint: latest_offset failed");
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

/// Maximum events returned by a single `/api/v1/events` page.
///
/// `limit` is clamped to this value server-side; requests exceeding it
/// don't error, they just receive at most this many entries. Sized to
/// keep a single page comfortably under typical reverse-proxy body
/// limits and avoid unbounded memory pressure from a malicious caller.
const MAX_EVENTS_LIMIT: usize = 1000;

#[derive(Deserialize)]
struct PaginateParams {
    #[serde(default)]
    from: u64,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    100
}

async fn events(
    State(state): State<AppState>,
    Query(params): Query<PaginateParams>,
) -> Result<Json<EventPage>, (StatusCode, Json<ErrorBody>)> {
    // Server-side clamp: a malicious or buggy caller passing
    // `?limit=18446744073709551615` would otherwise force a huge
    // allocation and read. Clamping (rather than rejecting with 400) is
    // the friendlier behaviour: legitimate over-fetch just gets the
    // server's maximum page, no error vocabulary to handle.
    let limit = params.limit.min(MAX_EVENTS_LIMIT);
    state
        .query
        .read_events(params.from, limit)
        .await
        .map(Json)
        .map_err(map_query_error)
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    detail: String,
}

/// Map a `QueryError` to an HTTP status + machine-readable JSON body.
///
/// - `RetentionExpired` -> 410 Gone: the requested offset is permanently
///   unreachable (the segment containing it has been evicted). Clients
///   should re-establish their cursor at `earliest_available`.
/// - `NotFound` -> 404 Not Found.
/// - `Internal` -> 503 Service Unavailable (storage is unreachable /
///   misbehaving; the request itself was well-formed). The full backend
///   error is logged server-side; the client receives only a generic
///   string so internal paths, filesystem layout, or library-internal
///   diagnostic text are not exposed to remote callers. Operators get
///   the full message in the journal, scrapers get a stable error code.
///
/// Previously every variant collapsed to 500 Internal Server Error,
/// which lost the retention signal entirely and made transient storage
/// outages look like persistent server bugs. The `Internal` variant
/// also forwarded `StorageError::to_string()` straight into the JSON
/// body, which leaked details like absolute segment paths to anyone
/// who could hit `/api/v1/events` while storage was unhealthy.
fn map_query_error(e: QueryError) -> (StatusCode, Json<ErrorBody>) {
    match e {
        QueryError::RetentionExpired { requested, earliest_available } => (
            StatusCode::GONE,
            Json(ErrorBody {
                error: "retention_expired",
                detail: format!(
                    "offset {requested} has been evicted; earliest_available={earliest_available:?}"
                ),
            }),
        ),
        QueryError::NotFound => (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: "not_found",
                detail: "the requested resource is not present".into(),
            }),
        ),
        QueryError::Internal(msg) => {
            tracing::warn!(error = %msg, "query port internal error");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorBody {
                    error: "internal",
                    detail: "storage backend error; see server logs".into(),
                }),
            )
        }
    }
}

#[derive(Deserialize)]
struct StreamParams {
    #[serde(default)]
    from: u64,
}

async fn events_stream(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
) -> Response {
    match state.query.subscribe_events(params.from).await {
        Ok(event_stream) => {
            // Stream items: each storage error is emitted as a named SSE
            // event (`event: error\ndata: {json}\n\n`) plus a server-side
            // warn log, then the stream ends *immediately* on the next
            // poll without waiting for another upstream item.
            //
            // Previously this used `scan` to track an "errored" flag and
            // return None on the NEXT input, but if the underlying
            // subscription went idle after producing the first error the
            // client would receive the error event yet the connection
            // would stay open indefinitely (waiting for the next upstream
            // poll that would never come). The `unfold` here owns its own
            // state machine: after emitting an error it transitions to
            // `Done` and any subsequent poll immediately returns None,
            // closing the SSE response.
            enum StreamState<S> {
                Streaming(S),
                Done,
            }

            let sse_stream = futures_util::stream::unfold(
                StreamState::Streaming(event_stream),
                |state| async move {
                    let mut source = match state {
                        StreamState::Done => return None,
                        StreamState::Streaming(s) => s,
                    };
                    match source.next().await {
                        None => None,
                        Some(Ok(entry)) => match serde_json::to_string(&entry) {
                            Ok(data) => Some((
                                Ok::<Event, std::convert::Infallible>(Event::default().data(data)),
                                StreamState::Streaming(source),
                            )),
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "events_stream: serializing event entry failed"
                                );
                                // Don't echo the serde error back to the client:
                                // it typically names internal struct fields, which
                                // leaks our event-model schema details. Logged
                                // server-side above is enough for diagnosis.
                                Some((
                                    Ok(error_event(
                                        "serialize_failed",
                                        "internal serialization error; see server logs",
                                    )),
                                    StreamState::Done,
                                ))
                            }
                        },
                        Some(Err(e)) => {
                            tracing::warn!(
                                error = %e,
                                "events_stream: subscription error"
                            );
                            // Same sanitisation rationale as `map_query_error`:
                            // `Internal` carries backend-side detail that may
                            // include filesystem paths or library-internal
                            // strings. Log the full error server-side (above)
                            // and emit a generic detail to the SSE client.
                            let (kind, detail) = match e {
                                QueryError::RetentionExpired {
                                    requested,
                                    earliest_available,
                                } => (
                                    "retention_expired",
                                    format!(
                                        "offset {requested} has been evicted; earliest_available={earliest_available:?}"
                                    ),
                                ),
                                QueryError::NotFound => ("not_found", "not found".into()),
                                QueryError::Internal(_) => (
                                    "internal",
                                    "storage backend error; see server logs".to_string(),
                                ),
                            };
                            Some((Ok(error_event(kind, &detail)), StreamState::Done))
                        }
                    }
                },
            );
            Sse::new(sse_stream).into_response()
        }
        Err(e) => {
            let (status, body) = map_query_error(e);
            (status, body).into_response()
        }
    }
}

fn error_event(kind: &str, detail: &str) -> Event {
    let body = serde_json::json!({ "error": kind, "detail": detail }).to_string();
    Event::default().event("error").data(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bearer_accepts_single_space() {
        assert_eq!(parse_bearer_token("Bearer abc123"), Some("abc123"));
    }

    #[test]
    fn parse_bearer_accepts_case_variants() {
        assert_eq!(parse_bearer_token("bearer abc123"), Some("abc123"));
        assert_eq!(parse_bearer_token("BEARER abc123"), Some("abc123"));
        assert_eq!(parse_bearer_token("BeArEr abc123"), Some("abc123"));
    }

    #[test]
    fn parse_bearer_accepts_tab_separator() {
        // RFC 7230 §3.2.3 OWS = *( SP / HTAB ); some clients/proxies emit a
        // tab between the scheme and the token. The previous `split_once(' ')`
        // form rejected this and 401'd otherwise-valid requests.
        assert_eq!(parse_bearer_token("Bearer\tabc123"), Some("abc123"));
    }

    #[test]
    fn parse_bearer_absorbs_extra_whitespace() {
        // Multiple spaces, mixed SP+HTAB, leading/trailing OWS.
        assert_eq!(parse_bearer_token("Bearer  abc123"), Some("abc123"));
        assert_eq!(parse_bearer_token("Bearer \t abc123"), Some("abc123"));
        assert_eq!(parse_bearer_token("  Bearer abc123  "), Some("abc123"));
        assert_eq!(parse_bearer_token("\tBearer\t\tabc123\t"), Some("abc123"));
    }

    #[test]
    fn parse_bearer_rejects_wrong_scheme() {
        assert_eq!(parse_bearer_token("Basic abc123"), None);
        assert_eq!(parse_bearer_token("Token abc123"), None);
    }

    #[test]
    fn parse_bearer_rejects_no_separator_or_empty_token() {
        assert_eq!(parse_bearer_token("Bearer"), None); // no separator at all
        assert_eq!(parse_bearer_token("Bearer "), None); // empty token after trim
        assert_eq!(parse_bearer_token("Bearer  \t "), None); // only whitespace
        assert_eq!(parse_bearer_token(""), None);
    }

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd")); // length mismatch
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }
}
