/// Consumer client for reading events from a timing node.
///
/// Phase 2: HTTP/SSE methods for `GET /api/v1/events` and
/// `GET /api/v1/events/stream` will be added here once the timing-node
/// REST API is implemented.
pub struct OtkClient {
    base_url: String,
}

impl OtkClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}
