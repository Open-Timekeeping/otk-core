//! Authoriser implementations for producer Connect handshakes and the
//! atomic-swap auth state that backs hot-reload.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ingest_protocol::ConnectAuthoriser;
use otk_protocol::{ids::ProducerId, ConnectRejectReason};

/// Shared, atomically swappable auth state.
///
/// Both producer-side and API-side token allow-lists live here behind
/// independent [`ArcSwap`]s. Reads (producer handshake, API request)
/// take a cheap atomic snapshot; reloads (config-file watcher,
/// SIGHUP) replace the snapshot wholesale. Active sessions hold no
/// reference to the old list past the snapshot they took for their
/// own admission decision, so a swap doesn't drop connections.
///
/// An empty list is the "no auth required" mode: producer side
/// accepts every Connect, API side accepts every request. The default
/// at startup if the operator hasn't set tokens.
pub struct AuthState {
    producer_tokens: ArcSwap<Vec<String>>,
    api_tokens: ArcSwap<Vec<String>>,
}

impl AuthState {
    pub fn new(producer_tokens: Vec<String>, api_tokens: Vec<String>) -> Self {
        Self {
            producer_tokens: ArcSwap::from_pointee(producer_tokens),
            api_tokens: ArcSwap::from_pointee(api_tokens),
        }
    }

    /// Cheap snapshot for read-side callers (per-request, per-handshake).
    /// The returned `Arc` is a snapshot; concurrent rotations don't
    /// invalidate it.
    pub fn current_producer_tokens(&self) -> Arc<Vec<String>> {
        self.producer_tokens.load_full()
    }

    pub fn current_api_tokens(&self) -> Arc<Vec<String>> {
        self.api_tokens.load_full()
    }

    /// Rotate the producer-side allow-list. Returns the previous list
    /// so the caller can decide whether to log a diff.
    pub fn set_producer_tokens(&self, new_tokens: Vec<String>) -> Arc<Vec<String>> {
        self.producer_tokens.swap(Arc::new(new_tokens))
    }

    /// Rotate the API-side allow-list. Returns the previous list.
    pub fn set_api_tokens(&self, new_tokens: Vec<String>) -> Arc<Vec<String>> {
        self.api_tokens.swap(Arc::new(new_tokens))
    }
}

/// `ConnectAuthoriser` that reads the producer-side allow-list from a
/// shared [`AuthState`] on every call. Empty list = accept; non-empty
/// = token must match. Equivalent to the previous `AllowAll`
/// / `TokenAuthoriser` split, collapsed into one type so hot-reload
/// can move between the two modes by swapping the list.
pub struct SwappableAuthoriser {
    state: Arc<AuthState>,
}

impl SwappableAuthoriser {
    pub fn new(state: Arc<AuthState>) -> Self {
        Self { state }
    }
}

impl ConnectAuthoriser for SwappableAuthoriser {
    fn authorise(
        &self,
        _producer_id: &ProducerId,
        token: Option<&str>,
    ) -> Result<(), ConnectRejectReason> {
        let allowed = self.state.current_producer_tokens();
        if allowed.is_empty() {
            // Empty list = accept (development / unauthenticated mode).
            return Ok(());
        }
        match token {
            Some(t) if allowed.iter().any(|allowed| allowed == t) => Ok(()),
            _ => Err(ConnectRejectReason::Unauthorized),
        }
    }
}

/// Build the runtime's producer-side authoriser around shared
/// [`AuthState`]. The returned `Arc<dyn ConnectAuthoriser>` is what
/// each `adapter-ingest-*` listener consumes; reloads happen via the
/// underlying `AuthState` and are visible on the next `authorise`
/// call without re-binding the listener.
pub fn build_producer_authoriser(state: Arc<AuthState>) -> Arc<dyn ConnectAuthoriser> {
    Arc::new(SwappableAuthoriser::new(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(producer: &[&str]) -> Arc<AuthState> {
        Arc::new(AuthState::new(
            producer.iter().map(|s| s.to_string()).collect(),
            vec![],
        ))
    }

    #[test]
    fn empty_producer_list_yields_accept_for_any_token() {
        let auth = build_producer_authoriser(state(&[]));
        assert!(auth.authorise(&ProducerId::from("p"), None).is_ok());
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("anything"))
            .is_ok());
    }

    #[test]
    fn populated_list_rejects_missing_and_wrong() {
        let auth = build_producer_authoriser(state(&["secret"]));
        assert!(auth.authorise(&ProducerId::from("p"), None).is_err());
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("wrong"))
            .is_err());
    }

    #[test]
    fn populated_list_accepts_listed_token() {
        let auth = build_producer_authoriser(state(&["secret", "other"]));
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("secret"))
            .is_ok());
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("other"))
            .is_ok());
    }

    #[test]
    fn swapping_producer_tokens_takes_effect_on_next_call() {
        let s = state(&["old"]);
        let auth = build_producer_authoriser(Arc::clone(&s));
        assert!(auth.authorise(&ProducerId::from("p"), Some("old")).is_ok());

        // Rotate.
        s.set_producer_tokens(vec!["new".into()]);

        // Old token rejected, new token accepted, on the SAME
        // authoriser instance.
        assert!(auth.authorise(&ProducerId::from("p"), Some("old")).is_err());
        assert!(auth.authorise(&ProducerId::from("p"), Some("new")).is_ok());
    }

    #[test]
    fn swapping_to_empty_list_returns_to_accept_mode() {
        let s = state(&["secret"]);
        let auth = build_producer_authoriser(Arc::clone(&s));
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("wrong"))
            .is_err());
        s.set_producer_tokens(vec![]);
        // Now accept-mode: any token (including None) is OK.
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("wrong"))
            .is_ok());
        assert!(auth.authorise(&ProducerId::from("p"), None).is_ok());
    }

    #[test]
    fn api_token_swap_visible_via_current_api_tokens() {
        let s = Arc::new(AuthState::new(vec![], vec!["api-old".into()]));
        assert_eq!(s.current_api_tokens().as_slice(), &["api-old".to_string()]);
        s.set_api_tokens(vec!["api-new".into(), "api-also".into()]);
        let current = s.current_api_tokens();
        assert_eq!(
            current.as_slice(),
            &["api-new".to_string(), "api-also".to_string()]
        );
    }
}
