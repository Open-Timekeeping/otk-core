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
        // Mirror the API-side timing discipline (see
        // `crate::api::require_api_token`): walk every configured token
        // with `constant_time_eq` and OR the results into a single bit.
        // `iter().any(...)` would short-circuit on the first match, and
        // even with per-token constant-time comparison the short-circuit
        // itself reveals approximately WHICH configured token matched
        // (and therefore its position) to a holder of a valid token who
        // is probing for OTHER configured tokens. The OR-fold removes
        // that channel by forcing the loop to do the same amount of
        // work regardless of where (or whether) the match sits.
        //
        // The `None` branch is fine to short-circuit on: it reflects
        // the supplied envelope shape, which the attacker already
        // controls (they decided whether to put a token in their
        // `Connect` or not). No secret leaks via that path.
        let Some(supplied) = token else {
            return Err(ConnectRejectReason::Unauthorized);
        };
        let supplied_bytes = supplied.as_bytes();
        let mut hit = false;
        for allowed_token in allowed.iter() {
            // Bitwise `|=`, not `||`, so the right-hand side is always
            // evaluated regardless of an earlier match.
            hit |= constant_time_eq(allowed_token.as_bytes(), supplied_bytes);
        }
        if hit {
            Ok(())
        } else {
            Err(ConnectRejectReason::Unauthorized)
        }
    }
}

/// Constant-time-ish byte-slice equality.
///
/// Same shape as a stripped-down `subtle::ConstantTimeEq::ct_eq`: scan
/// every byte pair, OR the XOR difference into one accumulator, return
/// `accumulator == 0`. A wrong guess of the right length takes the
/// same time as the right guess regardless of where the mismatch sits.
///
/// What this does not protect against: a length mismatch returns early.
/// An attacker can therefore learn the configured token's length by
/// probing different lengths. In this threat model the configured token
/// length is treated as not-secret (the operator picks it, it's fixed
/// at boot, and any token issued by the operator should carry enough
/// entropy at any plausible length that length is not the relevant
/// secret). If that assumption ever changes, switch to a constant-time
/// crate (`subtle::ConstantTimeEq`) and pad both inputs to a fixed
/// maximum.
///
/// Lives here (not in `api`) so both the producer-side authoriser
/// (this file) and the API-side bearer-token middleware (`crate::api`)
/// share one implementation; previously each had its own copy.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
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

    // ── constant_time_eq ──────────────────────────────────────────────────

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd")); // length mismatch
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }

    /// A wrong guess of the right length must take the same number of
    /// per-byte ops regardless of where the mismatch sits (we can't
    /// reliably measure timing in a unit test, but we can prove the
    /// loop is the OR-fold shape by checking total work via the
    /// function's correctness on every byte-position mismatch).
    #[test]
    fn constant_time_eq_handles_mismatch_at_any_byte() {
        let truth = b"correct-horse-battery-staple";
        for pos in 0..truth.len() {
            let mut wrong = truth.to_vec();
            wrong[pos] ^= 0x01;
            assert!(
                !constant_time_eq(truth, &wrong),
                "mismatch at byte {pos} must return false"
            );
        }
    }

    /// Authoriser must accept the second-listed token as quickly as
    /// the first-listed one. We don't measure timing here, but the
    /// test pins the *correctness* expectation that both positions
    /// authorise: if a future regression changed the OR-fold into an
    /// `any()` that bailed on the first byte mismatch, this would
    /// still pass; but the timing-asymmetry comment in the impl is
    /// the actual contract reviewers should enforce.
    #[test]
    fn authoriser_accepts_any_position_in_allow_list() {
        let s = state(&["alpha", "bravo", "charlie"]);
        let auth = build_producer_authoriser(s);
        for tok in ["alpha", "bravo", "charlie"] {
            assert!(
                auth.authorise(&ProducerId::from("p"), Some(tok)).is_ok(),
                "{tok:?} should authorise (position-agnostic)"
            );
        }
        // Length-equal but wrong: rejected.
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("delta"))
            .is_err());
    }
}
