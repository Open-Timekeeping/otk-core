//! Authoriser implementations for producer Connect handshakes.

use std::collections::HashSet;
use std::sync::Arc;

use ingest_protocol::{AllowAll, ConnectAuthoriser};
use otk_protocol::{ids::ProducerId, ConnectRejectReason};

/// Authoriser that accepts a `Connect` iff its `auth_token` is in the allow-list.
pub struct TokenAuthoriser {
    allowed: HashSet<String>,
}

impl TokenAuthoriser {
    pub fn new<I, S>(tokens: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            allowed: tokens.into_iter().map(Into::into).collect(),
        }
    }
}

impl ConnectAuthoriser for TokenAuthoriser {
    fn authorise(
        &self,
        _producer_id: &ProducerId,
        token: Option<&str>,
    ) -> Result<(), ConnectRejectReason> {
        match token {
            Some(t) if self.allowed.contains(t) => Ok(()),
            _ => Err(ConnectRejectReason::Unauthorized),
        }
    }
}

/// Build the runtime's producer-side authoriser from config.
///
/// An empty token list yields [`AllowAll`] (development default). A non-empty
/// list yields a [`TokenAuthoriser`].
pub fn build_producer_authoriser(producer_tokens: &[String]) -> Arc<dyn ConnectAuthoriser> {
    if producer_tokens.is_empty() {
        Arc::new(AllowAll)
    } else {
        Arc::new(TokenAuthoriser::new(producer_tokens.iter().cloned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_token_list_yields_allow_all() {
        let auth = build_producer_authoriser(&[]);
        assert!(auth.authorise(&ProducerId::from("p"), None).is_ok());
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("anything"))
            .is_ok());
    }

    #[test]
    fn token_list_rejects_missing_and_wrong() {
        let auth = build_producer_authoriser(&["secret".into()]);
        assert!(auth.authorise(&ProducerId::from("p"), None).is_err());
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("wrong"))
            .is_err());
    }

    #[test]
    fn token_list_accepts_listed_token() {
        let auth = build_producer_authoriser(&["secret".into(), "other".into()]);
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("secret"))
            .is_ok());
        assert!(auth
            .authorise(&ProducerId::from("p"), Some("other"))
            .is_ok());
    }
}
