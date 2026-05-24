//! W3C Trace Context `traceparent` header value: parsing and validation.
//!
//! The [`traceparent`](https://www.w3.org/TR/trace-context/#traceparent-header)
//! field carries distributed-trace identity across the OTK wire. This module
//! provides a lightweight, `no_std`-compatible validator and accessor for the
//! string form. The actual tracing-system integration (extracting from a
//! producer's current span, parenting a server-side span on a received value)
//! lives in `otk-sdk` and `timing-node` respectively, behind a
//! `tracing-opentelemetry` dependency that this crate does not pull in.
//!
//! # Format
//!
//! ```text
//! version "-" trace-id "-" parent-id "-" trace-flags
//! ```
//!
//! - `version`: 2 lowercase hex chars (the current spec version is `"00"`;
//!   higher versions are tolerated per the spec's forward-compatibility rule).
//! - `trace-id`: 32 lowercase hex chars; must not be all-zero.
//! - `parent-id` (a.k.a. span id): 16 lowercase hex chars; must not be all-zero.
//! - `trace-flags`: 2 lowercase hex chars.
//!
//! Total length is always 55 chars.

use alloc::string::String;

/// Errors returned by [`parse_traceparent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceParentError {
    /// Total length is not 55 chars.
    InvalidLength { got: usize },
    /// One of the four hyphens is missing or in the wrong place.
    InvalidStructure,
    /// One of the four fields contains a non-`[0-9a-f]` character. Uppercase
    /// hex is rejected per the W3C spec, which mandates lowercase.
    InvalidHex,
    /// `version` is `"ff"`, which the W3C spec reserves as invalid.
    ForbiddenVersion,
    /// `trace-id` is all zeros.
    InvalidTraceId,
    /// `parent-id` (span id) is all zeros.
    InvalidParentId,
}

/// The four logical segments of a parsed `traceparent`, borrowed from the
/// input string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceParentParts<'a> {
    pub version: &'a str,
    pub trace_id: &'a str,
    pub parent_id: &'a str,
    pub trace_flags: &'a str,
}

/// Parse a `traceparent` string into its four segments, enforcing the W3C
/// validity rules. Borrows from the input.
pub fn parse_traceparent(s: &str) -> Result<TraceParentParts<'_>, TraceParentError> {
    // Length first, before any indexing.
    if s.len() != 55 {
        return Err(TraceParentError::InvalidLength { got: s.len() });
    }

    // Structural: hyphens must be at exactly positions 2, 35, 52.
    let bytes = s.as_bytes();
    if bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
        return Err(TraceParentError::InvalidStructure);
    }

    let version = &s[0..2];
    let trace_id = &s[3..35];
    let parent_id = &s[36..52];
    let trace_flags = &s[53..55];

    if !is_lowercase_hex(version)
        || !is_lowercase_hex(trace_id)
        || !is_lowercase_hex(parent_id)
        || !is_lowercase_hex(trace_flags)
    {
        return Err(TraceParentError::InvalidHex);
    }

    // W3C: version "ff" is reserved as invalid. The spec advises forward-
    // compat: any other unknown version SHOULD be parsed, but the parent_id
    // semantics may differ. We accept; downstream tracing layers can choose
    // whether to use it.
    if version == "ff" {
        return Err(TraceParentError::ForbiddenVersion);
    }

    if is_all_zero(trace_id) {
        return Err(TraceParentError::InvalidTraceId);
    }
    if is_all_zero(parent_id) {
        return Err(TraceParentError::InvalidParentId);
    }

    Ok(TraceParentParts {
        version,
        trace_id,
        parent_id,
        trace_flags,
    })
}

/// Convenience: validate without keeping the parts.
pub fn is_valid_traceparent(s: &str) -> bool {
    parse_traceparent(s).is_ok()
}

/// Build a `traceparent` string from raw parts. `trace_flags` is the byte form
/// (e.g. `0x01` for "sampled"); we render it as 2 lowercase hex chars.
/// `version` defaults to `0x00`.
///
/// Returns `None` if `trace_id` is all-zero or `parent_id` is all-zero (the
/// W3C spec forbids both).
pub fn format_traceparent(trace_id: u128, parent_id: u64, trace_flags: u8) -> Option<String> {
    if trace_id == 0 || parent_id == 0 {
        return None;
    }
    // `format!` is provided by alloc; this crate is no_std + alloc.
    Some(alloc::format!(
        "00-{:032x}-{:016x}-{:02x}",
        trace_id,
        parent_id,
        trace_flags
    ))
}

fn is_lowercase_hex(s: &str) -> bool {
    s.bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn is_all_zero(s: &str) -> bool {
    s.bytes().all(|b| b == b'0')
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn valid_traceparent_parses() {
        let parts = parse_traceparent(VALID).unwrap();
        assert_eq!(parts.version, "00");
        assert_eq!(parts.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(parts.parent_id, "b7ad6b7169203331");
        assert_eq!(parts.trace_flags, "01");
        assert!(is_valid_traceparent(VALID));
    }

    #[test]
    fn wrong_length_rejected() {
        assert!(matches!(
            parse_traceparent("00-0af7651916cd43dd-b7ad6b7169203331-01"),
            Err(TraceParentError::InvalidLength { .. })
        ));
        assert!(matches!(
            parse_traceparent(""),
            Err(TraceParentError::InvalidLength { got: 0 })
        ));
    }

    #[test]
    fn wrong_hyphen_positions_rejected() {
        // Right length, wrong structure: drop a hyphen, pad with a hex char.
        let s = "000af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01a";
        assert_eq!(s.len(), 55);
        assert!(matches!(
            parse_traceparent(s),
            Err(TraceParentError::InvalidStructure)
        ));
    }

    #[test]
    fn uppercase_hex_rejected() {
        let s = "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01";
        assert_eq!(s.len(), 55);
        assert!(matches!(
            parse_traceparent(s),
            Err(TraceParentError::InvalidHex)
        ));
    }

    #[test]
    fn non_hex_rejected() {
        let s = "00-0af7651916cd43dd8448eb211c80319g-b7ad6b7169203331-01";
        assert!(matches!(
            parse_traceparent(s),
            Err(TraceParentError::InvalidHex)
        ));
    }

    #[test]
    fn forbidden_version_ff_rejected() {
        let s = "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        assert!(matches!(
            parse_traceparent(s),
            Err(TraceParentError::ForbiddenVersion)
        ));
    }

    #[test]
    fn all_zero_trace_id_rejected() {
        let s = "00-00000000000000000000000000000000-b7ad6b7169203331-01";
        assert!(matches!(
            parse_traceparent(s),
            Err(TraceParentError::InvalidTraceId)
        ));
    }

    #[test]
    fn all_zero_parent_id_rejected() {
        let s = "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01";
        assert!(matches!(
            parse_traceparent(s),
            Err(TraceParentError::InvalidParentId)
        ));
    }

    #[test]
    fn future_version_accepted() {
        // The spec says higher versions SHOULD parse for forward compat.
        let s = "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00";
        assert!(parse_traceparent(s).is_ok());
    }

    #[test]
    fn format_round_trips() {
        let s = format_traceparent(0x0af7651916cd43dd8448eb211c80319c, 0xb7ad6b7169203331, 0x01)
            .unwrap();
        assert_eq!(s, VALID);
        assert!(is_valid_traceparent(&s));
    }

    #[test]
    fn format_rejects_zero_ids() {
        assert!(format_traceparent(0, 0xb7ad6b7169203331, 0).is_none());
        assert!(format_traceparent(0x1234, 0, 0).is_none());
    }
}
