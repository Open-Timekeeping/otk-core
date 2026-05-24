//! Bridge from an incoming W3C `traceparent` string to an OpenTelemetry
//! remote span context applied to a [`tracing::Span`].
//!
//! Producers send their current trace identity in
//! [`otk_protocol::OtkEnvelope::traceparent`]. The ingest layer
//! validates the format ([`otk_protocol::is_valid_traceparent`]) and
//! drops malformed values silently before they reach this module. By
//! the time [`apply_traceparent`] is called the string is well-formed,
//! but we still parse defensively and treat parse failure as "skip"
//! rather than panic, in case a future revision of the validation
//! contract relaxes some constraint.
//!
//! When no OpenTelemetry SDK is installed at runtime, `set_parent` is
//! effectively a no-op (the span's OTel extensions store the context
//! but no exporter ships it anywhere). That is deliberate: an operator
//! who has not opted into OTel does not need to install one for the
//! runtime to accept traceparent-carrying events.

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Parse a W3C `traceparent` string and set the given span's OTel
/// parent context to a remote span derived from it.
///
/// Returns silently on any parse failure, on an all-zero trace_id /
/// span_id (the W3C spec forbids both, the validator should have
/// already rejected, but a future-version traceparent may carry
/// different semantics so we re-check), or on a flags byte that
/// doesn't parse as hex.
pub fn apply_traceparent(span: &tracing::Span, traceparent: &str) {
    let Ok(parts) = otk_protocol::parse_traceparent(traceparent) else {
        return;
    };

    let Some(trace_id_bytes) = hex_to_array::<16>(parts.trace_id) else {
        return;
    };
    let Some(span_id_bytes) = hex_to_array::<8>(parts.parent_id) else {
        return;
    };
    let Ok(flags_byte) = u8::from_str_radix(parts.trace_flags, 16) else {
        return;
    };

    let trace_id = TraceId::from_bytes(trace_id_bytes);
    let span_id = SpanId::from_bytes(span_id_bytes);
    if trace_id == TraceId::INVALID || span_id == SpanId::INVALID {
        return;
    }
    let flags = TraceFlags::new(flags_byte);

    let span_ctx = SpanContext::new(
        trace_id,
        span_id,
        flags,
        true, // is_remote: this context was reconstructed from a wire value.
        TraceState::default(),
    );

    // `with_remote_span_context` on a fresh Context attaches the parent
    // span info; `Span::set_parent` then stores it on the span via the
    // `tracing-opentelemetry` extensions, where the OTel layer (if
    // installed) will pick it up at export time. The `Result` returned
    // by `set_parent` reports whether the parent set actually landed on
    // a registered subscriber: ignored deliberately because the
    // runtime-without-OTel case is a no-op by design (this is not a
    // condition the operator can act on).
    let cx = opentelemetry::Context::new().with_remote_span_context(span_ctx);
    let _ = span.set_parent(cx);
}

/// Decode an even-length lowercase-hex string into a fixed-size byte
/// array. Returns `None` on any non-hex character or length mismatch
/// against the expected output size.
fn hex_to_array<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != 2 * N {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte_chunk) in s.as_bytes().chunks_exact(2).enumerate() {
        let hi = hex_nibble(byte_chunk[0])?;
        let lo = hex_nibble(byte_chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn apply_to_span_does_not_panic_with_valid_input() {
        // We cannot easily inspect the resulting span context without a
        // full OTel SDK, but the call must complete without panicking
        // even when no OTel subscriber is installed.
        let span = tracing::info_span!("test");
        apply_traceparent(&span, VALID);
    }

    #[test]
    fn apply_to_span_does_not_panic_with_invalid_input() {
        // Malformed strings reach this layer only when validation
        // contracts drift; the function must still be safe to call.
        let span = tracing::info_span!("test");
        apply_traceparent(&span, "not-a-traceparent");
        apply_traceparent(&span, "");
        apply_traceparent(
            &span,
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
        );
    }

    #[test]
    fn hex_to_array_decodes_known_value() {
        let bytes: [u8; 4] = hex_to_array("deadbeef").unwrap();
        assert_eq!(bytes, [0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn hex_to_array_rejects_wrong_length() {
        assert_eq!(hex_to_array::<4>("dead"), None);
        assert_eq!(hex_to_array::<4>("deadbe"), None);
        assert_eq!(hex_to_array::<4>("deadbeef00"), None);
    }

    #[test]
    fn hex_to_array_rejects_non_hex() {
        assert_eq!(hex_to_array::<4>("deadbeeg"), None);
        // Uppercase is also rejected; the upstream validator ensures
        // lowercase, but defensive parsing here is consistent with that.
        assert_eq!(hex_to_array::<4>("DEADBEEF"), None);
    }

    #[test]
    fn parse_round_trip_matches_w3c_ids() {
        // The example trace_id from the W3C spec, decoded both via
        // hex_to_array and verified against the documented bytes.
        let bytes: [u8; 16] = hex_to_array("0af7651916cd43dd8448eb211c80319c").unwrap();
        let expected = [
            0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80,
            0x31, 0x9c,
        ];
        assert_eq!(bytes, expected);
    }

    /// End-to-end: with a `tracing-opentelemetry` layer installed,
    /// `apply_traceparent` must result in `span.context()` returning
    /// an OTel context whose remote parent has the exact trace_id and
    /// span_id encoded in the W3C string.
    ///
    /// Without an OTel layer this test couldn't observe the parent;
    /// with one, the bridge stores the remote span context on the
    /// span's extensions and `OpenTelemetrySpanExt::context()` walks
    /// up to it.
    #[test]
    fn apply_parent_is_observable_through_otel_layer() {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::Registry;

        let subscriber = Registry::default().with(tracing_opentelemetry::layer());
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("under_test");
            apply_traceparent(&span, VALID);

            let cx = span.context();
            let span_ref = cx.span();
            let span_ctx = span_ref.span_context();

            assert!(
                span_ctx.is_valid(),
                "applied parent must be a valid span context"
            );

            // 0af7651916cd43dd8448eb211c80319c
            let expected_trace_id = TraceId::from_bytes([
                0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80,
                0x31, 0x9c,
            ]);
            assert_eq!(span_ctx.trace_id(), expected_trace_id);

            // b7ad6b7169203331
            let expected_span_id =
                SpanId::from_bytes([0xb7, 0xad, 0x6b, 0x71, 0x69, 0x20, 0x33, 0x31]);
            assert_eq!(span_ctx.span_id(), expected_span_id);

            // flags = 01 → sampled.
            assert!(span_ctx.trace_flags().is_sampled());
        });
    }
}
