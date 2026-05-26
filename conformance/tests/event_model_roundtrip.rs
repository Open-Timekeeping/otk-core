//! Event Model conformance: CBOR encode/decode round-trips.
//!
//! Every [`OtkEvent`] variant must survive a `minicbor::to_vec` →
//! `minicbor::decode` round-trip with byte-stable re-encoding. The Wire Protocol
//! layer wraps these values in envelopes and ships them across every supported
//! transport binding; if a variant doesn't round-trip cleanly, every transport
//! that carries it is broken.

use conformance_fixtures::events::canon;
use event_model::OtkEvent;

fn roundtrip(event: &OtkEvent) {
    let bytes = minicbor::to_vec(event).expect("encode");
    let decoded: OtkEvent = minicbor::decode(&bytes).expect("decode");
    let re_encoded = minicbor::to_vec(&decoded).expect("re-encode");
    assert_eq!(bytes, re_encoded, "CBOR is not stable across re-encode");
}

#[test]
fn detection_loop_transponder_roundtrip() {
    roundtrip(&canon::detection_loop_transponder());
}

#[test]
fn detection_beam_break_roundtrip() {
    roundtrip(&canon::detection_beam_break());
}

#[test]
fn detector_health_healthy_roundtrip() {
    roundtrip(&canon::detector_health_healthy());
}

#[test]
fn detector_health_degraded_roundtrip() {
    roundtrip(&canon::detector_health_degraded());
}

#[test]
fn timebase_status_roundtrip_every_sync_state() {
    // Drives `canon::timebase_status_all_states()` so adding a new
    // SyncState variant to `event-model` (and to the canon list)
    // automatically widens this test's coverage.
    for event in canon::timebase_status_all_states() {
        roundtrip(&event);
    }
}

#[test]
fn adapter_metadata_roundtrip() {
    roundtrip(&canon::adapter_metadata());
}

#[test]
fn crossing_roundtrip() {
    roundtrip(&canon::crossing());
}

#[test]
fn one_of_each_variant_roundtrip() {
    // Belt-and-braces: iterate the canonical "one of each" set so a
    // new variant added to `canon::one_of_each_variant()` gets a
    // round-trip check for free, even if no per-variant `#[test]`
    // is added here.
    for event in canon::one_of_each_variant() {
        roundtrip(&event);
    }
}
