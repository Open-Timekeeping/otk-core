//! Open Timekeeping conformance suite.
//!
//! Verifies whether an implementation satisfies the contracts in [`spec/`].
//! Tests are organised by contract; each contract module is independently
//! consumable so an implementer can run only the suites that apply.
//!
//! # Current test surface
//!
//! Seven test files in `tests/`, covering the v0 contract surface:
//!
//! | File | What it covers |
//! |---|---|
//! | `event_model_roundtrip.rs` | CBOR encode/decode for every `OtkEvent` variant. |
//! | `wire_protocol_handshake.rs` | `Connect` / `ConnectAck` / `ConnectReject` envelope shapes and version negotiation. |
//! | `event_log_contract.rs` | `EventLog::append` / `read_range` / `latest_offset` / `earliest_offset` / `subscribe` and dyn-safety. |
//! | `event_log_retention.rs` | `RetentionExpired` semantics for `read_range` and `subscribe`, fully-evicted-log behaviour, earliest_offset advancement. |
//! | `frame_codec_contract.rs` | Stream + serial frame round-trip, oversize + resync, CRC mismatch, partial-frame buffering, extra-zero delimiter handling. |
//! | `ingest_protocol_contract.rs` | Handshake accept/reject, processor `InboundAction` dispatch, source-spoofing rejection, auth allow-list. |
//! | `contracts_dyn_safety.rs` | `DetectorAdapter` and `Timebase` dyn-safety + first-event-is-Metadata invariant. |
//!
//! The suite is anchored on an in-crate [`mem_log::MemLog`] reference
//! `EventLog` implementation with explicit `evict_below` / `evict_all`
//! helpers, so retention paths are deterministic without a real on-disk
//! backend. Real backends (`adapter-event-log-segment`) honour the same
//! contract via segment deletion.
//!
//! # Out of scope today
//!
//! - **Runtime-node ingest end-to-end** (multi-listener parity over real
//!   TCP + Unix sockets concurrently, producer-side resume after reconnect,
//!   consumer-side `retention_expired` over the live API). Lands once the
//!   conformance crate gains a fixture-driven driver that can stand up a
//!   `timing-node` instance.
//! - **Hardware-in-the-loop**: how the suite drives physical detectors is
//!   tracked in [`open-questions.md`].
//! - **Vendor-specific test packs** (MYLAPS, RaceResult, etc.). Out of
//!   scope at the v0 contract level.
//!
//! [`spec/`]: https://github.com/Open-Timekeeping/open-timekeeping/tree/main/spec
//! [`open-questions.md`]: https://github.com/Open-Timekeeping/open-timekeeping/blob/main/spec/open-questions.md

pub mod mem_log;
