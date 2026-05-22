# conformance

Compatibility and conformance tests for Open Timekeeping.

> **Status: active.** v0 contract surface covered: 47 tests across seven categories. See [`src/lib.rs`](./src/lib.rs) for the canonical breakdown.

## What this is

The conformance suite that verifies whether a detector adapter, simulator feed, native firmware, producer client, timing-node plugin, timebase adapter, storage backend, or third-party implementation satisfies the Open Timekeeping contracts described in [`spec`](https://github.com/Open-Timekeeping/spec).

If you claim Open Timekeeping compatibility, you should be able to point at a passing run of this suite.

## What's covered today

| Test file | What it covers |
|---|---|
| `event_model_roundtrip.rs` | CBOR encode/decode for every `OtkEvent` variant. |
| `wire_protocol_handshake.rs` | `Connect` / `ConnectAck` / `ConnectReject` envelope shapes and version negotiation. |
| `event_log_contract.rs` | `EventLog::append` / `read_range` / `latest_offset` / `earliest_offset` / `subscribe` and dyn-safety. |
| `event_log_retention.rs` | `RetentionExpired` semantics for `read_range` and `subscribe`, fully-evicted-log behaviour, in-flight subscription eviction, earliest_offset advancement, boundary-clamp. |
| `frame_codec_contract.rs` | Stream + serial frame round-trip, oversize + resync, CRC mismatch, partial-frame buffering, extra-zero delimiter handling. |
| `ingest_protocol_contract.rs` | Handshake accept/reject, processor `InboundAction` dispatch, source-spoofing rejection, auth allow-list. |
| `contracts_dyn_safety.rs` | `DetectorAdapter` and `Timebase` dyn-safety plus first-event-is-Metadata invariant. |

The suite is anchored on an in-crate [`MemLog`](./src/mem_log.rs) reference `EventLog` implementation with explicit `evict_below` / `evict_all` helpers, so retention paths are deterministic without a real on-disk backend. Real backends (`adapter-event-log-segment`) honour the same contract via segment deletion.

## Out of scope today

- **Runtime-node ingest end-to-end** (multi-listener parity over real TCP + Unix sockets concurrently, producer-side resume after reconnect, consumer-side `retention_expired` over the live API). Lands once the conformance crate gains a fixture-driven driver that can stand up a `timing-node` instance.
- **Hardware-in-the-loop.** How the suite drives physical detectors is tracked in [`open-questions.md`](https://github.com/Open-Timekeeping/spec/blob/main/open-questions.md).
- **Vendor-specific test packs** (MYLAPS, RaceResult, etc.). Out of scope at the v0 contract level.

## What does not belong here

- Sample event streams and bad/edge-case streams. Future home: [`conformance-fixtures`](https://github.com/Open-Timekeeping/conformance-fixtures).
- Implementations of any contract. This crate is the suite, not the device under test.

## Dependencies

**Depends on:** [`spec`](https://github.com/Open-Timekeeping/spec) (normative), and the [`otk-core`](https://github.com/Open-Timekeeping/otk-core) member crates `event-model`, `otk-protocol`, `port-out-event-log`, `frame-codec`, `ingest-protocol`, `otk-contracts`. By design, this crate does **not** depend on any concrete adapter implementation (`adapter-ingest-tcp`, `adapter-event-log-segment`, etc.) so adding or removing adapters can't perturb the harness.

**Commonly depended on by:** every adapter, every timebase, every storage backend, [`timing-node`](https://github.com/Open-Timekeeping/timing-node), reference firmware, third-party implementations.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
