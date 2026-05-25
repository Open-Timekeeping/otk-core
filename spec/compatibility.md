# Compatibility and conformance

What it means to be Open Timekeeping–compatible, and how that's verified.

---

## The claim

An implementation that claims **Open Timekeeping compatibility** must:

1. Satisfy the contracts for every role it plays. Most contract crates live in this workspace; `plugin-api` is listed for completeness but is not yet specified.

   - For a **detector adapter** or **timebase**: the trait in [`otk-contracts`](../otk-contracts) (`DetectorAdapter` / `Timebase`).
   - For a **producer** talking to a runtime node across a process boundary: the OTK Protocol stack ([`event-model`](../event-model), [`otk-protocol`](../otk-protocol), [`frame-codec`](../frame-codec)) over at least one supported transport binding. Producers may use [`otk-sdk`](../otk-sdk)'s `producer` feature, or build directly on the stack.
   - For a **transport-binding ingest adapter**: the [`EventIngestPort`](../timing-core/src/ports/inbound/ingest.rs) trait in `timing_core::ports::inbound`. Server-side framing and handshake are reusable via [`frame-codec`](../frame-codec) + [`ingest-protocol`](../ingest-protocol); concrete adapters consume both.
   - For a **storage backend**: the [`EventLog`](../timing-core/src/ports/outbound/event_log.rs) trait in `timing_core::ports::outbound`.
   - For a **runtime-node plugin**: `plugin-api`. (Not yet specified; see [`open-questions.md`](open-questions.md).)

2. Produce or consume canonical events as defined in [`event-model`](../event-model).

3. Honor the cross-cutting principles in [architecture.md § Operating principles](architecture.md): honest provenance, immutability, mechanism-vs-policy, domain neutrality.

4. Pass the relevant subset of [`conformance`](../conformance) with the canonical [`conformance-fixtures`](../conformance-fixtures).

A claim of compatibility without a passing conformance run is just a claim.

---

## What the suite covers

`conformance` is organized by role. The categories below are the ones each role's suite is expected to verify. Exact tests grow with the contracts.

### Detector adapter

- **Schema validity.** Every emitted event matches its `event-model` schema.
- **Required provenance.** Every event carries timestamping, timebase, observation-quality, and adapter-metadata blocks with no silent omissions.
- **Timestamping method honesty.** The declared method matches what the adapter actually did.
- **Sequence numbers.** Strictly monotonic per detector; never repeated; never reset without registration.
- **Duplicate handling.** When the same physical event is observed twice (by hardware or by retry), the adapter behavior matches the contract (which it must declare).
- **Reconnect / resume.** After a disconnect, the adapter rejoins, registers, and resumes from a known sequence number.
- **Health reporting.** Periodic + state-change health events; minimum cadence; correct degraded-state transitions.
- **Metadata declaration.** Adapter announces identity, capabilities, and supported event types at startup.

### Timebase

- **Status reporting.** Sync state, declared resolution, current uncertainty, drift estimate, holdover state, last-sync time are all populated and update correctly.
- **Honesty.** A timebase in `holdover` or `free-run` says so. An `unsynchronized` timebase is never reported as `locked`.
- **State-change events.** Transitions in sync state produce status events promptly.
- **Uncertainty propagation.** Reported uncertainty corresponds to the actual sync regime.

### OTK Protocol (Event Model + Wire Protocol + Frame Codec + Transport Binding)

The protocol stack is verified layer by layer:

- **Event Model.** Every emitted event matches its `event-model` schema and carries the required provenance blocks.
- **Wire Protocol envelopes.** Versioning, content-type, sender id, sequence numbers, ack/error message types conform to the protocol spec.
- **Frame Codec.** Encode/decode round-trips. Stream framing handles partial reads, message boundaries, and oversize messages. Resynchronizable framing (e.g., COBS, SLIP) recovers cleanly after corruption on byte-stream transports.
- **Transport Binding.** Each binding's listener/client lifecycle, reconnect semantics, and error reporting match the [`EventIngestPort`](../timing-core/src/ports/inbound/ingest.rs) contract in `timing_core::ports::inbound`. Bindings are tested independently of higher layers.
- **Compatibility.** Producer and consumer can negotiate compatible protocol versions across any supported binding; mismatches fail clearly, not silently.

### Storage backend

- **Append correctness.** Order is preserved; offsets are monotonic.
- **Read-range correctness.** A range read returns exactly the records appended in that range.
- **Subscribe semantics.** A live subscribe delivers each appended record exactly once, in order.
- **Retention enforcement.** Time-based, size-based, and hybrid policies behave per the policy spec.
- **`retention_expired` semantics.** Consumers requesting expired offsets get a structured error, not silent gaps.

### Runtime node ingest

- **Handshake.** Producers and plugins can register; capabilities are recorded.
- **Multi-listener parity.** When a runtime node hosts more than one ingest listener (e.g., TCP and USB CDC and Unix socket concurrently), behavior across listeners is identical: the same handshake, the same registry effects, the same error vocabulary, the same canonical ingest pipeline.
- **Error vocabulary.** Schema-invalid input is rejected with a useful, machine-readable error; never silently dropped.
- **Detector / timebase registry consistency.** A registered producer's identity is visible to consumers; deregistration cleans up.

### Durability and resume

Two symmetric resume contracts, on either side of the runtime node. See [architecture.md § Durability and resume](architecture.md) for the canonical statement.

- **Producer-side resume.** An adapter that disconnects and reconnects rejoins, re-registers, and resumes from a known sequence number, with no gaps and no duplicates. The adapter buffers its own output locally for the duration of the outage.
- **Consumer-side resume.** A downstream consumer that disconnects and reconnects can read every event the node accepted during the outage, in order, with no gaps and no duplicates, provided the events are still within the configured retention window. The runtime node, not the consumer's peers, buffers on the consumer's behalf.
- **Retention as the bound on outage tolerance.** Consumers must not be expected to survive outages longer than the configured retention window. Outages longer than retention surface as `retention_expired` (see Storage backend above), never as silent gaps.
- **No cross-layer compensation.** Producers do not buffer on behalf of downstream consumers; consumers do not retry into the producer side. Each layer is responsible for its own boundary.

---

## Where the conformance suite runs

The intent is that conformance runs against:

- An in-process implementation (the suite owns the device under test).
- An external process the suite drives (the suite acts as a client / fixture and the device under test is a long-running binary).
- Recorded outputs (fixture-driven verification of past runs).

How conformance tests connect to *physical* devices for hardware-in-the-loop testing is an open question (a soft-real-time loop with recorded reference timestamps and a programmable signal source is one candidate). See [open-questions.md](open-questions.md).
