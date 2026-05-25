# AGENTS.md, Open Timekeeping

This file orients AI coding assistants (Claude Code, Codex, Cursor, etc.) and humans working in this repository.

It is intentionally thin. The canonical conceptual model lives in [`spec/`](spec/), start with [`spec/architecture.md`](spec/architecture.md). Each crate's own `README.md` is the orientation entry point for working inside that crate.

---

## 1. What this repo is

A single Cargo workspace containing every Rust crate that ships at v0 of Open Timekeeping. Hardware, firmware, frontend apps, and a future TypeScript SDK each live in their own future repos because they are different toolchains; everything that compiles to Rust binaries shipping together lives here.

**Open Timekeeping** is a full-stack open-source timing system spanning hardware, firmware, detector adapters, timebase / clock sync, a runtime node, the timing core, APIs, and conformance. The first motivating domain is motorsport; the same primitives are designed to serve athletics, cycling, rowing, karting, RC racing, industrial checkpoints, and similar contexts.

---

## 2. Canonical terminology

The terms below are normative. Spelled out in detail in [`spec/glossary.md`](spec/glossary.md):

- **Detector**, a device or process that observes a physical event at a timing point.
- **Detector adapter**, the role that turns detector-specific data into canonical Open Timekeeping events. Implemented by firmware, standalone producer processes, runtime-node plugins, simulators, and replays alike.
- **Timing point**, a fixed location where detections happen.
- **Timebase**, a first-class clock source with sync state, resolution, and uncertainty.
- **Timing Runtime Node** (binary `otk-node`, crate [`timing-node`](timing-node/)), the deployable server process. Hosts one or more ingest listeners, one per transport binding.
- **Timing Fabric**, the deployed topology: nodes + producers + adapters + timebases + apps + storage + operator tools. Not a single repo or process.
- **Timing Core** (crate [`timing-core`](timing-core/)), the hexagon: the timing-domain engine, the application services that orchestrate it, **and** the inbound/outbound port traits that define the boundary. Owns `CrossingProcessor`, `SequenceGate`, `EventIngestService`, plus `EventIngestPort` + `EventQueryPort` (inbound) and `EventLog` + `IngestMetrics` (outbound). A library; the composition root ([`timing-node`](timing-node/) at v0) wires up adapters that implement the outbound ports and routes inbound traffic to the service.
- **OTK Protocol**, the transport-independent wire protocol used by Open Timekeeping components to exchange canonical timing messages. Defined as four stacked layers: Event Model, Wire Protocol, Frame Codec, Transport Binding. Not synonymous with TCP, HTTP, or any single transport.

`timing-node` is broker-like in deployment shape; we do not call it "the broker" in normative documentation.

---

## 3. Crates in this workspace

Read [`spec/architecture.md § Roles and where they live`](spec/architecture.md) for the canonical breakdown. The summary by category:

| Category | Crates |
|---|---|
| **Standard / spec** | [`spec/`](spec/) (docs only, not a crate) |
| **Event Model** | [`event-model`](event-model/) |
| **Wire Protocol** | [`otk-protocol`](otk-protocol/) (package name; directory matches) |
| **Frame Codec** | [`frame-codec`](frame-codec/) |
| **Ingest Protocol** | [`ingest-protocol`](ingest-protocol/) (transport-agnostic server-side handshake / dispatch) |
| **External contracts** | [`otk-contracts`](otk-contracts/) (detector adapter + timebase traits) |
| **Timing Core** | [`timing-core`](timing-core/) (hosts `domain/`, `ports/inbound/`, `ports/outbound/`, and `services/`; the inbound `EventIngestPort` / `EventQueryPort` and outbound `EventLog` / `IngestMetrics` port traits live here too) |
| **Ingest transport adapters** | [`adapter-ingest-tcp`](adapter-ingest-tcp/), [`adapter-ingest-unix-socket`](adapter-ingest-unix-socket/) |
| **Storage adapters** | [`adapter-event-log-segment`](adapter-event-log-segment/) (v0 backend) |
| **Producer / consumer SDK** | [`otk-sdk`](otk-sdk/) |
| **Reference producer** | [`producer-simulated`](producer-simulated/) (`otk-simulator` binary) |
| **Runtime** | [`timing-node`](timing-node/) (`otk-node` binary) |
| **Conformance** | [`conformance`](conformance/), [`conformance-fixtures`](conformance-fixtures/) |

### Planned, not yet shipped

| Role | Notes |
|---|---|
| Serial / USB-CDC ingest adapters | Re-use `frame-codec` (serial mode) + `ingest-protocol`. Future siblings to the ingest adapters above. |
| Manual-entry adapter | Operator-triggered detections (button press, keyboard, tablet). Re-uses `otk-sdk`'s producer feature. |
| Embedded toolkit (RP2040 / STM32 targets) | Future workspace or sibling repo. The protocol-layer crates here are `no_std` + `alloc` and ready for embedded use. |
| Live-timing and diagnostics apps | TypeScript / frontend stack, not in this repo. Future per-app repos. |
| Race-management domain layer | Entries, classifications, splits, chip-to-bib. Mid-stack between `timing-node` and the frontends. Not started. |

---

## 4. The OTK Protocol is transport-independent

This is a load-bearing rule for documentation, code, and design discussions. Canonical statement in [`spec/architecture.md`](spec/architecture.md); agents working in any crate must respect it.

The OTK Protocol is four stacked layers, each with its own crate. The bottom three are protocol-layer crates that are `no_std` + `alloc`; the fourth (transport binding) lives in per-transport adapter crates.

1. **Event Model** ([`event-model/`](event-model/)), canonical event types and identifiers. No transport assumptions.
2. **Wire Protocol** ([`otk-protocol/`](otk-protocol/), crate `otk-protocol`), the OTK message envelope (versioning, message types, source identity, sequence numbers, acks, errors, compatibility rules).
3. **Frame Codec** ([`frame-codec/`](frame-codec/)), encode/decode of OTK messages into byte frames. Length-prefix stream framing for reliable transports; COBS + CRC-16/CCITT-FALSE serial framing for unreliable byte streams.
4. **Transport Binding** (per-transport adapter crates: [`adapter-ingest-tcp/`](adapter-ingest-tcp/), [`adapter-ingest-unix-socket/`](adapter-ingest-unix-socket/), future serial / USB-CDC / etc.), how frames move over a specific link.

A fifth crate, [`ingest-protocol/`](ingest-protocol/), holds the transport-agnostic server-side handshake / dispatch state machine that every ingest adapter consumes. Per-transport adapters end up being ~socket lifecycle + byte I/O around `frame-codec` + `ingest-protocol`.

The detector adapter role can live in firmware (device speaks OTK directly), in an external adapter process, or as a [`timing-node`](timing-node/) plugin. A device that emits valid OTK frames directly already implements the adapter role; no node-side adapter is required. The trait contract a third-party detector implements lives in [`otk-contracts/`](otk-contracts/).

[`timing-node`](timing-node/) supports config-driven ingest listeners. A single runtime node can simultaneously listen for OTK frames over TCP, Unix socket, etc.; all listeners feed the same canonical ingest pipeline (sequence-gate → crossing processor → storage).

[`otk-sdk`](otk-sdk/)'s `producer` feature is a convenience library for external producers. It is **not** a required runtime process. Producers may use it, or build directly on `event-model` + `otk-protocol` + `frame-codec` + a transport binding.

For v0, the shipped transport bindings are TCP and Unix socket. Serial / USB-CDC are planned. Other bindings (raw Ethernet, CAN, RS-485, QUIC, MQTT, WebSocket) are deferred until there is a concrete need.

**Documentation rules every agent must follow:**

- Do not say detectors must have TCP stacks.
- Do not say OTK Protocol equals TCP.
- Do not say HTTP is the detector ingest data plane.
- Do not imply every topology needs an external producer process.
- Do say that OTK has canonical messages and frames that can travel over multiple bindings.
- Do say that timing-node can ingest from multiple listener types.
- Do say that adapters may live in firmware, external processes, or timing-node plugins.
- Do say that if a device emits valid OTK frames directly, the adapter role is already implemented by that device/firmware.

---

## 5. Development conventions

- **One Cargo workspace, root `Cargo.toml` declares all members.** Intra-workspace deps use `path = "../<crate-name>"`. Common third-party version pins live in `[workspace.dependencies]` and members opt into features via `{ workspace = true, features = [...] }`.
- **No org or `otk-` prefix on directory names** (the workspace root is the namespace). Crate names, binaries, and CLI commands may use `otk-` (e.g. the runtime node's binary is `otk-node`, the wire protocol crate's package name is `otk-protocol`).
- **Shared code belongs in a contract crate.** If two transport adapters need the same helper, add it to `frame-codec` or `ingest-protocol`. If two timebases need the same helper, add it to `otk-contracts`.
- **`no_std` discipline at the protocol layer.** `event-model`, `otk-protocol`, `frame-codec`, `otk-contracts` must not pull `tokio`, `std::net`, `std::fs`, or any other std-only dependency. This keeps the door open for firmware to depend on them.
- **Cross-cutting principles** apply everywhere: honest provenance over false precision, immutability of canonical events, separation of mechanism and policy, domain neutrality at the timing layer. See [`spec/architecture.md § Operating principles`](spec/architecture.md).

---

## 6. Where to find what

- **Conceptual model + terminology:** [`spec/`](spec/), starting with [`spec/architecture.md`](spec/architecture.md).
- **Unresolved technical decisions:** [`spec/open-questions.md`](spec/open-questions.md). When something is decided, the decision lands in the relevant crate and the question is removed from here.
- **Per-crate orientation:** each crate's `README.md` defines what belongs there and what doesn't.
- **Org community-health docs** (contributing, code of conduct, security, issue/PR templates) and the **org profile** at <https://github.com/Open-Timekeeping> live in the separate [`.github`](https://github.com/Open-Timekeeping/.github) repo.
