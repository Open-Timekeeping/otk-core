# protocol

Shared DTOs for the OTK wire protocol: the message envelope, handshake types, and protocol semantics.

> **Status: active.** Envelope shape, message catalog, handshake flow, and acknowledgement model are settled. See [open questions](#open-questions) for remaining decisions.

## What this is

`protocol` defines the data types that the server and producers must both agree on to communicate. It is the shared DTO layer for the ingest boundary: transport-agnostic, encoding-agnostic, and dependency-free except for `event-model`.

Concretely, this crate owns:

- `OtkEnvelope`: the header that wraps every OTK message on the wire.
- `MessageType`: the full message catalog (event, handshake, heartbeat, error, disconnect).
- Handshake messages: `Connect`, `ConnectAck`, `ConnectReject`.
- Error reporting: `ErrorMessage`, `ErrorCode`.
- Keep-alive: `Heartbeat`.
- `OtkMessage`: the decoded, typed form of an envelope payload.
- Protocol semantics: version negotiation, error-only ack model, sequence-number field and error vocabulary enabling gap detection by producers and servers.

This crate does **not** own frame encoding (length-prefix, COBS, CRC); that is internal to each side's transport implementation. It does **not** own transport mechanics; those are in `server/adapters/adapter-ingest-tcp` (server side) and `sdk/otk-sdk` (producer side).

## Where this sits in the architecture

```text
server/
  core/
    event-model/     domain DTOs (Detection, OtkEvent, ...)
    protocol/        wire DTOs (OtkEnvelope, MessageType, ...)   <-- this repo
    timing-core/     business logic
  ports/
    port-in-ingest/  typed ingest port contract
  adapters/
    adapter-ingest-tcp/  decodes protocol -> OtkEvent, implements port-in-ingest
sdk/
  otk-sdk/           producer feature: encodes OtkEvent -> protocol
```

The same message envelope is used whether the producer speaks OTK over TCP, serial, or USB CDC.

## Design decisions

### Acknowledgement model

Silence means success. The server only sends messages on the event channel when something is wrong (`ErrorMessage`). Per-event acks are not used; producers detect loss via sequence-number gaps. The only mandatory request/response exchange is the handshake (`Connect` / `ConnectAck`).

### Handshake and version negotiation

Two-step. The producer sends `Connect` advertising a `[protocol_version_min, protocol_version_max]` range. The server replies with `ConnectAck` carrying the negotiated version, or `ConnectReject` if no overlap exists.

### Envelope fields

Every message carries: `protocol_version`, `message_type`, `source_id`, `stream_id` (None for most protocol messages), `sequence_number` (None for protocol messages), `correlation_id` (optional), and `payload` (CBOR-encoded bytes, or None for `Disconnect`).

### Plugin path

Adapters compiled into the same process as the timing-node use a Rust trait defined in `plugin-api` and produce `event_model::OtkEvent` values directly, with no envelope overhead.

## Source layout

```
src/
  lib.rs         crate doc, re-exports, PROTOCOL_VERSION constant
  ids.rs         ProducerId, CorrelationId
  envelope.rs    OtkEnvelope, MessageType
  handshake.rs   Connect, ConnectAck, ConnectReject, ConnectRejectReason
  error.rs       ErrorMessage, ErrorCode
  heartbeat.rs   Heartbeat
  message.rs     OtkMessage (decoded typed form)
```

## Dependencies

**Depends on:** [`event-model`](../event-model) (for `StreamId`, `StreamDescriptor`, and `OtkEvent` in `OtkMessage`).

**Used by:** `server/adapters/adapter-ingest-tcp` (server-side decode), `sdk/otk-sdk` producer feature (producer-side encode).

## Open questions

- **Sequence number scope.** Per-stream monotonic counter vs. single per-connection counter. Per-stream is better for gap detection; per-connection is simpler. Decision pending.
- **Backward-compatibility rules.** What constitutes a breaking change? Additive-only policy vs. explicit versioned breaks.
- **Disconnect payload.** A reason code (`GracefulShutdown`, `ConfigChange`, `Error`) may be useful for server-side logging.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
