# port-in-ingest

Inbound port contract for OTK event ingestion: the typed boundary between the timing node and its transport adapters.

> **Status: active.** Trait shape and session semantics are settled. See [open questions](#open-questions) for what remains.

## What this is

`port-in-ingest` is an inbound port in the OTK hexagonal architecture. It defines the boundary between the timing-node application and the transport layer that delivers events from producers. The timing node calls this contract; concrete transport adapters implement it.

Concretely:

- **`EventIngestPort`**: the server-side listener. `accept` suspends until the next producer connects and completes the OTK handshake, then returns a ready `IngestSession`. Framing, CBOR decoding, and handshake mechanics are adapter concerns and are not visible here.
- **`IngestSession`**: a single connected producer. Poll `next_event` until it returns `None` (clean disconnect) or `Err` (terminal error). `producer_id` and `peer_addr` identify the session.
- **`IngestError`**: error vocabulary covering connection failures, handshake failures, and decode errors.

## Where this sits in the architecture

```text
server/
  core/
    event-model/            domain DTOs (OtkEvent, Detection, ...)
    protocol/               wire DTOs (OtkEnvelope, MessageType, ...)
  ports/
    port-in-ingest/         inbound port contract   <-- this repo
    port-out-event-log/     outbound port contract
  adapters/
    adapter-ingest-tcp/     implements port-in-ingest over TCP
  app/
    timing-node/            depends on this port; injects the adapter
```

The timing node depends on `port-in-ingest` (the trait), not on any adapter directly. Adapters are the composition root's concern.

## Design decisions

**Typed event delivery.** The port delivers `OtkEvent` values directly. There are no raw frames or envelope types visible through this interface; framing, CBOR decoding, and the OTK handshake are fully encapsulated inside each adapter.

**Session-per-producer.** `accept` returns one session per producer connection. The timing node drives `next_event` on each session independently, typically in a spawned task.

**`&self` on `accept`.** The port itself is shared; sessions are independent. The adapter manages its own internal listener state.

**`&mut self` on session methods.** Sessions are not shared; a single task owns each session.

**`None` means clean disconnect; `Err` means terminal error.** Callers should distinguish these: a clean disconnect is expected behavior; a terminal error may warrant logging or alerting.

## Source layout

```
src/
  lib.rs      crate doc, re-exports
  error.rs    IngestError
  port.rs     EventIngestPort trait
  session.rs  IngestSession trait
```

## Dependencies

**Depends on:** `event-model`, `async-trait`, `thiserror`.

**Implemented by:** [`adapter-ingest-tcp`](../adapter-ingest-tcp) (TCP transport).

**Used by:** [`timing-node`](../timing-node) (injects the adapter at startup).

## Open questions

- **Conformance helper shape.** A test suite adapters can run against themselves to verify correct implementation.
- **Backpressure.** If the timing node processes events slower than a producer sends them, the session should signal backpressure. Currently not modeled.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
