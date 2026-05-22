# adapter-ingest-tcp

TCP ingest adapter for OTK. Implements [`port-in-ingest`](https://github.com/Open-Timekeeping/otk-core/tree/main/port-in-ingest) over plain TCP, accepting producer connections and delivering typed events to the timing node.

> **Status: active.** Wraps the upstream `frame-codec` + `ingest-protocol` crates with TCP socket lifecycle. See [open questions](#open-questions) for deferred items.

## What this is

`adapter-ingest-tcp` is an inbound adapter in the OTK hexagonal architecture. It implements the `EventIngestPort` and `IngestSession` traits from `port-in-ingest` using plain TCP as the physical transport.

The adapter is intentionally thin: it owns TCP accept loop and per-session byte I/O, and delegates everything else upward:

- **Framing** (length-prefix, partial-read buffering, oversize / truncation detection) → [`frame_codec::StreamFrameDecoder`](https://github.com/Open-Timekeeping/otk-core/tree/main/frame-codec) + `encode_stream`.
- **Handshake and post-handshake envelope dispatch** → [`ingest_protocol`](https://github.com/Open-Timekeeping/otk-core/tree/main/ingest-protocol): `perform_server_handshake_with_auth`, `PostHandshakeProcessor`, `InboundAction`.

The timing node receives typed `OtkEvent` values and never sees raw frames or protocol envelopes.

## Where this sits in the architecture

```text
otk-core (one repo, many crates)
  event-model/              domain DTOs (OtkEvent, Detection, ...)
  protocol/                 wire DTOs (OtkEnvelope, Connect, ConnectAck, ...)
  frame-codec/              length-prefix + COBS frame codecs
  ingest-protocol/          transport-agnostic handshake + dispatch state machine
  port-in-ingest/           inbound port contract (EventIngestPort, IngestSession)
adapter-ingest-tcp/         this repo: TCP socket lifecycle around the above
timing-node/                injects this adapter at startup
```

The timing node depends on `port-in-ingest` (the trait), not on this crate directly. This crate is the concrete adapter injected at the composition root.

## Design decisions

**Framing.** Each OTK frame is a 4-byte big-endian u32 length prefix followed by that many bytes of CBOR-encoded `OtkEnvelope`. The max payload size is configurable (default 65,535 bytes). Implementation lives in `frame-codec::StreamFrameDecoder`; this adapter only wires bytes in and envelopes out.

**Handshake.** `accept` completes the OTK protocol handshake before returning the session. The handshake state machine is `ingest_protocol::perform_server_handshake_with_auth`; this adapter feeds it the producer's first envelope, sends back the reply envelope (`ConnectAck` on success, `ConnectReject` on version mismatch or unauthorized), and on success retains the returned `PostHandshakeProcessor` for the session lifetime.

**Authentication.** `TcpIngestPort::bind` uses the `AllowAll` authoriser (development default). Production deployments use `TcpIngestPort::bind_with_auth(config, authoriser)` and supply a `ConnectAuthoriser` (typically a token allow-list constructed by `timing-node` from `NodeConfig.auth.producer_tokens`).

**Typed delivery.** `IngestSession::next_event` returns `OtkEvent` values. `Heartbeat` messages are validated then consumed silently; `Disconnect` and clean TCP close at frame boundaries both map to `Ok(None)`. EOF mid-frame is reported as `IngestError::Decode` so producers that crash mid-publish don't look like clean disconnects.

**Plain TCP only for v0.** TLS support is deferred.

## Development

This crate uses sibling-relative path deps to `otk-core`:

```toml
port-in-ingest  = { path = "../otk-core/port-in-ingest" }
frame-codec     = { path = "../otk-core/frame-codec" }
ingest-protocol = { path = "../otk-core/ingest-protocol" }
# ...
```

Local development expects the standard Open Timekeeping workspace layout: each repo cloned as a sibling under one parent directory. From a fresh clone of just this repo, `cargo build` will fail until `otk-core` is present alongside it. See [`open-timekeeping/AGENTS.md`](https://github.com/Open-Timekeeping/.github) for the workspace conventions. Crates.io / published-version deps will replace path deps once the `otk-core` crates publish.

## Usage

```rust
use adapter_ingest_tcp::{TcpIngestPort, TcpIngestConfig};
use port_in_ingest::{EventIngestPort, IngestSession};

let config = TcpIngestConfig {
    bind_addr: "0.0.0.0:8463".parse().unwrap(),
    ..Default::default()
};
let port = TcpIngestPort::bind(config).await?;

loop {
    match port.accept().await {
        Ok(mut session) => {
            tokio::spawn(async move {
                while let Ok(Some(event)) = session.next_event().await {
                    // deliver event to timing node
                }
            });
        }
        Err(e) => {
            // transient errors (handshake timeout, version mismatch) do not affect
            // the listener; log and continue
            eprintln!("accept error: {e}");
        }
    }
}
```

## Dependencies

**Depends on:** [`port-in-ingest`](https://github.com/Open-Timekeeping/otk-core/tree/main/port-in-ingest), [`protocol`](https://github.com/Open-Timekeeping/otk-core/tree/main/protocol), [`event-model`](https://github.com/Open-Timekeeping/otk-core/tree/main/event-model), [`frame-codec`](https://github.com/Open-Timekeeping/otk-core/tree/main/frame-codec), [`ingest-protocol`](https://github.com/Open-Timekeeping/otk-core/tree/main/ingest-protocol), `async-trait`, `tokio`.

**Used by:** [`timing-node`](https://github.com/Open-Timekeeping/timing-node) as its default ingest transport.

## Open questions

- **TLS support.** Deferred for v0. When added, `TcpIngestConfig` will gain an optional `tls` field wrapping a `rustls::ServerConfig`.
- **Backpressure.** `next_event` is demand-driven and enforces `max_frame_bytes` per frame, so the adapter does not read frames into memory without bound on its own. If the timing node decouples I/O from processing via an unbounded channel, that channel is where growth can occur; a bounded channel between the adapter and the node pipeline would address it.
- **Sequence gap detection.** The adapter sees sequence numbers on `OtkEnvelope` but does not currently validate gaps or report them to the timing node.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
