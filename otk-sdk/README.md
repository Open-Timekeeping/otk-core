# otk-sdk

Open Timekeeping SDK for producers and consumers.

> **Status: active.** Producer feature is complete: `DetectorAdapter`, `Timebase`, builders, `Producer` connection. Consumer feature is a stub pending the timing-node REST/SSE API (Phase 2).

## What this is

`otk-sdk` is the SDK for applications that interact with an OTK timing node. It has two feature sets:

- **`client`** (default): consumer-side API for reading events from a timing node over HTTP/SSE. Stub for Phase 2.
- **`producer`**: producer-side API for connecting to a timing node and publishing events. Includes `DetectorAdapter` and `Timebase` trait contracts, builder helpers, and the `Producer` connection type.

The SDK re-exports `event-model` types so dependents need only add `otk-sdk` to their `Cargo.toml`.

## Feature selection

```toml
# Consumer only (default)
otk-sdk = { git = "https://github.com/Open-Timekeeping/otk-sdk" }

# Producer only (excludes HTTP client code)
otk-sdk = { git = "...", default-features = false, features = ["producer"] }

# Both roles
otk-sdk = { git = "...", features = ["producer"] }
```

## Producer usage

The example below uses `#[tokio::main]`. Add `tokio` to your `Cargo.toml`:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use otk_sdk::producer::{
    DetectionBuilder, MetadataBuilder, Producer, ProducerConfig,
    SequenceCounter, Transport, now_ns,
};
use otk_sdk::event_model::{DetectorId, TimingPointId, OtkEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect
    let config = ProducerConfig::new("loop-adapter-1");
    let mut producer = Producer::connect(
        Transport::Tcp("127.0.0.1:8463".parse()?),
        config,
    ).await?;

    // Publish events
    let detector_id = DetectorId::new("loop-a");
    let tp_id = TimingPointId::new("finish");
    let seq = SequenceCounter::new();

    let detection = DetectionBuilder::new(&detector_id, &tp_id, now_ns(), seq.next()).build();
    producer.send_event(OtkEvent::Detection(detection)).await?;

    // Graceful disconnect
    producer.disconnect().await?;
    Ok(())
}
```

## Implementing a detector adapter

The `DetectorAdapter` and `Timebase` traits use async methods. Add `async-trait`
to your `Cargo.toml`:

```toml
async-trait = "0.1"
```

```rust
use otk_sdk::producer::{DetectorAdapter, AdapterEvent, AdapterError, AdapterState};
use otk_sdk::event_model::DetectorId;

struct MyDetector { id: DetectorId }

#[async_trait::async_trait]
impl DetectorAdapter for MyDetector {
    fn detector_id(&self) -> &DetectorId { &self.id }
    fn state(&self) -> AdapterState { AdapterState::Running }
    async fn start(&mut self) -> Result<(), AdapterError> { Ok(()) }
    async fn stop(&mut self) -> Result<(), AdapterError> { Ok(()) }
    async fn next_event(&mut self) -> Option<Result<AdapterEvent, AdapterError>> {
        // return events or None when stopped
        None
    }
}
```

## Where this sits in the architecture

```text
sdk/
  otk-sdk/               this repo: consumer default, producer opt-in

producers/
  simulator/             uses otk-sdk producer feature

server/
  core/event-model/      re-exported by otk-sdk
  core/protocol/         used internally by producer feature (not re-exported)
```

The SDK does not depend on any server-side ports or adapters. The only shared
dependencies with the server are `event-model` (always) and `protocol` (producer
feature only, for wire encoding).

## Dependencies

**Always:** `event-model`, `thiserror`.

**`producer` feature:** `otk-contracts`, `protocol`, `minicbor`, `tokio` (in addition to `event-model`). Vendors implementing the `DetectorAdapter` / `Timebase` traits also need their own direct `async-trait = "0.1"` dep — see the example above.

**`client` feature:** no additional deps yet (placeholder). `reqwest` and `tokio-stream` will be added in Phase 2 when the HTTP/SSE implementation lands.

## Open questions

- **`client` feature implementation.** Pending timing-node REST/SSE API (Phase 2).
- **`producer-serial` feature.** Extend `Transport` with a `Serial { port, baud }` variant and add `tokio-serial` dependency.
- **Reconnect helpers.** Exponential backoff wrapper around `Producer::connect`.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
