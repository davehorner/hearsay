<h1 align="center">hearsay 👂🗣️</h1>

<p align="center">
  <a href="https://github.com/matthewjberger/hearsay"><img alt="github" src="https://img.shields.io/badge/github-matthewjberger/hearsay-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20"></a>
  <a href="https://crates.io/crates/hearsay"><img alt="crates.io" src="https://img.shields.io/crates/v/hearsay.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20"></a>
  <a href="https://github.com/matthewjberger/hearsay/blob/main/LICENSE-MIT"><img alt="license" src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue?style=for-the-badge&labelColor=555555" height="20"></a>
</p>

<p align="center"><strong>Let your Rust apps talk to each other.</strong></p>

<p align="center">
  <code>cargo add hearsay</code>
</p>

hearsay is topic-based pub/sub for Rust, built on [tokio](https://tokio.rs). One process hosts the broker with a single call; any number of others connect and exchange binary or JSON messages over TCP. The broker is a value your program holds, not a separate server to run.

- **Typed contracts**: message schemas are strongly typed enums via [enum2contract](https://github.com/matthewjberger/enum2contract), re-exported so contracts need no extra dependency. Topics accept any `AsRef<str>`, but naming them through a contract's generated topic accessors (for example `MyContract::event_topic(channel)`) keeps topic strings checked at compile time instead of scattered as literals
- **Bridging**: brokers connect to each other
- **WebSockets**: browser and WASM apps join the same bus
- **Spawning**: the broker can own, supervise, and restart its client processes
- **Batching**: high-frequency traffic coalesces into single binary frames
- **Wildcards**: a subscription matches one exact topic, or an MQTT-style pattern with `+` (one level) and trailing `#` (the rest), so `sensors/+/temperature` and `sensors/#` fan a family of topics into one subscription while exact topics stay the fast path

## Quick Start

One program hosts the broker:

```rust
let broker = hearsay::Broker::start("127.0.0.1:9612").await?;
```

Any other program connects to it as a client:

```rust
let client = hearsay::Client::new("listener", hearsay::ClientSettings::default());
client.connect("127.0.0.1:9612").await?;
client.subscribe(&["greetings"]).await?;

client.publish("greetings", &"hello".to_string(), hearsay::Route::Global).await?;

if let Some(message) = client.next_message().await {
    if let Ok(text) = message.read::<String>() {
        println!("{text}");
    }
}
```

A `Client` is cheap to clone and every method takes `&self`, so the same connection can be shared across tasks.

The bus runs for as long as the `Broker` is held; `broker.stop()` (or dropping it) shuts everything down.

## Features

| Feature | Adds |
|---------|------|
| `websockets` | The broker accepts WebSocket peers, so browser and WASM apps participate directly |
| `udp` | The broker accepts UDP peers with per-message delivery guarantees (unreliable or reliable-ordered) for latency-sensitive traffic, sharing one subscription table with TCP and WebSocket peers |
| `nat` | Experimental UDP hole-punching rendezvous for direct peer-to-peer between hosts behind NAT |
| `spawn` | The broker owns, supervises, and restarts client app processes |

All are additive; the default build is the TCP broker, client, contracts, batching, and lifecycle runner.

## WebSockets

```toml
hearsay = { version = "0.3", features = ["websockets"] }
```

```rust
let broker = hearsay::Broker::start("127.0.0.1:9612").await?;
broker.start_websocket_listener("127.0.0.1:9613").await?;
```

WebSocket peers speak the same wire protocol as TCP peers and share the broker's subscription table. On `wasm32` targets the crate compiles down to the protocol surface (`Message`, the `encode_hello`/`encode_subscribe`/`encode_publish_text`/`encode_publish_binary` frame builders, and `decode_message`), so a browser client depends on hearsay itself for the wire format instead of mirroring it.

## UDP

```toml
hearsay = { version = "0.3", features = ["udp"] }
```

```rust
let broker = hearsay::Broker::start("127.0.0.1:9612").await?;
broker.start_udp_listener("127.0.0.1:9613").await?;

let client = hearsay::UdpClient::connect("player", "127.0.0.1:9613").await?;
client.subscribe(&["world/state"]).await?;
client.publish("input", &command, hearsay::Delivery::Unreliable).await?;
client.publish("chat", &line, hearsay::Delivery::ReliableOrdered).await?;
```

UDP peers share the broker's subscription table with TCP and WebSocket peers, and every publish chooses a delivery guarantee: `Unreliable` for high-frequency state where the freshest message matters more than every message, `ReliableOrdered` for events that must arrive once, in order (fragmented and reassembled when large). It is the transport substrate real-time networking builds on; see [docs/NETWORKING.md](docs/NETWORKING.md) for the reliability model and the authentication and timing primitives that complete it.

## Spawn

```toml
hearsay = { version = "0.3", features = ["spawn"] }
```

With the `spawn` feature, the broker owns its client app processes: they are killed when the broker is dropped, restarted according to a per-app policy, and each child receives the broker address in the `HEARSAY_BROKER` environment variable.

```rust
let broker = hearsay::Broker::start("127.0.0.1:9612").await?;
broker.spawn_app(hearsay::App {
    name: "worker".to_string(),
    path: "target/debug/worker".to_string(),
    restart_policy: hearsay::RestartPolicy::OnFailure,
    ..Default::default()
}).await?;

for line in broker.drain_output().await {
    println!("[{}] {}", line.app_name, line.line);
}
```

## Batching

High-frequency traffic coalesces into single binary messages, paying the per-message overhead once per flush instead of once per item:

```rust
let mut batch = hearsay::Batch::new("scene/updates", hearsay::Route::Global, 64, Duration::from_millis(50));
batch.push(&client, update).await?;  // flushes on size or interval
batch.flush(&client).await?;         // or flush manually

// subscriber side
let updates: Vec<EntityUpdate> = message.read_batch()?;
```

Batches travel as binary postcard frames. That same typed binary encoding is what `publish` itself uses for single values: `client.publish(topic, &value, Route::Global)` serializes one value with postcard and `message.read::<T>()` decodes it. `publish` is the default because postcard is the smallest and cheapest encoding for the common case of Rust services that share their types; it is not self-describing, so both ends must agree on the type and version. When a peer needs a self-describing format instead (a browser or WASM client, or a consumer on a different version of the types), `publish_json` sends JSON text and `message.read_json::<T>()` decodes it. `publish_text` forwards an already-serialized string and `publish_bytes` sends raw bytes.

## Delivery and backpressure

Delivery is best-effort: there are no acknowledgements and no replay, so a message published while a subscriber is disconnected is gone. Each subscriber has two bounded queues at the broker: a control queue for the broker's own coordination messages (the `hearsay/` topics: peer connections, introspection reports, bridge events) and a data queue for everything else. High-frequency application data that outruns a subscriber is dropped from the data queue, the conventional best-effort behavior. Control messages are never silently dropped; if a subscriber is so stalled that even its low-volume control queue fills, the broker disconnects it, and with `autoreconnect` (the default) the client reconnects and re-subscribes rather than carrying a silent gap in coordination state.

Across bridges, each message is forwarded with the set of brokers it has already traversed and a per-origin sequence number. A broker drops a message whose path already includes itself, which guarantees forwarding terminates on any topology including cycles. Each broker tracks a per-origin high-water mark, so a given message is delivered at most once no matter how many redundant paths carry it to that broker; the only state bounded for safety is the number of distinct origins tracked, evicted least-recently-seen first. The reorder window is likewise bounded, so a single message delayed behind a very large number of newer ones from the same origin may be dropped rather than delivered late, which is consistent with the best-effort contract.

## Testing

The `Publisher`, `Subscriber`, and `MessageBus` traits let application code be generic over the transport. A `Client` implements them, and so does `MockBus`, an in-memory bus that records what was published and lets a test inject messages, so logic that talks to the bus can be unit-tested without a running broker:

```rust
let bus = hearsay::MockBus::new();
bus.publish("greetings", &"hello".to_string(), hearsay::Route::Global).await?;
assert_eq!(bus.published().len(), 1);

bus.inject_text("greetings", "\"hi back\"");
let message = bus.next_message().await.unwrap();
```

Write functions against `impl MessageBus` (or `impl Publisher` / `impl Subscriber`) and pass a real `Client` in production and a `MockBus` under test.

## Example

`examples/pingpong.rs` exchanges a command and an event between two services using an enum contract. Run each role as its own process in separate terminals:

```
cargo run --example pingpong -- broker
cargo run --example pingpong -- responder
cargo run --example pingpong -- requester
```

Run all three in a single process:

```
cargo run --example pingpong
```

Or have the broker spawn and supervise the other roles as child processes:

```
cargo run --example pingpong --features spawn -- host
```

## Demo

The [demo/](demo) directory is a complete multi-process application built on hearsay: a process-per-window desktop shell using Bevy and egui. The first window hosts the broker and connects to it as a client; "New Window" spawns another process of the same executable, supervised by the broker, and all windows coordinate layouts over pub/sub topics.

```
just run-demo
```

[demo-cubelink/](demo-cubelink) is a two-player game on the nightshade engine that networks over the UDP transport: each player drives a cube around a floor with a pan-orbit view and they chat over a retained-UI panel, positions sent unreliably and chat sent reliably.

```
just run-cubelink
```

## Documentation

Architecture documentation lives in [docs/](docs/ARCHITECTURE.md), covering the broker, client, wire protocol and contracts, batching, lifecycles, process spawning, WebSockets, and [networking](docs/NETWORKING.md).

## License

Dual-licensed under MIT ([LICENSE-MIT](LICENSE-MIT)) or Apache 2.0 ([LICENSE-APACHE](LICENSE-APACHE)).
