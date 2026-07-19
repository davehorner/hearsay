# WebSockets

With the `websockets` feature (`websockets = ["dep:tokio-tungstenite", "dep:futures-util"]`), the broker accepts WebSocket peers alongside TCP peers. This is the path for browser and WASM apps, which cannot open raw TCP sockets. Source lives in `src/websocket.rs`, with the transport split in `src/broker.rs` (`PeerWriter`).

WebSocket support is integrated into the broker rather than provided as a separate gateway process: a WebSocket peer is a first-class peer in the same subscription table as TCP peers, with no translation protocol and no polling relay in between.

## 1. Starting the listener

```rust
let broker = hearsay::Broker::start("127.0.0.1:9612").await?;
broker.start_websocket_listener("127.0.0.1:9613").await?;
```

`start_websocket_listener` binds its own address, resolved the same way as the TCP listener (standard host resolution, IPv4 preferred, so `localhost` binds loopback and `0.0.0.0` is the explicit all-interfaces opt-in), and spawns an accept loop that feeds the same broker event channel as the TCP listener. Each accepted connection goes through the tungstenite handshake and then `websocket_connection_task`, which mirrors the TCP connection task: it parses incoming frames, forwards them via the shared `process_incoming_frame`, and parks the sink half as the peer's writer (`PeerWriter::WebSocket`).

## 2. Wire format

The protocol is byte-identical to TCP (see [CONTRACTS.md](CONTRACTS.md)): each **binary** WebSocket message carries one complete hearsay frame, including the 4-byte length prefix, so the broker fans the same shared buffer out to TCP and WebSocket peers alike.

- Text frames and pings are ignored; a close frame ends the session. A malformed binary frame kills the connection, the same as on TCP.
- tungstenite's default 64 MiB message cap matches the library's own frame cap.

A session looks like:

1. Send `encode_hello(id)`.
2. Send `encode_subscribe(topic)` per topic.
3. Send `encode_publish_text(..)` / `encode_publish_binary(..)` to publish.
4. Read binary frames and decode each with `decode_message`.

`tests/websocket.rs` is the reference implementation of a WebSocket peer (using tokio-tungstenite as the client) and verifies both directions interoperate with a TCP peer through one broker.

## 3. Disconnects and lifetimes

A WebSocket peer gets the same writer task, shutdown oneshot, and generation-checked disconnect handling as a TCP peer ([BROKER.md](BROKER.md) section 4). When the socket dies, the peer is removed from the subscription table; reconnecting with the same id replaces the old session safely.

## 4. WASM clients

A browser client implements the four-step session above over the platform WebSocket API, and it depends on hearsay itself for the wire format: on `wasm32` targets the crate compiles down to the protocol surface (the `encode_*` helpers, `decode_message`, `Message` with its `read_batch` and `read` decoders, and `BrokerContract` and its payloads), with all the tokio-backed machinery and its dependencies compiled out. There is no byte-format mirroring and no gateway process: the WASM client builds frames with the helpers, sends them as binary messages, and decodes incoming binary messages with `decode_message`. CI checks the wasm32 build on every push.
