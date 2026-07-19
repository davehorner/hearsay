# Client

The client connects to a broker, publishes, subscribes, and receives messages. Source lives in `src/client.rs`. The public API is methods on the `Client` struct; the struct itself only carries state.

## 1. State

```rust
pub struct Client {
    shared: Arc<ClientShared>,
}
```

`ClientShared` holds the peer id (immutable after creation), the settings, the live connection handle in an `ArcSwapOption<Connection>` (loaded atomically on every publish, no lock), the receiver slot (`Mutex<Option<Receiver<Message>>>`), and a cold-state mutex guarding the subscription caches, the broker address, and the reconnection-task flag. Waiting for a message locks only the receiver slot, and publishing locks nothing (see [ARCHITECTURE.md](ARCHITECTURE.md) section 5).

`Client::new(name, settings)` assigns the peer id as `{name}_{uuid_v4}`, so multiple instances of the same program get distinct identities on the broker. `client.id()` returns it. The broker overrides the id at creation for bridge clients (bridge echo prevention).

`ClientSettings`:

- `max_connection_attempts: Option<u16>`. `Some(n)` retries up to n times with a 1-second pause between attempts; `None` means exactly one attempt (used by bridges).
- `autoreconnect: bool`. Spawns the background reconnection task on first connect.
- `timeout_per_attempt: Duration`. Per-attempt TCP connect timeout.
- `read_timeout: Option<Duration>`. If set, the read task treats a silent connection as dead after this long without a frame.

## 2. Connecting and reconnecting

`client.connect(address)` records the address in state, runs `establish_connection`, and (if `autoreconnect` is set and no task exists yet) spawns the reconnection task. Exactly one reconnection task ever exists per client, and it holds only a `Weak` reference to the state, so it exits when the `Client` is dropped.

`establish_connection`:

1. `connect_with_retries` dials the address using standard host resolution, attempting IPv4 addresses first to match the broker's bind preference (so `localhost` reaches a broker that bound `127.0.0.1` even when the resolver lists `::1` first, or when a firewall black-holes one family). TCP keepalive (1s probe, 1s interval) is configured on the stream.
2. The stream is split, the reader and writer tasks are spawned, the new receiver is installed into the receiver slot, and the new connection handle is swapped in atomically. `Hello` is sent and `resubscribe` replays the union of cached and pending subscriptions (a pending topic is only removed from the pending set after its subscribe frame is sent successfully, so partial failures cannot lose topics).

The reconnection task starts at a 2-second interval with exponential backoff (capped at 30 seconds); when the client is disconnected and an address is known, it re-runs `establish_connection`. Reconnecting to a different address is just calling `client.connect` again: the recorded address changes and the existing task follows it.

Disconnection is detected in two places: the reconnection task observes a closed outbound channel when the writer task dies (so publish-only clients and bridges notice promptly), and `next_message`/`try_next_message` clear the receiver and connection handle when the receive channel closes (the read task exited because the socket died).

## 3. Publishing

- `client.publish(topic, payload, route)` serializes any `impl Serialize` as JSON directly into the frame buffer. JSON is the default: it is self-describing, tolerant across schema versions, and the format browser and WASM peers decode with `serde_json`, which makes it the right choice for typed contracts and cross-language interop.
- `client.publish_postcard(topic, &value, route)` serializes any `impl Serialize` as postcard and sends it as a binary publish, the compact counterpart to `publish`. The subscriber decodes it with `message.read::<T>()`. Prefer this for high-frequency Rust-to-Rust traffic where the byte savings matter and both ends share the type; prefer `publish` everywhere schema tolerance or non-Rust peers are in play.
- `client.publish_json(topic, payload, route)` sends an existing JSON string as a text publish.
- `client.publish_bytes(topic, payload, route)` sends a raw binary publish; subscribers receive it through `Message::bytes()`.

Each publish builds the complete wire frame in one buffer — length prefix, header, and payload serialized in place — and sends it to the writer task. There are no intermediate strings, no re-serialization, and no lock: the connection handle is an atomic load.

The `Route` controls the scope of the publish:

- `Route::Global` (default): delivered to local subscribers and forwarded across bridges.
- `Route::Local`: delivered to local subscribers only; sets the local-only flag on the wire so the broker skips bridges.

A send failure (the writer task died) surfaces as `Error::NotConnected`.

## 4. Subscriptions

`client.subscribe(topics)` sends a subscribe frame per topic and caches it. While disconnected, topics go to the pending set instead and are applied on the next successful connect. If a subscribe send fails mid-call, the topic is moved to pending and the error is returned, so the subscription still applies after reconnect.

A subscription is either an exact topic or an MQTT-style wildcard pattern: `+` matches exactly one topic level and a trailing `#` matches the rest of the levels. `client.subscribe(&["sensors/+/temperature"])` receives every per-room temperature topic and `client.subscribe(&["sensors/#"])` receives everything under `sensors/`, both through one subscription. Exact topics stay the fast path in the broker (see [BROKER.md](BROKER.md) section 5); wildcard patterns are tested against publish traffic only when some subscriber has actually registered one.

`client.unsubscribe(topics)` removes the topic from both the pending set and the cache and notifies the broker when connected, so an offline unsubscribe does not resurrect on reconnect.

`client.subscriptions()` returns the current cache.

## 5. Receiving

- `client.next_message()` awaits the receive channel. `None` means the connection is gone (and the receiver and connection handle have been cleared so reconnection can proceed). The await holds only the receiver-slot mutex and is cancel-safe, so wrapping it in `tokio::time::timeout` (as `run_lifecycle` does) cannot lose messages or the receiver.
- `client.try_next_message()` is the non-blocking variant using `try_recv`.

There is no message filtering in the client: every message for every subscribed topic comes through the same stream, and receivers match on `message.topic()` (see [CONTRACTS.md](CONTRACTS.md) for the idiomatic pattern).

## 6. Bridge requests

`client.open_bridge(source_address, target_address)` and `client.close_bridge(target_address)` send the corresponding bridge frames. The acknowledged forms are internal, broker-to-broker halves of the handshake described in [BROKER.md](BROKER.md) section 6.

## 7. The bus abstraction and testing

Three traits let application code be generic over the transport instead of naming `Client` directly: `Publisher` (the `publish` family), `Subscriber` (`subscribe`, `unsubscribe`, `next_message`, `try_next_message`, `subscriptions`), and `MessageBus`, which is both. A `Client` implements all three, so a function written against `impl MessageBus` (or `impl Publisher` / `impl Subscriber`) runs unchanged against a real connection.

`MockBus` is the in-memory implementation of `Publisher` and `Subscriber` for tests. It records every publish and lets a test feed messages back in without a broker or a socket:

```rust
let bus = hearsay::MockBus::new();

bus.publish("greetings", &"hello".to_string(), hearsay::Route::Global).await?;
let sent: Vec<hearsay::Published> = bus.published();
assert_eq!(sent.len(), 1);

bus.inject_text("greetings", "\"hi back\"");
let message = bus.next_message().await.unwrap();
assert_eq!(message.text(), Some("hi back"));
```

`published()` returns the recorded publishes as `Published` values; `inject_text(topic, json)`, `inject_bytes(topic, &[u8])`, and `inject(message)` place a message on the receive side; `next_message()` drains it; and `subscriptions()` reports what the code under test subscribed to. This lets the message-handling half of a service (its `receive_message` logic, its contract matching) be unit-tested directly, with a `MockBus` in the test and a `Client` in production behind the same `impl MessageBus`.
