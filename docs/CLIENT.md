# Client

The client connects to a broker, publishes, subscribes, and receives messages. Source lives in `src/client.rs`. The public API is free functions over the `Client` struct; the struct itself only carries state.

## 1. State

```rust
pub struct Client {
    shared: Arc<ClientShared>,
}
```

`ClientShared` holds the peer id (immutable after creation), the settings, the live connection handle in an `ArcSwapOption<Connection>` (loaded atomically on every publish, no lock), the receiver slot (`Mutex<Option<Receiver<Message>>>`), and a cold-state mutex guarding the subscription caches, the broker address, and the reconnection-task flag. Waiting for a message locks only the receiver slot, and publishing locks nothing (see [ARCHITECTURE.md](ARCHITECTURE.md) section 5).

`create_client(name, settings)` assigns the peer id as `{name}_{uuid_v4}`, so multiple instances of the same program get distinct identities on the broker. `client_id` returns it. The broker overrides the id at creation for bridge clients (bridge echo prevention).

`ClientSettings`:

- `max_connection_attempts: Option<u16>`. `Some(n)` retries up to n times with a 1-second pause between attempts; `None` means exactly one attempt (used by bridges).
- `autoreconnect: bool`. Spawns the background reconnection task on first connect.
- `timeout_per_attempt: Duration`. Per-attempt TCP connect timeout.
- `read_timeout: Option<Duration>`. If set, the read task treats a silent connection as dead after this long without a frame.

## 2. Connecting and reconnecting

`connect(&client, address)` records the address in state, runs `establish_connection`, and (if `autoreconnect` is set and no task exists yet) spawns the reconnection task. Exactly one reconnection task ever exists per client, and it holds only a `Weak` reference to the state, so it exits when the `Client` is dropped.

`establish_connection`:

1. `connect_with_retries` dials the address using standard host resolution, attempting IPv4 addresses first to match the broker's bind preference (so `localhost` reaches a broker that bound `127.0.0.1` even when the resolver lists `::1` first, or when a firewall black-holes one family). TCP keepalive (1s probe, 1s interval) is configured on the stream.
2. The stream is split, the reader and writer tasks are spawned, the new receiver is installed into the receiver slot, and the new connection handle is swapped in atomically. `Hello` is sent and `resubscribe` replays the union of cached and pending subscriptions (a pending topic is only removed from the pending set after its subscribe frame is sent successfully, so partial failures cannot lose topics).

The reconnection task starts at a 2-second interval with exponential backoff (capped at 30 seconds); when the client is disconnected and an address is known, it re-runs `establish_connection`. Reconnecting to a different address is just calling `connect` again: the recorded address changes and the existing task follows it.

Disconnection is detected in two places: the reconnection task observes a closed outbound channel when the writer task dies (so publish-only clients and bridges notice promptly), and `next_message`/`try_next_message` clear the receiver and connection handle when the receive channel closes (the read task exited because the socket died).

## 3. Publishing

- `publish(client, topic, payload, route)` serializes any `impl Serialize` as JSON directly into the frame buffer.
- `publish_json(client, topic, payload, route)` sends an existing JSON string as a text publish.
- `publish_bytes(client, topic, payload, route)` sends a binary publish; subscribers receive it through `Message::bytes()`.

Each publish builds the complete wire frame in one buffer — length prefix, header, and payload serialized in place — and sends it to the writer task. There are no intermediate strings, no re-serialization, and no lock: the connection handle is an atomic load.

The `Route` controls the scope of the publish:

- `Route::Global` (default): delivered to local subscribers and forwarded across bridges.
- `Route::Local`: delivered to local subscribers only; sets the local-only flag on the wire so the broker skips bridges.

A send failure (the writer task died) surfaces as `Error::NotConnected`.

## 4. Subscriptions

`subscribe(client, topics)` sends a subscribe frame per topic and caches it. While disconnected, topics go to the pending set instead and are applied on the next successful connect. If a subscribe send fails mid-call, the topic is moved to pending and the error is returned, so the subscription still applies after reconnect.

`unsubscribe(client, topics)` removes the topic from both the pending set and the cache and notifies the broker when connected, so an offline unsubscribe does not resurrect on reconnect.

`subscriptions(client)` returns the current cache.

## 5. Receiving

- `next_message(&client)` awaits the receive channel. `None` means the connection is gone (and the receiver and connection handle have been cleared so reconnection can proceed). The await holds only the receiver-slot mutex and is cancel-safe, so wrapping it in `tokio::time::timeout` (as `run_lifecycle` does) cannot lose messages or the receiver.
- `try_next_message(&client)` is the non-blocking variant using `try_recv`.

There is no message filtering in the client: every message for every subscribed topic comes through the same stream, and receivers match on `message.topic()` (see [CONTRACTS.md](CONTRACTS.md) for the idiomatic pattern).

## 6. Bridge requests

`open_bridge(client, source_address, target_address)` and `close_bridge(client, target_address)` send the corresponding bridge frames. The acknowledged forms are internal, broker-to-broker halves of the handshake described in [BROKER.md](BROKER.md) section 6.
