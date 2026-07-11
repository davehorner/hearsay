# Broker

The broker is the message router: it accepts peer connections, maintains the subscription table, delivers publishes to subscribers, answers introspection requests, and manages bridges to other brokers. Source lives in `src/broker.rs` with the bridge wrapper in `src/bridge.rs`.

## 1. Startup

`start_broker(address)` resolves the address with standard host resolution (`resolve_address`, preferring an IPv4 result), so `localhost` binds the loopback interface and hostnames work. Listening on all interfaces requires the explicit `0.0.0.0:port`. The protocol carries no authentication, so the bind address is the security boundary: local tools should bind loopback, and `0.0.0.0` belongs on trusted networks only (any peer that can reach the port can subscribe to every topic, publish, and request bridges).

The listener itself is built with socket2 (`create_listener`): `SO_REUSEADDR`, keepalive (30s initial probe, 1s interval, 3 retries where the platform supports it), and a backlog of 1024. The configured socket is converted to a nonblocking std listener and then into a tokio `TcpListener`.

Two tasks are spawned: `broker_loop` (the event loop) and `accept_loop` (which spawns a `connection_task` per accepted stream). `start_broker` returns a `Broker` holding the event-channel sender and a shutdown watch sender; `broker_is_running` reports whether the loop is still alive by checking the event sender. With the `spawn` feature, the `Broker` also carries the process spawner (see [SPAWN.md](SPAWN.md)).

The broker stops in two ways, with identical effect: `stop_broker(&broker)` sends the shutdown signal explicitly, and dropping the `Broker` drops the watch sender, which the loops observe the same way. Both accept loops (TCP and WebSocket) and the event loop exit, peer writer channels close, sockets shut, and connected clients observe the disconnect. Hold the `Broker` for as long as the bus should run.

## 2. Connection tasks

`connection_task` owns the read half of a peer's TCP stream and parks the write half in an `Option<PeerWriter>`. It reads length-prefixed frames through a reusable buffer (`FrameReader`), parses each header into byte ranges (`FrameView`, no copies), and hands each frame to `process_incoming_frame`:

- A `Hello` frame takes the parked writer, creates a oneshot shutdown channel, forwards `BrokerEvent::Hello` to the event loop, and waits on a second oneshot for the assigned `PeerId`. The task then holds its identity — the `PeerId` and the peer's name as an `Arc<str>` — for the life of the connection. A second `Hello` on the same socket is ignored (the writer was already taken).
- Every other frame forwards as `BrokerEvent::Frame { peer, name, frame, view }`. Stamping the name on the event (a refcount bump, not an allocation) means frame handling never has to resolve identity from mutable broker state, so a frame racing its own connection's disconnect still carries correct identity. Frames arriving before `Hello` are dropped.

When the read side dies, the task drops its shutdown sender. That resolves the oneshot held by the peer's writer task, which exits and reports the disconnect.

`PeerWriter` is the transport enum: `Tcp(OwnedWriteHalf)` or, with the `websockets` feature, `WebSocket(SplitSink<...>)`. Frames already carry their length prefix, so the TCP variant writes them as-is — vectored, in batches — and the WebSocket variant sends each frame as one binary message.

## 3. The event loop

`broker_loop` owns all broker state as one plain struct, `BrokerState`:

- `peers: Arena<PeerSlot>` holds each peer's name and its two bounded writer channels (control and data), addressed by `PeerId` (slot index plus generation). `peer_names` maps name to `PeerId` for `Hello` replacement.
- `topic_index: HashMap<Box<str>, u32>` interns topic names to slots in `topics: Vec<Option<TopicState>>`; each topic's subscribers are a `SmallVec<[PeerId; 4]>`. Matching is exact string equality; there are no wildcards. A topic's slot is released to a free list when its last subscriber leaves, so the table cannot accumulate empty entries.
- `origin_progress` implements cross-broker deduplication (section 6).
- `bridges: BridgeRegistry`.
- `disconnect_sender`, cloned into every writer task.

The loop is a `tokio::select!` over three sources: the broker event channel, the disconnect channel, and the shutdown watch channel (section 1).

## 4. Peers, generations, and disconnects

`establish_peer` removes any existing peer with the same name, creates the new peer's control and data channels, inserts it into the arena (which assigns a fresh generation for the slot), and spawns `connection_writer_task` with the writer, the shutdown receiver, and the `PeerId`.

The writer task drains its channels in batches (control queue first, then data) and writes each batch with one vectored write, so bursts cost one syscall instead of one per frame. It exits on write failure, channel close, or the shutdown oneshot resolving (the read side died). Either way it ends by sending its `PeerId` on the disconnect channel.

`remove_disconnected_peer` removes by `PeerId`, and the arena's generation check makes stale reports harmless: when a client reconnects under the same name, the old slot is removed and the new peer occupies a slot under a different generation, so the old writer task's eventual disconnect report matches nothing.

## 5. Delivery

A publish frame is handled without deserializing or re-serializing the message:

1. The kind byte is patched from `Publish` to `Deliver` in place and the frame is frozen into a shared `Bytes` buffer.
2. For text publishes, `answer_introspection_requests` checks the topic against the `BrokerContract` request topics and publishes the matching report (section 7).
3. Unless the publish was local-only, a forward header is encoded once and paired with a slice of the same payload buffer, then handed to every bridge except the one matching the publisher (cycle prevention, section 6).
4. `deliver` hashes the topic once to find its interned id, then walks the subscriber `PeerId` list and clones the shared frame into each peer's queue — a refcount bump per subscriber, no payload copies. Peers no longer in the arena are skipped silently.

`Subscribe` and `Unsubscribe` only mutate the local subscription table. Subscriptions are deliberately not forwarded over bridges: cross-broker delivery is publish-driven (every non-local publish crosses every bridge), so forwarding subscriptions would only cause the remote broker to stream messages into a bridge client that never reads them.

## 6. Bridges

A `Bridge` (`src/bridge.rs`) is a `Client` connected to the remote broker's address, plus its peer id and the `target_address`, driven by a dedicated task that forwards frames and reconnects the client on a 2-second interval when its connection dies. Bridge clients use `autoreconnect: false` and a single connection attempt; the bridge task owns their reconnection.

The handshake is symmetric. A client asks its local broker to bridge by sending `OpenBridge { source_address, target_address, ack: false }`:

1. The local broker prunes any existing bridge to the same target (sending a removal acknowledgement over the old bridge if it was alive), creates a bridge client, and connects it to `target_address`.
2. Because `ack` is false, it then sends `OpenBridge { source_address: target, target_address: source, ack: true }` over the new bridge client, asking the remote broker to create the reverse bridge.
3. The remote broker creates its reverse bridge with the requesting peer's id as an override id, connects it back, and publishes `BrokerContract::BridgeCreated`.

The override id is the echo prevention: frames arriving from a bridge peer carry that connection's identity, and the matching reverse bridge has the same id, so the broker never republishes a message back across the bridge it arrived on. Full cycle and duplicate suppression across arbitrary topologies is handled by the forward envelope: each forwarded message carries the set of broker instance ids it has visited (a broker drops anything that already visited it) and a per-origin sequence number checked against a fixed-size reorder window per origin (a ring bitmap over the last 1024 sequences, with least-recently-seen origins evicted beyond 1024 tracked origins), so redundant mesh paths deliver each message at most once in bounded memory.

`close_bridge` sends `CloseBridge { ack: false }` with the target address; the broker tears down its bridge and the bridge notifies the remote side with the `ack: true` form, which the remote broker matches to its reverse bridge by the sending connection's identity.

Bridge health is checked by each bridge task on its 2-second tick using read-side liveness (`is_connected`): a bridge counts as dead when its writer is gone or its receiver channel has closed, which happens as soon as the bridge's read task observes the dead socket. Dead bridges are reconnected in place with a single attempt per tick.

## 7. Introspection topics

The broker answers three request topics, publishing the report globally (across bridges) as `BROKER_ID` (`"broker"`):

| Request topic | Report topic | Payload |
|---|---|---|
| `hearsay/subscriptions/request` | `hearsay/subscriptions/report` | the full topic-to-subscribers table |
| `hearsay/peers/request` | `hearsay/peers/report` | connected peer ids |
| `hearsay/bridges/request` | `hearsay/bridges/report` | bridge ids |

It also announces `hearsay/peers/connected` (`PeerConnected { id }`) whenever a peer connects, and `hearsay/bridges/created` (`BridgeCreated`) when an acknowledged bridge comes up. All five are defined by `BrokerContract` in `src/contract.rs`.
