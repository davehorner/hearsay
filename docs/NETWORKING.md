# Networking

hearsay is a transport-agnostic message bus. Its job is to move typed messages between peers reliably enough, cheaply enough, and with enough delivery control that a higher layer can build real-time networked applications on top of it — including game netcode. This document describes how hearsay serves as that networking substrate: the transports it offers, the UDP transport built for latency-sensitive traffic, and the delivery, authentication, and timing primitives that round out the substrate.

hearsay deliberately stops at the substrate. It owns transport, framing, routing, delivery semantics, authentication, and time synchronization. It does **not** own replication, client-side prediction, interpolation, or rollback: those are coupled to a specific application's state model, so they belong in the layer above. hearsay gives that layer the primitives — unreliable and reliable channels, authenticated peers, an authoritative clock, sequence and acknowledgement information — and stays out of the world model.

## 1. Transports

A peer is a peer regardless of how its bytes arrive. All transports carry the same [wire frames](CONTRACTS.md) and feed the same broker event loop and the same subscription table, so peers on different transports interoperate with no translation.

| Transport | Feature | Delivery | Use |
|---|---|---|---|
| TCP | (default) | reliable, ordered stream | coordination, control, anything that must not be lost |
| WebSocket | `websockets` | reliable, ordered | browser and WASM peers |
| UDP | `udp` | unreliable, unordered datagrams | latency-sensitive state where the freshest message matters more than every message |

The transports are additive. A single broker can run TCP, WebSocket, and UDP listeners at once, and a peer chooses the transport that fits its traffic.

## 2. The UDP transport

```rust
let broker = hearsay::start_broker("127.0.0.1:9612").await?;
hearsay::start_udp_listener(&broker, "127.0.0.1:9613").await?;
```

Over a stream transport a connection is a socket. UDP has no connections, so hearsay models each source address as a **virtual connection**: one listener socket demultiplexes every peer by its remote address, each with its own reliability endpoint. A UDP peer is driven through the same connection state machine as a TCP peer — it sends the same frames, beginning with `Hello` — but those frames travel inside the reliability protocol described in section 3 rather than one-to-one with datagrams.

```rust
let client = hearsay::create_udp_client("player", "127.0.0.1:9613").await?;
hearsay::udp_subscribe(&client, &["world/state"]).await?;

// choose a guarantee per publish
hearsay::udp_publish(&client, "input", &command, hearsay::Delivery::Unreliable).await?;
hearsay::udp_publish(&client, "chat", &line, hearsay::Delivery::ReliableOrdered).await?;

if let Some(message) = hearsay::next_udp_message(&client).await {
    // ...
}
```

- **Each peer runs a reliability endpoint.** A datagram is a reliability packet (sequence, acknowledgements, one message or fragment), not a bare frame. The `Hello` handshake and subscriptions are sent reliably and ordered; publishes carry whatever [`Delivery`](#3-delivery-classes) the caller chose. The broker delivers its coordination frames (`hearsay/` topics) reliably and application data unreliably, so control always arrives while high-frequency traffic stays best-effort.
- **A malformed datagram does not disconnect the peer.** A datagram that fails to parse is dropped and the peer survives, because an unreliable transport must tolerate corruption of individual datagrams.
- **Liveness is timeout-based.** With no connection to close, a peer is dropped when no datagram has arrived from its address for the idle timeout, and a peer that never completes its `Hello` is dropped sooner. A peer that only subscribes and never publishes must send periodic traffic to stay registered; a continuous sender never notices, and the reliability layer's own acknowledgement traffic counts.
- **Large reliable messages fragment and reassemble.** A reliable message larger than one datagram is split into fragments, each retransmitted until acknowledged, and reassembled in order at the receiver.

Because the frame format is identical across transports, once a UDP peer's endpoint has reassembled a frame the broker routes it through exactly the same path as a TCP or WebSocket frame, sharing one subscription table and one reference-counted delivery buffer.

### Identity caveat

A UDP peer's identity is its source address. If a peer's address changes mid-session (NAT rebinding) it appears as a new peer, and if an address is reused within the idle-timeout window a new peer inheriting that address is misattributed to the old one. Address-independent identity is provided by the authentication layer below; until then, UDP identity is address-bound.

## 3. Delivery classes

Every UDP publish chooses a [`Delivery`], and control frames are always reliable and ordered:

- **`Unreliable`** — fire and forget. A lost datagram is gone; a reordered one arrives out of order. For high-frequency state (positions, transforms, inputs) where the next message supersedes this one and head-of-line blocking would be worse than loss.
- **`UnreliableSequenced`** — like unreliable, but the receiver drops anything older than the newest it has seen, so stale state never arrives after fresh state.
- **`ReliableOrdered`** — retransmitted until acknowledged and delivered in send order, with fragmentation for messages larger than one datagram.

Reliability is layered on the datagram core with per-packet sequence numbers, acknowledgement bitfields, retransmission timers, ordered reassembly, and duplicate suppression — the standard construction for reliable messaging over UDP. One connection carries unreliable state and reliable events side by side: a position stream goes `Unreliable`, a "player joined" event goes `ReliableOrdered`, and neither blocks the other. The reliability endpoint is transport-agnostic and is verified against simulated packet loss, reordering, and duplication rather than only over loopback.

## 4. Authentication and authorization (planned)

The substrate is designed for untrusted peers from the start. The connection handshake becomes a token-and-challenge exchange that establishes a per-peer identity the broker trusts, independent of transport address, which also resolves the UDP identity caveat above. On top of identity, per-topic authorization decides which topics a peer may publish to and subscribe from, so a client can only affect what it is permitted to. Per-packet encryption protects confidentiality and integrity on the wire. Until this lands, the bind address remains the security boundary, as it is for the existing transports.

## 5. Time and tick primitives (planned)

Prediction, interpolation, and rollback all need a shared sense of time and a handle on in-flight messages. hearsay supplies the raw material: a broker-authoritative tick clock, round-trip and jitter estimates per peer, and the sequence and acknowledgement state the delivery layer already tracks, surfaced to the application. hearsay provides the clock and the numbers; the layer above decides how to predict, interpolate, and reconcile against them.

## 6. The boundary

To keep hearsay reusable and uncoupled from any application's state model, the line is fixed:

| hearsay owns | the layer above owns |
|---|---|
| transports and framing | entity and component replication |
| routing and subscriptions | client-side prediction |
| delivery classes (reliability, ordering) | interpolation and smoothing |
| authentication and authorization | rollback and reconciliation |
| the authoritative clock and RTT/jitter | the world model and game rules |

Everything hearsay provides is expressed in terms of topics, messages, peers, and time — never entities, components, or frames of simulation. That boundary is what lets the same bus coordinate windows of a desktop application and carry the netcode of a networked one without knowing the difference.
