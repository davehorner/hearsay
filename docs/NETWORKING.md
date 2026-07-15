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

Over a stream transport a connection is a socket. UDP has no connections, so hearsay models each source address as a **virtual connection**: one listener socket demultiplexes every peer by its remote address. A UDP peer is driven through exactly the same connection state machine as a TCP peer — it sends the same frames, beginning with `Hello`.

- **Registration is Hello-gated.** The first datagram from an address must be a `Hello`; any other datagram from an unknown address is dropped without allocating state. A flood of spoofed source addresses therefore cannot grow the peer table.
- **One frame per datagram, unreliable and unordered.** A published message is one datagram, sent best-effort. There is no retransmission, no acknowledgement, no ordering, and no de-duplication. A lost datagram is simply gone; a reordered one arrives out of order. This is the correct shape for high-frequency state (positions, transforms, inputs) where a dropped update is superseded by the next one and head-of-line blocking would be worse than loss.
- **A malformed datagram does not disconnect the peer.** Over a stream, a frame that fails to parse means the stream is corrupt and the connection dies. Over UDP a single bad datagram is dropped and the peer survives, because unreliable delivery must tolerate corruption of individual datagrams.
- **Liveness is timeout-based.** With no connection to close, a peer is considered gone when no datagram has arrived from its address for the idle timeout, at which point the broker tears it down exactly as it would a dropped TCP peer. A peer that only subscribes and never publishes must therefore send periodic traffic (a keepalive) to stay registered; continuous senders never notice.
- **Datagrams are size-capped.** A frame whose encoding would exceed one datagram is dropped rather than sent. Application-level fragmentation for large reliable messages is part of the reliability work below, not this layer.

Because the frame format is identical across transports, the broker fans a single reference-counted buffer out to TCP, WebSocket, and UDP subscribers alike; the UDP writer sends that buffer as a datagram with no re-encoding.

### Identity caveat

A UDP peer's identity is currently its source address. If a peer's address changes mid-session (NAT rebinding) it appears as a new peer, and if an address is reused within the idle-timeout window a new peer inheriting that address is misattributed to the old one. Address-independent identity is provided by the authentication layer below; until then, UDP identity is address-bound.

## 3. Delivery classes (planned)

The UDP transport today provides the unreliable class. The full substrate exposes a delivery class per message, chosen by the publisher:

- **Unreliable** — fire and forget. (Available now over UDP.)
- **Unreliable sequenced** — drop anything older than the newest seen; never deliver stale state.
- **Reliable** — retransmit until acknowledged; delivery guaranteed, order not.
- **Reliable ordered** — retransmit and deliver in order.

Reliability is layered on the unreliable datagram core with sequence numbers, acknowledgement bitfields, retransmission timers, and fragmentation for messages larger than one datagram — the standard construction for reliable messaging over UDP. This lets one connection carry unreliable state and reliable events side by side: a position stream goes unreliable, a "player joined" event goes reliable-ordered, and neither blocks the other.

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
