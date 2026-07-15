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

A UDP peer's identity is its source address. If a peer's address changes mid-session (NAT rebinding) it appears as a new peer, and if an address is reused within the idle-timeout window a new peer inheriting that address is misattributed to the old one. hearsay has no address-independent identity; UDP identity is address-bound.

## 3. Delivery classes

Every UDP publish chooses a [`Delivery`], and control frames are always reliable and ordered:

- **`Unreliable`** — fire and forget. A lost datagram is gone; a reordered one arrives out of order. For high-frequency state (positions, transforms, inputs) where the next message supersedes this one and head-of-line blocking would be worse than loss.
- **`UnreliableSequenced`** — like unreliable, but the receiver drops anything older than the newest it has seen, so stale state never arrives after fresh state.
- **`ReliableOrdered`** — retransmitted until acknowledged and delivered in send order, with fragmentation for messages larger than one datagram.

Reliability is layered on the datagram core with per-packet sequence numbers, acknowledgement bitfields, retransmission timers, ordered reassembly, and duplicate suppression — the standard construction for reliable messaging over UDP. One connection carries unreliable state and reliable events side by side: a position stream goes `Unreliable`, a "player joined" event goes `ReliableOrdered`, and neither blocks the other. The reliability endpoint is transport-agnostic and is verified against simulated packet loss, reordering, and duplication rather than only over loopback.

Reliable traffic is paced by additive-increase / multiplicative-decrease congestion control: the number of unacknowledged fragments in flight is bounded by a window that grows by one per acknowledgement and halves on a retransmission, so a lossy link is not flooded with retransmissions. This is verified by the loss simulation, which confirms the window backs off under loss while every reliable message still arrives in order.

## 4. Authentication and encryption

hearsay does not provide these. Anyone who can reach the transport can subscribe and publish, so the bind address is the security boundary — bind loopback for local use and only expose a broker on a network you trust. Confidentiality and per-identity authentication belong to a layer that has been designed and reviewed as cryptography, not composed ad hoc inside a message bus; run hearsay over a transport that already provides them (a VPN or overlay such as WireGuard or Tailscale, or a TLS tunnel) when you need them.

## 5. Time primitives

The reliability endpoint measures the smoothed round-trip time and jitter to its peer from acknowledgement timing, using Karn's algorithm so a retransmitted packet never skews the estimate. The UDP client exposes them as `udp_rtt_millis(&client)` (`None` until the first acknowledgement) and `udp_jitter_millis(&client)`. These are the raw measurements prediction, interpolation, and reconciliation build on; the tick clock and the policy that uses it stay in the layer above, coupled to the world model.

## 6. NAT traversal (feature `nat`, experimental)

Because clients connect outbound to the broker, hosting the broker on a publicly reachable machine already traverses NAT — that relay path is the recommended answer and needs no extra code. For direct peer-to-peer between two hosts behind separate NATs, the `nat` feature adds a rendezvous: `start_rendezvous(address)` pairs peers by session id and tells each the other's observed public address, and `punch(&socket, rendezvous, session)` registers, learns the peer's address, and sends a burst of datagrams to open the local NAT mapping.

This is **experimental and unverified against real NATs** — it cannot be tested without two machines behind two NATs. The test validates only the coordination path on a single machine (registration, observed-address exchange, and a direct datagram once each side knows the other). Treat direct traversal as best-effort and fall back to the broker as a public relay when it fails.

## 7. The boundary

To keep hearsay reusable and uncoupled from any application's state model, the line is fixed:

| hearsay owns | the layer above owns |
|---|---|
| transports and framing | entity and component replication |
| routing and subscriptions | client-side prediction |
| delivery classes (reliability, ordering) | interpolation and smoothing |
| congestion control | rollback and reconciliation |
| RTT and jitter measurement | the tick clock policy, world model, and game rules |

Everything hearsay provides is expressed in terms of topics, messages, peers, and time — never entities, components, or frames of simulation. That boundary is what lets the same bus coordinate windows of a desktop application and carry the netcode of a networked one without knowing the difference.
