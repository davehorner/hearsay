# Contracts and the Wire Protocol

This document covers the wire protocol (the hand-rolled frame format) and the contract pattern applications use to define their own strongly typed messages. Source lives in `src/wire.rs` and `src/contract.rs`.

## 1. Framing

Every frame — on TCP, WebSocket, and UDP alike — is:

```
[u32 length][u8 kind][kind-specific fields...]
```

All integers are big-endian. `length` counts every byte after the prefix. Strings are `[u16 length][bytes]`, UTF-8. The frame kinds:

| Kind | Value | Direction | Fields after the kind byte |
|---|---|---|---|
| Hello | 1 | peer to broker | id string |
| Subscribe | 2 | peer to broker | topic string |
| Unsubscribe | 3 | peer to broker | topic string |
| Publish | 4 | peer to broker | `u8` flags, topic string, payload (rest of frame) |
| Deliver | 5 | broker to peer | `u8` flags, topic string, payload (rest of frame) |
| Forward | 6 | broker to broker | `u8` flags, `u64` sequence, `u8` hop count, hop count × `u128` broker ids, topic string, payload |
| OpenBridge | 7 | peer to broker | `u8` ack, source address string, target address string |
| CloseBridge | 8 | peer to broker | `u8` ack, target address string |

Flags: bit 0 marks a binary payload (unset means UTF-8 text), bit 1 marks a local-only publish (`Route::Local`).

Publish and Deliver share a layout deliberately: the broker turns a publish into a delivery by patching the single kind byte in place, so the buffer a publisher encodes is byte-for-byte the buffer every subscriber receives. Delivery fan-out shares that one reference-counted buffer; payload bytes are never copied inside the broker.

A peer's identity crosses the wire exactly once, in `Hello`; after that the connection is the identity. Frames sent before `Hello` are dropped.

Frames larger than 64 MiB are rejected on both encode and decode, so a corrupt length prefix cannot trigger an unbounded allocation. A malformed or oversized frame kills the connection, which the reconnection machinery then handles.

## 2. Messages

Subscribers receive [`Message`], which is a view over the received `Deliver` frame: `topic()` returns the topic, `text()` the payload for text publishes, and `bytes()` the payload for binary publishes. Cloning a `Message` is cheap (the frame buffer is reference-counted).

`Route` (`Global`, `Local`) is a client-side concept; only the local-only flag derived from it crosses the wire. See [CLIENT.md](CLIENT.md) section 3.

Clients that implement the protocol directly (WebSocket and WASM peers) build frames with the public helpers instead of hand-assembling bytes: `encode_hello`, `encode_subscribe`, `encode_unsubscribe`, `encode_publish_text`, `encode_publish_binary`, and `decode_message`. These helpers, `Message`, and `read_batch` compile on `wasm32`, so a browser crate depends on hearsay itself for the wire format instead of mirroring it.

## 3. The contract pattern

Applications define their messages as enums with the [enum2contract](https://github.com/matthewjberger/enum2contract) derive. hearsay re-exports the crate, so importing `hearsay::enum2contract` is all an application needs; no direct enum2contract dependency is required. Each variant declares its topic, and the macro generates a payload struct, a topic constructor, and a message constructor per variant:

```rust
use hearsay::enum2contract::{self, EnumContract};
use serde::{Deserialize, Serialize};

#[derive(Debug, EnumContract, Serialize, Deserialize, Default)]
pub enum PingPongContract {
    #[topic("pingpong/command-{channel}")]
    Command { command: PingPongCommand },

    #[topic("pingpong/event-{channel}")]
    Event { event: PingPongEvent },

    #[topic("")]
    #[default]
    #[serde(other)]
    Empty,
}
```

Generated per variant:

- `CommandPayload { command: PingPongCommand }` with `to_json()` and `from_json()`.
- `PingPongContract::command_topic(channel: &str) -> String`, substituting each `{placeholder}`.
- `PingPongContract::command(channel: &str) -> (String, CommandPayload)`, returning the topic and a default payload ready to mutate.

The conventions:

- A `Command { command: XCommand }` variant for action requests and an `Event { event: XEvent }` variant for things that happened, with the inner types as plain enums.
- An `Empty` variant with `#[topic("")]`, `#[default]`, and `#[serde(other)]` on both the contract and the inner enums, so unknown variants deserialize to a harmless catch-all instead of failing when peers run different versions.

## 4. Publishing and receiving with a contract

Publish with the generated constructors:

```rust
let (topic, mut payload) = PingPongContract::command("broadcast");
payload.command = PingPongCommand::Ping;
hearsay::publish(&client, &topic, &payload, hearsay::Route::Global).await?;
```

Receive by matching the topic and deserializing the payload struct:

```rust
match message.topic() {
    topic if PingPongContract::command_topic("broadcast") == topic => {
        if let Ok(payload) = CommandPayload::from_json(message.text().unwrap_or_default()) {
            // handle payload.command
        }
    }
    _ => {}
}
```

## 5. The broker's own contract

`BrokerContract` in `src/contract.rs` is the one contract the library defines, covering broker introspection and announcements: `RequestSubscriptions`/`ReportSubscriptions`, `RequestPeers`/`ReportPeers`, `RequestBridges`/`ReportBridges`, `PeerConnected`, and `BridgeCreated`. Its generated payload types (`ReportPeersPayload`, `BridgeCreatedPayload`, and so on) are re-exported so applications can subscribe to and decode these topics. Application contracts are deliberately out of scope for the library; see [ARCHITECTURE.md](ARCHITECTURE.md) section 3.
