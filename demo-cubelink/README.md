# cubelink

A two-player game on the [nightshade](https://crates.io/crates/nightshade-api) engine, networked with [hearsay](..)'s UDP transport.

Each player drives a cube around a large floor with a pan-orbit view (drag to orbit, scroll to zoom), and the two players chat over a retained-UI panel.

Networking is host-authoritative: guests send only their movement input, and the host owns every cube's position, applies the movement rules, and broadcasts the authoritative positions unreliably (the freshest wins). A guest cannot teleport or move faster than the rules allow, because it never sends a position. Chat is published reliably and in order. All traffic is encrypted with a shared passphrase, so only the two players can join or read the session.

## Play

### Both players on one computer (simplest)

One command launches both windows:

```
just play
```

It builds once and starts two instances as separate processes — a host (blue cube) and a guest (orange cube) — each in its own window.

> Don't launch two players with `cargo run` in two terminals: the two invocations share one target directory and serialize on cargo's build lock, so the second one blocks and you only see one window. `just play` (and `just host` / `just join`) run the compiled binary directly, which takes no lock. If you prefer two terminals, use `just host` in one and `just join` in another.

Only the host binds the ports; the guest is an ordinary UDP client on an ephemeral port, so the broker treats the two instances as distinct peers by source port — exactly as it would two separate computers. Start them in either order; the reliable handshake retransmits until the host is up.

### Two separate computers

On the host machine run `just host`. On the other machine, pass the host's LAN address:

```
just join 192.168.1.42      # or: cargo run -- join 192.168.1.42
```

The host binds every interface (`0.0.0.0:9501`), so a guest elsewhere on the network reaches it. (Allow the port through the host's firewall if prompted.)

## Controls

- **WASD** — move your cube (relative to the camera)
- **Mouse drag** — orbit the view; **scroll** — zoom
- **Chat panel** (bottom left) — type a message, then click **Send** or tick the **Send** checkbox to send it to the other player
- **Show log** checkbox — show or hide the chat log
- **Escape** — quit

## Layout

- `src/net.rs` — the networking seam: hearsay's UDP client and broker on a background tokio thread, bridged to the game loop with channels.
- `src/main.rs` — the game: floor, two cubes, pan-orbit camera, WASD movement, and the retained-UI chat panel.
