use crate::{
    Result,
    arena::PeerId,
    broker::{Broker, BrokerEvent, PeerWriter, process_incoming_frame, resolve_address},
    wire::{FrameView, parse_frame},
};
use bytes::BytesMut;
use std::{
    collections::{HashMap, hash_map::Entry},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc::Sender, oneshot, watch},
    time::Instant,
};

/// The largest datagram body hearsay will send over UDP. A frame whose
/// contiguous encoding exceeds this is dropped rather than sent, since a
/// single UDP datagram cannot exceed the protocol's own ceiling and this
/// transport does not yet fragment. Reliable, fragmented delivery is a
/// planned channel type; until then UDP carries best-effort traffic that
/// fits one datagram.
pub(crate) const MAX_DATAGRAM_PAYLOAD: usize = 65_507;

const RECEIVE_BUFFER: usize = 65_536;
const PEER_TIMEOUT: Duration = Duration::from_secs(15);
const SWEEP_INTERVAL: Duration = Duration::from_secs(5);

/// Starts a UDP transport on `address`, so peers can join the broker's bus
/// over UDP alongside its TCP and WebSocket peers, sharing one subscription
/// table. A UDP peer is a virtual connection keyed by its source address: it
/// sends the same wire frames as any other peer, beginning with `Hello`.
///
/// Delivery over this transport is unreliable and unordered (one frame per
/// datagram, best-effort). It is the foundation for the reliability and
/// ordering channels described in the networking documentation, not a
/// drop-in replacement for the TCP client's guarantees.
pub async fn start_udp_listener(broker: &Broker, address: &str) -> Result<()> {
    let socket = Arc::new(UdpSocket::bind(resolve_address(address).await?).await?);
    tokio::spawn(udp_listen_loop(
        socket,
        broker.sender.clone(),
        broker.shutdown_sender.subscribe(),
    ));
    Ok(())
}

struct UdpPeer {
    writer: Option<PeerWriter>,
    peer: Option<(PeerId, Arc<str>)>,
    shutdown_signal: Option<oneshot::Sender<()>>,
    last_seen: Instant,
}

async fn udp_listen_loop(
    socket: Arc<UdpSocket>,
    event_sender: Sender<BrokerEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut peers: HashMap<SocketAddr, UdpPeer> = HashMap::new();
    let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
    let mut scratch = vec![0_u8; RECEIVE_BUFFER];
    loop {
        tokio::select! {
            received = socket.recv_from(&mut scratch) => {
                let Ok((length, address)) = received else {
                    continue;
                };
                // Copy out exactly the datagram's bytes: the broker shares this
                // buffer with every subscriber, so sizing it to the message
                // rather than the receive scratch avoids pinning the full
                // receive buffer per in-flight message.
                let mut datagram = BytesMut::with_capacity(length);
                datagram.extend_from_slice(&scratch[..length]);
                receive_datagram(&socket, &event_sender, &mut peers, address, datagram).await;
            }
            _ = sweep.tick() => {
                expire_idle_peers(&mut peers);
            }
            _ = shutdown.changed() => break,
        }
    }
}

async fn receive_datagram(
    socket: &Arc<UdpSocket>,
    event_sender: &Sender<BrokerEvent>,
    peers: &mut HashMap<SocketAddr, UdpPeer>,
    address: SocketAddr,
    datagram: BytesMut,
) {
    let slot = match peers.entry(address) {
        Entry::Occupied(occupied) => occupied.into_mut(),
        Entry::Vacant(vacant) => {
            // Only a Hello brings a new address onto the bus; unsolicited
            // datagrams never allocate a peer slot, so a flood of spoofed
            // sources cannot grow the table.
            if !matches!(parse_frame(&datagram), Some(FrameView::Hello { .. })) {
                return;
            }
            vacant.insert(UdpPeer {
                writer: Some(PeerWriter::Udp {
                    socket: socket.clone(),
                    address,
                }),
                peer: None,
                shutdown_signal: None,
                last_seen: Instant::now(),
            })
        }
    };
    slot.last_seen = Instant::now();
    // The return distinguishes "malformed frame" from "peer still live", but
    // over an unreliable transport a single bad datagram must not tear down a
    // healthy peer: it is dropped and the peer survives. Real death is handled
    // by the idle sweep and by the broker's own disconnect path.
    let _ = process_incoming_frame(
        datagram,
        event_sender,
        &mut slot.writer,
        &mut slot.peer,
        &mut slot.shutdown_signal,
    )
    .await;
}

fn expire_idle_peers(peers: &mut HashMap<SocketAddr, UdpPeer>) {
    let now = Instant::now();
    peers.retain(|_address, slot| now.duration_since(slot.last_seen) < PEER_TIMEOUT);
}
