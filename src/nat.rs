//! Experimental UDP hole-punching rendezvous.
//!
//! Two peers behind separate NATs cannot address each other directly, but each
//! can reach a public rendezvous. The rendezvous observes each peer's public
//! source address and tells the other peer about it; both then send to that
//! address at once to open their NAT mappings, after which they exchange
//! datagrams directly.
//!
//! This is unverified against real NATs — it cannot be, without two machines
//! behind two NATs. What the test validates on a single machine is the
//! coordination path: registration, observed-address exchange, and a direct
//! datagram once each side knows the other. Treat direct traversal as
//! experimental and fall back to the broker as a public relay when it fails.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, time::Duration};
use tokio::{net::UdpSocket, sync::watch};

const PUNCH_BURST: usize = 10;
const PUNCH_INTERVAL: Duration = Duration::from_millis(50);
const REGISTER_RETRY: Duration = Duration::from_millis(200);
const PEER_WAIT: Duration = Duration::from_secs(10);

#[derive(Serialize, Deserialize)]
enum RendezvousMessage {
    Register { session: u64 },
    Peer { address: SocketAddr },
}

/// A running rendezvous server. Dropping it stops the server.
pub struct Rendezvous {
    address: SocketAddr,
    shutdown: watch::Sender<bool>,
}

impl Rendezvous {
    /// Starts a rendezvous server that pairs peers by session id, telling each
    /// the other's observed public address. Dropping the returned value stops
    /// the server.
    pub async fn start(address: &str) -> Result<Rendezvous> {
        let socket = UdpSocket::bind(address).await?;
        let bound = socket.local_addr()?;
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        tokio::spawn(rendezvous_loop(socket, shutdown_receiver));
        Ok(Rendezvous {
            address: bound,
            shutdown: shutdown_sender,
        })
    }

    /// The address peers should register with.
    pub fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for Rendezvous {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

async fn rendezvous_loop(socket: UdpSocket, mut shutdown: watch::Receiver<bool>) {
    let mut waiting: HashMap<u64, SocketAddr> = HashMap::new();
    let mut buffer = vec![0_u8; 1024];
    loop {
        tokio::select! {
            received = socket.recv_from(&mut buffer) => {
                let Ok((length, from)) = received else {
                    continue;
                };
                let Ok(RendezvousMessage::Register { session }) =
                    postcard::from_bytes::<RendezvousMessage>(&buffer[..length])
                else {
                    continue;
                };
                match waiting.remove(&session) {
                    Some(first) if first != from => {
                        send_peer(&socket, first, from).await;
                        send_peer(&socket, from, first).await;
                    }
                    _ => {
                        waiting.insert(session, from);
                    }
                }
            }
            _ = shutdown.changed() => break,
        }
    }
}

async fn send_peer(socket: &UdpSocket, to: SocketAddr, peer: SocketAddr) {
    if let Ok(bytes) = postcard::to_allocvec(&RendezvousMessage::Peer { address: peer }) {
        let _ = socket.send_to(&bytes, to).await;
    }
}

/// Registers `socket` with the rendezvous under `session`, waits for the other
/// peer's observed address, punches a burst of datagrams at it, and returns it
/// for direct communication. Both peers must call this with the same session.
pub async fn punch(socket: &UdpSocket, rendezvous: SocketAddr, session: u64) -> Result<SocketAddr> {
    let register = postcard::to_allocvec(&RendezvousMessage::Register { session })?;
    let mut buffer = vec![0_u8; 1024];

    let peer = loop {
        socket.send_to(&register, rendezvous).await?;
        match tokio::time::timeout(REGISTER_RETRY, socket.recv_from(&mut buffer)).await {
            Ok(Ok((length, from))) if from == rendezvous => {
                if let Ok(RendezvousMessage::Peer { address }) =
                    postcard::from_bytes::<RendezvousMessage>(&buffer[..length])
                {
                    break address;
                }
            }
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {}
        }
    };

    for _ in 0..PUNCH_BURST {
        let _ = socket.send_to(&[], peer).await;
        tokio::time::sleep(PUNCH_INTERVAL).await;
    }
    Ok(peer)
}

/// Like [`punch`], but gives up after a fixed wait instead of retrying
/// registration forever.
pub async fn punch_with_timeout(
    socket: &UdpSocket,
    rendezvous: SocketAddr,
    session: u64,
) -> Result<SocketAddr> {
    tokio::time::timeout(PEER_WAIT, punch(socket, rendezvous, session))
        .await
        .map_err(|_| Error::Transport("rendezvous timed out".to_string()))?
}
