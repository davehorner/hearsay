use crate::{
    Error, Message, Result,
    crypto::Cipher,
    decode_message,
    reliability::{Delivery, Endpoint},
    wire::{
        encode_hello, encode_publish_binary, encode_publish_text, encode_subscribe,
        encode_unsubscribe,
    },
};
use bytes::Bytes;
use serde::Serialize;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{
        Mutex,
        mpsc::{self, Receiver, Sender},
    },
    time::Instant,
};

const RETRANSMIT_INTERVAL: Duration = Duration::from_millis(20);
const COMMAND_CAPACITY: usize = 1024;
const INBOUND_CAPACITY: usize = 1024;
const RECEIVE_BUFFER: usize = 65_536;
const UNKNOWN: u32 = u32::MAX;

/// A client that speaks hearsay over UDP with per-message delivery guarantees.
///
/// Unlike the TCP [`crate::Client`], every publish chooses a [`Delivery`]: a
/// position stream goes [`Delivery::Unreliable`], an event goes
/// [`Delivery::ReliableOrdered`]. Control frames (the handshake and
/// subscriptions) are always reliable and ordered.
pub struct UdpClient {
    id: Box<str>,
    commands: Sender<Command>,
    inbound: Mutex<Receiver<Message>>,
    stats: Arc<Stats>,
}

struct Stats {
    rtt_millis: AtomicU32,
    jitter_millis: AtomicU32,
}

enum Command {
    Subscribe(String),
    Unsubscribe(String),
    Publish { frame: Bytes, delivery: Delivery },
}

/// Creates a UDP client and connects it to the broker's UDP listener. The
/// `Hello` handshake is sent reliably in the background; the returned client is
/// usable immediately (subscriptions and publishes queue behind it in order).
pub async fn create_udp_client(name: &str, broker_address: &str) -> Result<UdpClient> {
    connect_udp_client(name, broker_address, None).await
}

/// Like [`create_udp_client`], but every datagram is encrypted with a key
/// derived from `passphrase`. A peer without the passphrase can neither read
/// this client's traffic nor forge a datagram the broker (started with
/// [`crate::start_udp_listener_secure`] and the same passphrase) will accept.
#[cfg(feature = "secure")]
pub async fn create_udp_client_secure(
    name: &str,
    broker_address: &str,
    passphrase: &str,
) -> Result<UdpClient> {
    connect_udp_client(
        name,
        broker_address,
        Some(Cipher::from_passphrase(passphrase)),
    )
    .await
}

async fn connect_udp_client(
    name: &str,
    broker_address: &str,
    cipher: Option<Cipher>,
) -> Result<UdpClient> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(broker_address).await?;
    let id: Box<str> = format!("{name}_{}", uuid::Uuid::new_v4()).into();
    let (command_sender, command_receiver) = mpsc::channel(COMMAND_CAPACITY);
    let (inbound_sender, inbound_receiver) = mpsc::channel(INBOUND_CAPACITY);
    let stats = Arc::new(Stats {
        rtt_millis: AtomicU32::new(UNKNOWN),
        jitter_millis: AtomicU32::new(0),
    });
    tokio::spawn(udp_client_task(
        socket,
        id.clone(),
        command_receiver,
        inbound_sender,
        cipher,
        stats.clone(),
    ));
    Ok(UdpClient {
        id,
        commands: command_sender,
        inbound: Mutex::new(inbound_receiver),
        stats,
    })
}

pub fn udp_client_id(client: &UdpClient) -> &str {
    &client.id
}

/// The smoothed round-trip time to the broker in milliseconds, or `None` until
/// the first reliable message has been acknowledged.
pub fn udp_rtt_millis(client: &UdpClient) -> Option<u32> {
    match client.stats.rtt_millis.load(Ordering::Relaxed) {
        UNKNOWN => None,
        rtt => Some(rtt),
    }
}

/// The round-trip jitter estimate to the broker in milliseconds.
pub fn udp_jitter_millis(client: &UdpClient) -> u32 {
    client.stats.jitter_millis.load(Ordering::Relaxed)
}

pub async fn udp_subscribe(client: &UdpClient, topics: &[impl AsRef<str>]) -> Result<()> {
    for topic in topics {
        client
            .commands
            .send(Command::Subscribe(topic.as_ref().to_string()))
            .await
            .map_err(|_| Error::NotConnected)?;
    }
    Ok(())
}

pub async fn udp_unsubscribe(client: &UdpClient, topics: &[impl AsRef<str>]) -> Result<()> {
    for topic in topics {
        client
            .commands
            .send(Command::Unsubscribe(topic.as_ref().to_string()))
            .await
            .map_err(|_| Error::NotConnected)?;
    }
    Ok(())
}

pub async fn udp_publish(
    client: &UdpClient,
    topic: impl AsRef<str>,
    payload: &impl Serialize,
    delivery: Delivery,
) -> Result<()> {
    let json = serde_json::to_string(payload)?;
    let frame = encode_publish_text(topic.as_ref(), &json, false)?;
    client
        .commands
        .send(Command::Publish { frame, delivery })
        .await
        .map_err(|_| Error::NotConnected)
}

pub async fn udp_publish_bytes(
    client: &UdpClient,
    topic: impl AsRef<str>,
    payload: &[u8],
    delivery: Delivery,
) -> Result<()> {
    let frame = encode_publish_binary(topic.as_ref(), payload, false)?;
    client
        .commands
        .send(Command::Publish { frame, delivery })
        .await
        .map_err(|_| Error::NotConnected)
}

pub async fn next_udp_message(client: &UdpClient) -> Option<Message> {
    client.inbound.lock().await.recv().await
}

async fn udp_client_task(
    socket: UdpSocket,
    id: Box<str>,
    mut commands: Receiver<Command>,
    inbound: Sender<Message>,
    cipher: Option<Cipher>,
    stats: Arc<Stats>,
) {
    let mut endpoint = Endpoint::new();
    let started = Instant::now();
    if let Ok(hello) = encode_hello(&id) {
        endpoint.send(Delivery::ReliableOrdered, &hello);
    }
    flush(&socket, &mut endpoint, started, &cipher).await;

    let mut retransmit = tokio::time::interval(RETRANSMIT_INTERVAL);
    let mut buffer = vec![0_u8; RECEIVE_BUFFER];
    loop {
        tokio::select! {
            received = socket.recv(&mut buffer) => {
                let Ok(length) = received else {
                    break;
                };
                let now_millis = started.elapsed().as_millis() as u64;
                let mut delivered: Vec<Bytes> = Vec::new();
                match &cipher {
                    Some(cipher) => {
                        if let Some(plaintext) = cipher.open(&buffer[..length]) {
                            endpoint.receive(now_millis, &plaintext, &mut delivered);
                        }
                    }
                    None => endpoint.receive(now_millis, &buffer[..length], &mut delivered),
                }
                if let Some(rtt) = endpoint.rtt_millis() {
                    stats.rtt_millis.store(rtt, Ordering::Relaxed);
                    stats.jitter_millis.store(endpoint.jitter_millis(), Ordering::Relaxed);
                }
                for frame in delivered {
                    if let Some(message) = decode_message(frame)
                        && inbound.send(message).await.is_err()
                    {
                        return;
                    }
                }
                flush(&socket, &mut endpoint, started, &cipher).await;
            }
            command = commands.recv() => {
                match command {
                    Some(Command::Subscribe(topic)) => {
                        if let Ok(frame) = encode_subscribe(&topic) {
                            endpoint.send(Delivery::ReliableOrdered, &frame);
                        }
                    }
                    Some(Command::Unsubscribe(topic)) => {
                        if let Ok(frame) = encode_unsubscribe(&topic) {
                            endpoint.send(Delivery::ReliableOrdered, &frame);
                        }
                    }
                    Some(Command::Publish { frame, delivery }) => {
                        endpoint.send(delivery, &frame);
                    }
                    None => break,
                }
                flush(&socket, &mut endpoint, started, &cipher).await;
            }
            _ = retransmit.tick() => {
                flush(&socket, &mut endpoint, started, &cipher).await;
            }
        }
    }
}

async fn flush(
    socket: &UdpSocket,
    endpoint: &mut Endpoint,
    started: Instant,
    cipher: &Option<Cipher>,
) {
    let now_millis = started.elapsed().as_millis() as u64;
    let mut datagrams: Vec<Bytes> = Vec::new();
    endpoint.drain_outgoing(now_millis, &mut datagrams);
    for datagram in datagrams {
        match cipher {
            Some(cipher) => {
                let _ = socket.send(&cipher.seal(&datagram)).await;
            }
            None => {
                let _ = socket.send(&datagram).await;
            }
        }
    }
}
