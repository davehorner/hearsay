use crate::{
    Error, Message, Result, decode_message,
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

impl UdpClient {
    /// Creates a UDP client and connects it to the broker's UDP listener at
    /// `broker_address`. The `Hello` handshake is sent reliably in the
    /// background; the returned client is usable immediately, with subscriptions
    /// and publishes queued behind the handshake in order.
    pub async fn connect(name: &str, broker_address: &str) -> Result<UdpClient> {
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
            stats.clone(),
        ));
        Ok(UdpClient {
            id,
            commands: command_sender,
            inbound: Mutex::new(inbound_receiver),
            stats,
        })
    }

    /// The client's full broker-side identity, including its random suffix.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The smoothed round-trip time to the broker in milliseconds, or `None`
    /// until the first reliable message has been acknowledged.
    pub fn rtt_millis(&self) -> Option<u32> {
        match self.stats.rtt_millis.load(Ordering::Relaxed) {
            UNKNOWN => None,
            rtt => Some(rtt),
        }
    }

    /// The round-trip jitter estimate to the broker in milliseconds.
    pub fn jitter_millis(&self) -> u32 {
        self.stats.jitter_millis.load(Ordering::Relaxed)
    }

    /// Subscribes to each of `topics`. Literal names and MQTT-style `+`/`#`
    /// patterns are both accepted, matched against the same subscription table
    /// as TCP and WebSocket peers.
    pub async fn subscribe(&self, topics: &[impl AsRef<str>]) -> Result<()> {
        for topic in topics {
            self.commands
                .send(Command::Subscribe(topic.as_ref().to_string()))
                .await
                .map_err(|_| Error::NotConnected)?;
        }
        Ok(())
    }

    /// Unsubscribes from each of `topics`.
    pub async fn unsubscribe(&self, topics: &[impl AsRef<str>]) -> Result<()> {
        for topic in topics {
            self.commands
                .send(Command::Unsubscribe(topic.as_ref().to_string()))
                .await
                .map_err(|_| Error::NotConnected)?;
        }
        Ok(())
    }

    /// Publishes `payload` to `topic` as postcard binary with the chosen
    /// [`Delivery`] guarantee. This is the default typed path; subscribers decode
    /// it with [`Message::read`]. Use [`UdpClient::publish_json`] for a
    /// self-describing encoding.
    pub async fn publish(
        &self,
        topic: impl AsRef<str>,
        payload: &impl Serialize,
        delivery: Delivery,
    ) -> Result<()> {
        let bytes = postcard::to_allocvec(payload)?;
        let frame = encode_publish_binary(topic.as_ref(), &bytes, false)?;
        self.commands
            .send(Command::Publish { frame, delivery })
            .await
            .map_err(|_| Error::NotConnected)
    }

    /// Publishes `payload` to `topic` as JSON text with the chosen [`Delivery`]
    /// guarantee, the second-class counterpart to [`UdpClient::publish`].
    pub async fn publish_json(
        &self,
        topic: impl AsRef<str>,
        payload: &impl Serialize,
        delivery: Delivery,
    ) -> Result<()> {
        let json = serde_json::to_string(payload)?;
        let frame = encode_publish_text(topic.as_ref(), &json, false)?;
        self.commands
            .send(Command::Publish { frame, delivery })
            .await
            .map_err(|_| Error::NotConnected)
    }

    /// Publishes raw `payload` bytes to `topic` with the chosen [`Delivery`]
    /// guarantee.
    pub async fn publish_bytes(
        &self,
        topic: impl AsRef<str>,
        payload: &[u8],
        delivery: Delivery,
    ) -> Result<()> {
        let frame = encode_publish_binary(topic.as_ref(), payload, false)?;
        self.commands
            .send(Command::Publish { frame, delivery })
            .await
            .map_err(|_| Error::NotConnected)
    }

    /// Waits for and returns the next message, or `None` once the client's
    /// receive task has stopped.
    pub async fn next_message(&self) -> Option<Message> {
        self.inbound.lock().await.recv().await
    }
}

async fn udp_client_task(
    socket: UdpSocket,
    id: Box<str>,
    mut commands: Receiver<Command>,
    inbound: Sender<Message>,
    stats: Arc<Stats>,
) {
    let mut endpoint = Endpoint::new();
    let started = Instant::now();
    if let Ok(hello) = encode_hello(&id) {
        endpoint.send(Delivery::ReliableOrdered, &hello);
    }
    flush(&socket, &mut endpoint, started).await;

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
                endpoint.receive(now_millis, &buffer[..length], &mut delivered);
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
                flush(&socket, &mut endpoint, started).await;
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
                flush(&socket, &mut endpoint, started).await;
            }
            _ = retransmit.tick() => {
                flush(&socket, &mut endpoint, started).await;
            }
        }
    }
}

async fn flush(socket: &UdpSocket, endpoint: &mut Endpoint, started: Instant) {
    let now_millis = started.elapsed().as_millis() as u64;
    let mut datagrams: Vec<Bytes> = Vec::new();
    endpoint.drain_outgoing(now_millis, &mut datagrams);
    for datagram in datagrams {
        let _ = socket.send(&datagram).await;
    }
}
