use crate::{
    Error, Message, Result, Route, decode_message,
    wire::{
        FLAG_BINARY, Frame, FrameReader, MAX_WRITE_BATCH, begin_publish_frame, encode_close_bridge,
        encode_hello, encode_open_bridge, encode_subscribe, encode_unsubscribe, finish_frame,
        route_flags, write_frames,
    },
};
use arc_swap::ArcSwapOption;
use bytes::BufMut;
use serde::Serialize;
use socket2::TcpKeepalive;
use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{
        Mutex,
        mpsc::{self, Receiver, Sender, error::TryRecvError},
    },
};

const RECONNECTION_INTERVAL: Duration = Duration::from_secs(2);
const MAX_RECONNECTION_BACKOFF: Duration = Duration::from_secs(30);
const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const INBOUND_QUEUE_CAPACITY: usize = 1024;
const OUTBOUND_QUEUE_CAPACITY: usize = 1024;

/// How a [`Client`] establishes and maintains its connection to a broker.
///
/// The defaults are tuned for a long-lived client that expects the broker to be
/// available and wants to ride out transient outages: a bounded number of
/// initial attempts, then automatic background reconnection with exponential
/// backoff.
#[derive(Debug, Clone)]
pub struct ClientSettings {
    /// Number of additional connection attempts after the first failure before
    /// [`Client::connect`] gives up. `Some(0)` makes a single attempt; `Some(n)`
    /// retries up to `n` more times; `None` retries forever, so `connect` will
    /// not return until a broker is reachable.
    ///
    /// With `autoreconnect` enabled (the default) a small value is usually
    /// right: `connect` returning an error does not mean the client gives up,
    /// because the background reconnection task keeps trying regardless.
    pub max_connection_attempts: Option<u16>,
    /// Whether the client reconnects on its own after the connection drops,
    /// re-sending its subscriptions each time. Enabled by default.
    pub autoreconnect: bool,
    /// How long a single connection attempt may take before it is abandoned and
    /// retried.
    pub timeout_per_attempt: Duration,
    /// If set, the read half is torn down when no frame arrives within this
    /// window, which surfaces a silently dead connection as a disconnect.
    pub read_timeout: Option<Duration>,
}

impl Default for ClientSettings {
    fn default() -> Self {
        Self {
            max_connection_attempts: Some(10),
            autoreconnect: true,
            timeout_per_attempt: Duration::from_secs(2),
            read_timeout: None,
        }
    }
}

/// A connection to a broker over which messages are published and received.
///
/// A `Client` is a cheap, reference-counted handle: cloning it shares one
/// underlying connection, and every method takes `&self`, so the same client
/// can be shared freely across tasks. Create one with [`Client::new`], connect
/// it with [`Client::connect`], then [`Client::publish`],
/// [`Client::subscribe`], and [`Client::next_message`].
#[derive(Clone)]
pub struct Client {
    shared: Arc<ClientShared>,
}

struct ClientShared {
    id: Box<str>,
    settings: ClientSettings,
    connection: ArcSwapOption<Connection>,
    cold: Mutex<ColdState>,
}

struct Connection {
    outbound: Sender<Frame>,
    inbound: Mutex<Receiver<Message>>,
    alive: Arc<AtomicBool>,
}

impl Connection {
    fn is_live(&self) -> bool {
        self.alive.load(Ordering::Acquire) && !self.outbound.is_closed()
    }
}

struct ColdState {
    subscriptions: HashSet<String>,
    pending_subscriptions: HashSet<String>,
    broker_address: Option<String>,
    reconnection_task_spawned: bool,
}

impl Client {
    /// Creates a client identified by `name`. A random suffix is appended so
    /// two clients created with the same name still get distinct broker-side
    /// identities; use [`Client::id`] to read the full identity.
    pub fn new(name: &str, settings: ClientSettings) -> Client {
        Self::with_id(format!("{name}_{}", uuid::Uuid::new_v4()), settings)
    }

    pub(crate) fn with_id(id: String, settings: ClientSettings) -> Client {
        Client {
            shared: Arc::new(ClientShared {
                id: id.into(),
                settings,
                connection: ArcSwapOption::empty(),
                cold: Mutex::new(ColdState {
                    subscriptions: HashSet::new(),
                    pending_subscriptions: HashSet::new(),
                    broker_address: None,
                    reconnection_task_spawned: false,
                }),
            }),
        }
    }

    /// The client's full broker-side identity, including the random suffix
    /// added by [`Client::new`]. Two clients that register the same identity
    /// displace each other at the broker, so identities should be unique.
    pub fn id(&self) -> &str {
        &self.shared.id
    }

    /// Whether the client currently holds a live connection to the broker.
    ///
    /// This reflects the transport, not the presence of pending messages: a
    /// connected client with an empty queue still returns `true`.
    pub async fn is_connected(&self) -> bool {
        self.shared
            .connection
            .load_full()
            .is_some_and(|connection| connection.is_live())
    }

    /// The set of topics the client is currently subscribed to, including any
    /// subscriptions still pending because the client is not yet connected.
    pub async fn subscriptions(&self) -> HashSet<String> {
        let cold = self.shared.cold.lock().await;
        cold.subscriptions
            .iter()
            .chain(cold.pending_subscriptions.iter())
            .cloned()
            .collect()
    }

    /// Connects to a broker at `address`.
    ///
    /// With `autoreconnect` enabled the client also re-establishes the
    /// connection if it later drops and re-sends its subscriptions, but it does
    /// not replay messages published to those topics while it was disconnected;
    /// any such messages are lost. `address` accepts a host:port pair and is
    /// resolved fresh on every attempt, so a broker that moves to a new address
    /// behind the same name is reconnected to automatically.
    pub async fn connect(&self, address: &str) -> Result<()> {
        self.shared.cold.lock().await.broker_address = Some(address.to_string());
        establish_connection(&self.shared, address).await?;
        if self.shared.settings.autoreconnect {
            let mut cold = self.shared.cold.lock().await;
            if !cold.reconnection_task_spawned {
                cold.reconnection_task_spawned = true;
                tokio::spawn(reconnection_task(Arc::downgrade(&self.shared)));
            }
        }
        Ok(())
    }

    /// Publishes `payload` to `topic`, serialized with [postcard] into a compact
    /// binary frame. This is the default typed path.
    ///
    /// Binary is the default because it is the smallest and cheapest encoding
    /// for the common case of Rust services that share their message types.
    /// postcard is not self-describing, so both ends must agree on the type; when
    /// they cannot (a browser or WASM peer, or a consumer on a different schema
    /// version) reach for [`Client::publish_json`] instead. Subscribers decode
    /// this with [`Message::read`].
    ///
    /// The payload is serialized straight into the frame buffer, so there is no
    /// intermediate allocation. Delivery is best-effort: a subscriber that is
    /// disconnected or too far behind may never see the message. `route` selects
    /// whether the message crosses broker bridges ([`Route::Global`]) or stays on
    /// the local broker ([`Route::Local`]).
    ///
    /// [postcard]: https://docs.rs/postcard
    pub async fn publish(
        &self,
        topic: impl AsRef<str>,
        payload: &impl Serialize,
        route: Route,
    ) -> Result<()> {
        let mut buffer = begin_publish_frame(
            topic.as_ref(),
            FLAG_BINARY | route_flags(route == Route::Local),
        )?;
        postcard::to_io(payload, (&mut buffer).writer())?;
        let frame = finish_frame(buffer)?;
        self.send_frame(Frame::single(frame)).await
    }

    /// Publishes `payload` to `topic`, serialized as JSON text.
    ///
    /// JSON is the second-class path: prefer [`Client::publish`] (binary) for
    /// typed Rust-to-Rust traffic, and choose JSON when a peer needs a
    /// self-describing, schema-tolerant format, such as a browser or WASM client
    /// or a consumer on a different version of the types. Subscribers decode this
    /// with [`Message::read_json`], or read the raw string with [`Message::text`].
    pub async fn publish_json(
        &self,
        topic: impl AsRef<str>,
        payload: &impl Serialize,
        route: Route,
    ) -> Result<()> {
        let mut buffer = begin_publish_frame(topic.as_ref(), route_flags(route == Route::Local))?;
        serde_json::to_writer((&mut buffer).writer(), payload)?;
        let frame = finish_frame(buffer)?;
        self.send_frame(Frame::single(frame)).await
    }

    /// Publishes an already-serialized text `payload` to `topic` as a text
    /// frame, skipping serialization. Useful for forwarding a JSON string a tool
    /// or UI already produced; subscribers read it with [`Message::text`].
    pub async fn publish_text(
        &self,
        topic: impl AsRef<str>,
        payload: &str,
        route: Route,
    ) -> Result<()> {
        let mut buffer = begin_publish_frame(topic.as_ref(), route_flags(route == Route::Local))?;
        buffer.put_slice(payload.as_bytes());
        let frame = finish_frame(buffer)?;
        self.send_frame(Frame::single(frame)).await
    }

    /// Publishes raw bytes to `topic` as a binary message, with no
    /// serialization applied. Subscribers read them back with [`Message::bytes`].
    pub async fn publish_bytes(
        &self,
        topic: impl AsRef<str>,
        payload: &[u8],
        route: Route,
    ) -> Result<()> {
        let mut buffer = begin_publish_frame(
            topic.as_ref(),
            FLAG_BINARY | route_flags(route == Route::Local),
        )?;
        buffer.put_slice(payload);
        let frame = finish_frame(buffer)?;
        self.send_frame(Frame::single(frame)).await
    }

    pub(crate) async fn publish_encoded(
        &self,
        topic: &str,
        payload: &[u8],
        binary: bool,
        route: Route,
    ) -> Result<()> {
        let flags = if binary { FLAG_BINARY } else { 0 } | route_flags(route == Route::Local);
        let mut buffer = begin_publish_frame(topic, flags)?;
        buffer.put_slice(payload);
        let frame = finish_frame(buffer)?;
        self.send_frame(Frame::single(frame)).await
    }

    /// Subscribes to each of `topics`.
    ///
    /// A topic may be a literal name or an MQTT-style pattern: a `+` segment
    /// matches exactly one topic level and a trailing `#` matches the remaining
    /// levels, so `sensors/+/temperature` and `sensors/#` both select many
    /// concrete topics. Subscriptions requested before the client is connected
    /// are remembered and applied on connect, and re-sent automatically after a
    /// reconnect.
    pub async fn subscribe(&self, topics: &[impl AsRef<str>]) -> Result<()> {
        for topic in topics {
            let topic = topic.as_ref();
            {
                let mut cold = self.shared.cold.lock().await;
                if self.shared.connection.load_full().is_none() {
                    cold.pending_subscriptions.insert(topic.to_string());
                    continue;
                }
            }
            let frame = Frame::single(encode_subscribe(topic)?);
            if let Err(error) = self.send_frame(frame).await {
                self.shared
                    .cold
                    .lock()
                    .await
                    .pending_subscriptions
                    .insert(topic.to_string());
                return Err(error);
            }
            self.shared
                .cold
                .lock()
                .await
                .subscriptions
                .insert(topic.to_string());
        }
        Ok(())
    }

    /// Unsubscribes from each of `topics`, cancelling both active and
    /// not-yet-applied subscriptions so a topic dropped while offline does not
    /// silently return on the next reconnect.
    pub async fn unsubscribe(&self, topics: &[impl AsRef<str>]) -> Result<()> {
        for topic in topics {
            let topic = topic.as_ref();
            let connected = {
                let mut cold = self.shared.cold.lock().await;
                cold.pending_subscriptions.remove(topic);
                cold.subscriptions.remove(topic);
                self.shared.connection.load_full().is_some()
            };
            if connected {
                self.send_frame(Frame::single(encode_unsubscribe(topic)?))
                    .await?;
            }
        }
        Ok(())
    }

    /// Returns the next message without waiting.
    ///
    /// Unlike [`Client::next_message`] this never blocks: it reports
    /// [`Reception::Empty`] when the queue is momentarily empty and
    /// [`Reception::Disconnected`] when the connection has dropped, letting a
    /// polling loop distinguish "nothing yet" from "nothing ever again".
    pub async fn try_next_message(&self) -> Reception {
        let Some(connection) = self.shared.connection.load_full() else {
            return Reception::Disconnected;
        };
        let mut receiver = connection.inbound.lock().await;
        match receiver.try_recv() {
            Ok(message) => Reception::Message(message),
            Err(TryRecvError::Disconnected) => {
                connection.alive.store(false, Ordering::Release);
                Reception::Disconnected
            }
            Err(TryRecvError::Empty) => Reception::Empty,
        }
    }

    /// Waits for and returns the next message, or `None` once the connection has
    /// dropped and its queue is drained.
    ///
    /// When `autoreconnect` is enabled a `None` here does not mean the client is
    /// finished: after the background task reconnects, calling this again yields
    /// messages from the new connection. A returned `None` therefore marks the
    /// end of one connection's stream, not the end of the client.
    pub async fn next_message(&self) -> Option<Message> {
        let connection = self.shared.connection.load_full()?;
        let mut receiver = connection.inbound.lock().await;
        match receiver.recv().await {
            Some(message) => Some(message),
            None => {
                connection.alive.store(false, Ordering::Release);
                None
            }
        }
    }

    /// Asks the broker to open a bridge from `source_address` to
    /// `target_address`, joining the two brokers' buses so globally-routed
    /// messages flow between them.
    pub async fn open_bridge(&self, source_address: &str, target_address: &str) -> Result<()> {
        self.open_bridge_acked(source_address, target_address, false)
            .await
    }

    pub(crate) async fn open_bridge_acked(
        &self,
        source_address: &str,
        target_address: &str,
        ack: bool,
    ) -> Result<()> {
        let frame = encode_open_bridge(ack, source_address, target_address)?;
        self.send_frame(Frame::single(frame)).await
    }

    /// Asks the broker to close the bridge to `target_address`.
    pub async fn close_bridge(&self, target_address: &str) -> Result<()> {
        let frame = encode_close_bridge(false, target_address)?;
        self.send_frame(Frame::single(frame)).await
    }

    pub(crate) async fn notify_close_bridge(&self) -> Result<()> {
        let frame = encode_close_bridge(true, "")?;
        self.send_frame(Frame::single(frame)).await
    }

    pub(crate) async fn send_bridge_frame(&self, frame: Frame) -> Result<()> {
        self.send_frame(frame).await
    }

    async fn send_frame(&self, frame: Frame) -> Result<()> {
        let Some(connection) = self.shared.connection.load_full() else {
            return Err(Error::NotConnected);
        };
        if connection.outbound.send(frame).await.is_err() {
            return Err(Error::NotConnected);
        }
        Ok(())
    }
}

/// The outcome of a non-blocking [`Client::try_next_message`]: a message is
/// available, the queue is empty but the client is still connected, or the
/// client has disconnected and no further messages arrive until it reconnects.
#[derive(Debug)]
pub enum Reception {
    /// A message was waiting and is returned.
    Message(Message),
    /// The queue is currently empty; the client is still connected.
    Empty,
    /// The connection has dropped; the queue will not produce more messages
    /// until the client reconnects.
    Disconnected,
}

async fn establish_connection(shared: &Arc<ClientShared>, address: &str) -> Result<()> {
    let stream = connect_with_retries(
        address,
        shared.settings.max_connection_attempts,
        shared.settings.timeout_per_attempt,
    )
    .await?;
    let (read_half, write_half) = stream.into_split();
    let (message_sender, message_receiver) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
    let (outbound_sender, outbound_receiver) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
    let alive = Arc::new(AtomicBool::new(true));
    tokio::spawn(writer_task(write_half, outbound_receiver, alive.clone()));
    tokio::spawn(read_task(
        read_half,
        message_sender,
        shared.settings.read_timeout,
        alive.clone(),
    ));
    shared.connection.store(Some(Arc::new(Connection {
        outbound: outbound_sender,
        inbound: Mutex::new(message_receiver),
        alive,
    })));
    send_frame_on(shared, Frame::single(encode_hello(&shared.id)?)).await?;
    resubscribe(shared).await?;
    Ok(())
}

async fn send_frame_on(shared: &Arc<ClientShared>, frame: Frame) -> Result<()> {
    let Some(connection) = shared.connection.load_full() else {
        return Err(Error::NotConnected);
    };
    if connection.outbound.send(frame).await.is_err() {
        return Err(Error::NotConnected);
    }
    Ok(())
}

async fn reconnection_task(shared: Weak<ClientShared>) {
    let mut backoff = RECONNECTION_INTERVAL;
    loop {
        tokio::time::sleep(backoff).await;
        let Some(shared) = shared.upgrade() else {
            break;
        };
        let connected = shared
            .connection
            .load_full()
            .is_some_and(|connection| connection.is_live());
        if connected {
            backoff = RECONNECTION_INTERVAL;
            continue;
        }
        let address = shared.cold.lock().await.broker_address.clone();
        let Some(address) = address else {
            continue;
        };
        let _ = establish_connection(&shared, &address).await;
        backoff = (backoff * 2).min(MAX_RECONNECTION_BACKOFF);
    }
}

async fn connect_with_retries(
    address: &str,
    max_connection_attempts: Option<u16>,
    timeout_per_attempt: Duration,
) -> Result<TcpStream> {
    let mut attempt: u16 = 0;
    loop {
        if let Ok(Ok(stream)) =
            tokio::time::timeout(timeout_per_attempt, create_tcp_connection(address)).await
        {
            return Ok(stream);
        }
        if let Some(max_attempts) = max_connection_attempts
            && attempt >= max_attempts
        {
            return Err(Error::MaxConnectionAttempts);
        }
        attempt += 1;
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

pub(crate) async fn resolve_addresses(address: &str) -> Result<Vec<SocketAddr>> {
    let mut addresses: Vec<SocketAddr> = tokio::net::lookup_host(address).await?.collect();
    addresses.sort_by_key(|resolved| !resolved.is_ipv4());
    if addresses.is_empty() {
        return Err(Error::AddressResolution);
    }
    Ok(addresses)
}

async fn create_tcp_connection(address: &str) -> Result<TcpStream> {
    let addresses = resolve_addresses(address).await?;
    let mut last_error: crate::Error = Error::AddressResolution;
    for socket_address in addresses {
        match TcpStream::connect(socket_address).await {
            Ok(stream) => {
                configure_stream(&stream)?;
                return Ok(stream);
            }
            Err(error) => last_error = error.into(),
        }
    }
    Err(last_error)
}

fn configure_stream(stream: &TcpStream) -> Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        stream.set_nodelay(true)?;
    }
    let socket_reference = socket2::SockRef::from(stream);
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(1))
        .with_interval(Duration::from_secs(1));
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let keepalive = keepalive.with_retries(3);
    socket_reference.set_tcp_keepalive(&keepalive)?;
    Ok(())
}

async fn read_task(
    read_half: OwnedReadHalf,
    message_sender: Sender<Message>,
    read_timeout: Option<Duration>,
    alive: Arc<AtomicBool>,
) {
    let mut reader = FrameReader::new(read_half);
    loop {
        let frame = match read_timeout {
            Some(limit) => match tokio::time::timeout(limit, reader.next_frame()).await {
                Ok(Ok(frame)) => frame,
                _ => break,
            },
            None => match reader.next_frame().await {
                Ok(frame) => frame,
                Err(_) => break,
            },
        };
        let Some(message) = decode_message(frame.freeze()) else {
            break;
        };
        if message_sender.send(message).await.is_err() {
            break;
        }
    }
    alive.store(false, Ordering::Release);
}

async fn writer_task(
    mut write_half: OwnedWriteHalf,
    mut outbound: Receiver<Frame>,
    alive: Arc<AtomicBool>,
) {
    let mut frames: Vec<Frame> = Vec::with_capacity(MAX_WRITE_BATCH);
    while let Some(frame) = outbound.recv().await {
        frames.clear();
        frames.push(frame);
        while frames.len() < MAX_WRITE_BATCH
            && let Ok(next) = outbound.try_recv()
        {
            frames.push(next);
        }
        if write_frames(&mut write_half, &frames).await.is_err() {
            break;
        }
    }
    alive.store(false, Ordering::Release);
}

async fn resubscribe(shared: &Arc<ClientShared>) -> Result<()> {
    let topics: HashSet<String> = {
        let cold = shared.cold.lock().await;
        cold.subscriptions
            .iter()
            .cloned()
            .chain(cold.pending_subscriptions.iter().cloned())
            .collect()
    };
    for topic in topics {
        send_frame_on(shared, Frame::single(encode_subscribe(&topic)?)).await?;
        let mut cold = shared.cold.lock().await;
        cold.subscriptions.insert(topic.clone());
        cold.pending_subscriptions.remove(&topic);
    }
    Ok(())
}
