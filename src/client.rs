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
    sync::{Arc, Weak},
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
const INBOUND_QUEUE_CAPACITY: usize = 1024;
const OUTBOUND_QUEUE_CAPACITY: usize = 1024;

#[derive(Debug, Clone)]
pub struct ClientSettings {
    /// Number of additional connection attempts after the first failure before
    /// [`connect`] gives up. `Some(0)` makes a single attempt; `Some(n)` retries
    /// up to `n` more times; `None` retries forever, so `connect` will not return
    /// until a broker is reachable.
    pub max_connection_attempts: Option<u16>,
    pub autoreconnect: bool,
    pub timeout_per_attempt: Duration,
    pub read_timeout: Option<Duration>,
}

impl Default for ClientSettings {
    fn default() -> Self {
        Self {
            max_connection_attempts: Some(100),
            autoreconnect: true,
            timeout_per_attempt: Duration::from_secs(2),
            read_timeout: None,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    shared: Arc<ClientShared>,
}

struct ClientShared {
    id: Box<str>,
    settings: ClientSettings,
    connection: ArcSwapOption<Connection>,
    receiver: Mutex<Option<Receiver<Message>>>,
    cold: Mutex<ColdState>,
}

struct Connection {
    outbound: Sender<Frame>,
}

struct ColdState {
    subscriptions: HashSet<String>,
    pending_subscriptions: HashSet<String>,
    broker_address: Option<String>,
    reconnection_task_spawned: bool,
}

pub fn create_client(name: &str, settings: ClientSettings) -> Client {
    create_client_with_id(format!("{name}_{}", uuid::Uuid::new_v4()), settings)
}

pub(crate) fn create_client_with_id(id: String, settings: ClientSettings) -> Client {
    Client {
        shared: Arc::new(ClientShared {
            id: id.into(),
            settings,
            connection: ArcSwapOption::empty(),
            receiver: Mutex::new(None),
            cold: Mutex::new(ColdState {
                subscriptions: HashSet::new(),
                pending_subscriptions: HashSet::new(),
                broker_address: None,
                reconnection_task_spawned: false,
            }),
        }),
    }
}

pub fn client_id(client: &Client) -> &str {
    &client.shared.id
}

pub async fn is_connected(client: &Client) -> bool {
    let Some(connection) = client.shared.connection.load_full() else {
        return false;
    };
    if connection.outbound.is_closed() {
        return false;
    }
    let receiver_guard = client.shared.receiver.lock().await;
    receiver_guard
        .as_ref()
        .is_some_and(|receiver| !receiver.is_closed())
}

pub async fn subscriptions(client: &Client) -> HashSet<String> {
    client.shared.cold.lock().await.subscriptions.clone()
}

/// Connects to a broker. With `autoreconnect` enabled the client also
/// re-establishes the connection if it later drops and re-sends its
/// subscriptions, but it does not replay messages published to those topics
/// while it was disconnected; any such messages are lost.
pub async fn connect(client: &Client, address: &str) -> Result<()> {
    client.shared.cold.lock().await.broker_address = Some(address.to_string());
    establish_connection(&client.shared, address).await?;
    if client.shared.settings.autoreconnect {
        let mut cold = client.shared.cold.lock().await;
        if !cold.reconnection_task_spawned {
            cold.reconnection_task_spawned = true;
            tokio::spawn(reconnection_task(Arc::downgrade(&client.shared)));
        }
    }
    Ok(())
}

pub async fn publish(
    client: &Client,
    topic: impl AsRef<str>,
    payload: &impl Serialize,
    route: Route,
) -> Result<()> {
    let mut buffer = begin_publish_frame(topic.as_ref(), route_flags(route == Route::Local))?;
    serde_json::to_writer((&mut buffer).writer(), payload)?;
    let frame = finish_frame(buffer)?;
    send_frame(&client.shared, Frame::single(frame)).await
}

pub async fn publish_json(
    client: &Client,
    topic: impl AsRef<str>,
    payload: &str,
    route: Route,
) -> Result<()> {
    let mut buffer = begin_publish_frame(topic.as_ref(), route_flags(route == Route::Local))?;
    buffer.put_slice(payload.as_bytes());
    let frame = finish_frame(buffer)?;
    send_frame(&client.shared, Frame::single(frame)).await
}

pub async fn publish_bytes(
    client: &Client,
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
    send_frame(&client.shared, Frame::single(frame)).await
}

pub async fn subscribe(client: &Client, topics: &[impl AsRef<str>]) -> Result<()> {
    for topic in topics {
        let topic = topic.as_ref();
        {
            let mut cold = client.shared.cold.lock().await;
            if client.shared.connection.load_full().is_none() {
                cold.pending_subscriptions.insert(topic.to_string());
                continue;
            }
        }
        let frame = Frame::single(encode_subscribe(topic)?);
        if let Err(error) = send_frame(&client.shared, frame).await {
            client
                .shared
                .cold
                .lock()
                .await
                .pending_subscriptions
                .insert(topic.to_string());
            return Err(error);
        }
        client
            .shared
            .cold
            .lock()
            .await
            .subscriptions
            .insert(topic.to_string());
    }
    Ok(())
}

pub async fn unsubscribe(client: &Client, topics: &[impl AsRef<str>]) -> Result<()> {
    for topic in topics {
        let topic = topic.as_ref();
        let connected = {
            let mut cold = client.shared.cold.lock().await;
            cold.pending_subscriptions.remove(topic);
            cold.subscriptions.remove(topic);
            client.shared.connection.load_full().is_some()
        };
        if connected {
            send_frame(&client.shared, Frame::single(encode_unsubscribe(topic)?)).await?;
        }
    }
    Ok(())
}

/// Result of a non-blocking [`try_next_message`]: a message is available, the
/// queue is currently empty but the client is still connected, or the client
/// has disconnected and no further messages will arrive until reconnection.
#[derive(Debug)]
pub enum Reception {
    Message(Message),
    Empty,
    Disconnected,
}

pub async fn try_next_message(client: &Client) -> Reception {
    let mut receiver_guard = client.shared.receiver.lock().await;
    let Some(receiver) = receiver_guard.as_mut() else {
        return Reception::Disconnected;
    };
    match receiver.try_recv() {
        Ok(message) => Reception::Message(message),
        Err(TryRecvError::Disconnected) => {
            *receiver_guard = None;
            client.shared.connection.store(None);
            Reception::Disconnected
        }
        Err(TryRecvError::Empty) => Reception::Empty,
    }
}

pub async fn next_message(client: &Client) -> Option<Message> {
    let mut receiver_guard = client.shared.receiver.lock().await;
    let receiver = receiver_guard.as_mut()?;
    match receiver.recv().await {
        Some(message) => Some(message),
        None => {
            *receiver_guard = None;
            client.shared.connection.store(None);
            None
        }
    }
}

pub async fn open_bridge(
    client: &Client,
    source_address: &str,
    target_address: &str,
) -> Result<()> {
    open_bridge_acked(client, source_address, target_address, false).await
}

pub(crate) async fn open_bridge_acked(
    client: &Client,
    source_address: &str,
    target_address: &str,
    ack: bool,
) -> Result<()> {
    let frame = encode_open_bridge(ack, source_address, target_address)?;
    send_frame(&client.shared, Frame::single(frame)).await
}

pub async fn close_bridge(client: &Client, target_address: &str) -> Result<()> {
    let frame = encode_close_bridge(false, target_address)?;
    send_frame(&client.shared, Frame::single(frame)).await
}

pub(crate) async fn notify_close_bridge(client: &Client) -> Result<()> {
    let frame = encode_close_bridge(true, "")?;
    send_frame(&client.shared, Frame::single(frame)).await
}

pub(crate) async fn send_bridge_frame(client: &Client, frame: Frame) -> Result<()> {
    send_frame(&client.shared, frame).await
}

async fn send_frame(shared: &ClientShared, frame: Frame) -> Result<()> {
    let Some(connection) = shared.connection.load_full() else {
        return Err(Error::NotConnected);
    };
    if connection.outbound.send(frame).await.is_err() {
        return Err(Error::NotConnected);
    }
    Ok(())
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
    tokio::spawn(writer_task(write_half, outbound_receiver));
    tokio::spawn(read_task(
        read_half,
        message_sender,
        shared.settings.read_timeout,
    ));
    *shared.receiver.lock().await = Some(message_receiver);
    shared.connection.store(Some(Arc::new(Connection {
        outbound: outbound_sender,
    })));
    send_frame(shared, Frame::single(encode_hello(&shared.id)?)).await?;
    resubscribe(shared).await?;
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
            .is_some_and(|connection| !connection.outbound.is_closed());
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
        tokio::time::sleep(Duration::from_secs(1)).await;
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
                configure_keepalive(&stream)?;
                return Ok(stream);
            }
            Err(error) => last_error = error.into(),
        }
    }
    Err(last_error)
}

fn configure_keepalive(stream: &TcpStream) -> Result<()> {
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
}

async fn writer_task(mut write_half: OwnedWriteHalf, mut outbound: Receiver<Frame>) {
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
        send_frame(shared, Frame::single(encode_subscribe(&topic)?)).await?;
        let mut cold = shared.cold.lock().await;
        cold.subscriptions.insert(topic.clone());
        cold.pending_subscriptions.remove(&topic);
    }
    Ok(())
}
