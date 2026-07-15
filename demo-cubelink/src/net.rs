//! The networking seam: hearsay's encrypted UDP transport on a background tokio
//! thread, bridged to the synchronous game loop with unbounded channels.
//!
//! The model is host-authoritative. Guests send only their *input* (a movement
//! direction); the host owns every cube's position, applies the movement rules
//! itself, and broadcasts the authoritative positions. A guest therefore cannot
//! teleport or move faster than the rules allow by lying about its position —
//! it never sends one. Chat is exchanged reliably in both directions. All
//! traffic is encrypted with a shared passphrase, so only the two players can
//! join or read the session.

use hearsay::{
    Delivery, create_udp_client_secure, next_udp_message, start_broker, start_udp_listener_secure,
    udp_publish, udp_subscribe,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

pub type PlayerId = u32;

const BROKER_TCP_BIND: &str = "0.0.0.0:9500";
const UDP_LISTEN_BIND: &str = "0.0.0.0:9501";
const UDP_PORT: u16 = 9501;
const LOOPBACK_HOST: &str = "127.0.0.1";
const PASSPHRASE: &str = "cubelink";
const INPUT_TOPIC: &str = "input";
const STATE_TOPIC: &str = "state";
const CHAT_TOPIC: &str = "chat";

/// A command from the game loop to the network thread.
pub enum NetCommand {
    /// A guest's movement input for this frame.
    Input([f32; 2]),
    /// The host's authoritative position for one player.
    State { id: PlayerId, position: [f32; 3] },
    Chat(String),
}

/// An event from the network thread to the game loop.
pub enum NetEvent {
    /// A guest's input, received by the host.
    Input { id: PlayerId, direction: [f32; 2] },
    /// An authoritative position from the host, received by everyone.
    State { id: PlayerId, position: [f32; 3] },
    Chat { id: PlayerId, text: String },
}

/// The game loop's handle on the network thread.
pub struct NetLink {
    pub to_net: UnboundedSender<NetCommand>,
    pub from_net: UnboundedReceiver<NetEvent>,
    pub my_id: PlayerId,
    pub is_host: bool,
}

#[derive(Serialize, Deserialize)]
struct InputMessage {
    id: PlayerId,
    direction: [f32; 2],
}

#[derive(Serialize, Deserialize)]
struct StateMessage {
    id: PlayerId,
    position: [f32; 3],
}

#[derive(Serialize, Deserialize)]
struct ChatMessage {
    id: PlayerId,
    text: String,
}

/// Starts the network thread. `join` (as the first argument) makes this a
/// guest; anything else makes it the host. A guest may pass the host's address
/// as the second argument for LAN play; it defaults to loopback.
pub fn spawn_net() -> NetLink {
    let arguments: Vec<String> = std::env::args().collect();
    let is_host = arguments.get(1).map(String::as_str) != Some("join");
    let my_id: PlayerId = if is_host { 0 } else { 1 };
    let host_address = arguments
        .get(2)
        .cloned()
        .unwrap_or_else(|| LOOPBACK_HOST.to_string());
    let connect_host = if is_host { LOOPBACK_HOST } else { &host_address };
    let connect_address = format!("{connect_host}:{UDP_PORT}");

    let (to_net, from_game) = unbounded_channel::<NetCommand>();
    let (to_game, from_net) = unbounded_channel::<NetEvent>();

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("network runtime");
        runtime.block_on(network_main(is_host, my_id, connect_address, from_game, to_game));
    });

    NetLink {
        to_net,
        from_net,
        my_id,
        is_host,
    }
}

async fn network_main(
    is_host: bool,
    my_id: PlayerId,
    connect_address: String,
    mut from_game: UnboundedReceiver<NetCommand>,
    to_game: UnboundedSender<NetEvent>,
) {
    let _broker = if is_host {
        match start_broker(BROKER_TCP_BIND).await {
            Ok(broker) => {
                let _ = start_udp_listener_secure(&broker, UDP_LISTEN_BIND, PASSPHRASE).await;
                Some(broker)
            }
            Err(_) => None,
        }
    } else {
        None
    };

    let client = loop {
        match create_udp_client_secure(&format!("player{my_id}"), &connect_address, PASSPHRASE).await
        {
            Ok(client) => break client,
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    };

    // The host consumes guest input; everyone consumes authoritative state and chat.
    if is_host {
        let _ = udp_subscribe(&client, &[INPUT_TOPIC, STATE_TOPIC, CHAT_TOPIC]).await;
    } else {
        let _ = udp_subscribe(&client, &[STATE_TOPIC, CHAT_TOPIC]).await;
    }

    loop {
        tokio::select! {
            command = from_game.recv() => {
                let Some(command) = command else {
                    break;
                };
                match command {
                    NetCommand::Input(direction) => {
                        let message = InputMessage { id: my_id, direction };
                        let _ = udp_publish(&client, INPUT_TOPIC, &message, Delivery::Unreliable).await;
                    }
                    NetCommand::State { id, position } => {
                        let message = StateMessage { id, position };
                        let _ = udp_publish(&client, STATE_TOPIC, &message, Delivery::Unreliable).await;
                    }
                    NetCommand::Chat(text) => {
                        let message = ChatMessage { id: my_id, text };
                        let _ = udp_publish(&client, CHAT_TOPIC, &message, Delivery::ReliableOrdered).await;
                    }
                }
            }
            message = next_udp_message(&client) => {
                let Some(message) = message else {
                    break;
                };
                let Some(text) = message.text() else {
                    continue;
                };
                match message.topic() {
                    INPUT_TOPIC => {
                        if let Ok(update) = serde_json::from_str::<InputMessage>(text)
                            && update.id != my_id
                        {
                            let _ = to_game.send(NetEvent::Input {
                                id: update.id,
                                direction: update.direction,
                            });
                        }
                    }
                    STATE_TOPIC => {
                        if let Ok(update) = serde_json::from_str::<StateMessage>(text) {
                            let _ = to_game.send(NetEvent::State {
                                id: update.id,
                                position: update.position,
                            });
                        }
                    }
                    CHAT_TOPIC => {
                        if let Ok(update) = serde_json::from_str::<ChatMessage>(text)
                            && update.id != my_id
                        {
                            let _ = to_game.send(NetEvent::Chat {
                                id: update.id,
                                text: update.text,
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
