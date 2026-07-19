use hearsay::{
    Broker, Client, ClientSettings, Interrupt, Lifecycle, LifecycleSettings, Message, Route,
    enum2contract::{self, EnumContract},
    run_lifecycle,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[cfg(feature = "spawn")]
use hearsay::{App, AppStatus};

const BROKER_ADDRESS: &str = "127.0.0.1:9612";

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

impl PingPongContract {
    pub const BROADCAST: &'static str = "broadcast";
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum PingPongCommand {
    Ping,

    #[default]
    #[serde(other)]
    Empty,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum PingPongEvent {
    Pong {
        value: u32,
    },

    #[default]
    #[serde(other)]
    Empty,
}

struct Responder;

impl Lifecycle for Responder {
    async fn initialize(&mut self, client: &Client) {
        let topic = PingPongContract::command_topic(PingPongContract::BROADCAST);
        let _ = client.subscribe(&[&topic]).await;
    }

    async fn receive_message(&mut self, message: &Message, client: &Client) -> Option<Interrupt> {
        match message.topic() {
            topic if PingPongContract::command_topic(PingPongContract::BROADCAST) == topic => {
                let Ok(payload) = CommandPayload::from_json(message.text().unwrap_or_default())
                else {
                    return None;
                };
                if matches!(payload.command, PingPongCommand::Ping) {
                    println!("[responder] received ping, sending pong");
                    let (event_topic, mut event_payload) =
                        PingPongContract::event(PingPongContract::BROADCAST);
                    event_payload.event = PingPongEvent::Pong { value: 42 };
                    let _ = client
                        .publish(&event_topic, &event_payload, Route::Global)
                        .await;
                }
            }
            _ => {}
        }
        None
    }
}

#[tokio::main]
async fn main() -> hearsay::Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("broker") => run_broker().await,
        Some("responder") => run_lifecycle(Responder, responder_settings()).await,
        Some("requester") => run_requester().await,
        #[cfg(feature = "spawn")]
        Some("host") => run_host().await,
        Some(role) => Err(format!("unknown role: {role}").into()),
        None => run_all_in_one().await,
    }
}

#[cfg(feature = "spawn")]
async fn run_host() -> hearsay::Result<()> {
    let broker = Broker::start(BROKER_ADDRESS).await?;
    let executable = std::env::current_exe()?.display().to_string();
    broker
        .spawn_app(App {
            name: "responder".to_string(),
            path: executable.clone(),
            args: "responder".to_string(),
            ..Default::default()
        })
        .await?;
    broker
        .spawn_app(App {
            name: "requester".to_string(),
            path: executable,
            args: "requester".to_string(),
            ..Default::default()
        })
        .await?;

    loop {
        for line in broker.drain_output().await {
            println!("[host] {}: {}", line.app_name, line.line);
        }
        if broker.app_status("requester").await == AppStatus::ExitedSuccessfully {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn responder_settings() -> LifecycleSettings {
    LifecycleSettings {
        name: "responder".to_string(),
        broker_address: BROKER_ADDRESS.to_string(),
        update_interval: Duration::from_millis(50),
    }
}

async fn run_broker() -> hearsay::Result<()> {
    let broker = Broker::start(BROKER_ADDRESS).await?;
    println!("[broker] listening on {BROKER_ADDRESS}");
    while broker.is_running() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn run_requester() -> hearsay::Result<()> {
    let requester = Client::new("requester", ClientSettings::default());
    requester.connect(BROKER_ADDRESS).await?;
    requester
        .subscribe(&[&PingPongContract::event_topic(PingPongContract::BROADCAST)])
        .await?;

    loop {
        let (command_topic, mut command_payload) =
            PingPongContract::command(PingPongContract::BROADCAST);
        command_payload.command = PingPongCommand::Ping;
        requester
            .publish(&command_topic, &command_payload, Route::Global)
            .await?;

        let received =
            tokio::time::timeout(Duration::from_millis(100), requester.next_message()).await;
        let Ok(Some(message)) = received else {
            continue;
        };
        if message.topic() != PingPongContract::event_topic(PingPongContract::BROADCAST) {
            continue;
        }
        let Ok(event_payload) = EventPayload::from_json(message.text().unwrap_or_default()) else {
            continue;
        };
        if let PingPongEvent::Pong { value } = event_payload.event {
            println!("[requester] received pong with value {value}");
            return Ok(());
        }
    }
}

async fn run_all_in_one() -> hearsay::Result<()> {
    let _broker = Broker::start(BROKER_ADDRESS).await?;
    tokio::spawn(run_lifecycle(Responder, responder_settings()));
    run_requester().await
}
