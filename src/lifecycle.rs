use crate::{Client, ClientSettings, Message, Result};
use std::{
    future::Future,
    time::{Duration, Instant},
};

/// Returned from [`Lifecycle::receive_message`] to steer the run loop: run the
/// next [`Lifecycle::update`] now instead of waiting out the interval, or stop
/// the loop entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interrupt {
    /// Break out of message-waiting and call [`Lifecycle::update`] immediately.
    UpdateImmediately,
    /// End the run loop; [`run_lifecycle`] returns.
    Stop,
}

/// An event-driven service driven by [`run_lifecycle`].
///
/// Implement the hooks you need and leave the rest as their no-op defaults:
/// [`Lifecycle::initialize`] runs once after connecting (subscribe here),
/// [`Lifecycle::receive_message`] runs for each inbound message and may return
/// an [`Interrupt`], and [`Lifecycle::update`] runs every tick of the
/// configured interval. Each hook is handed the connected [`Client`] so it can
/// publish and subscribe.
pub trait Lifecycle: Send {
    /// Runs once, immediately after the client connects. The default does
    /// nothing.
    fn initialize(&mut self, _client: &Client) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }

    /// Runs once per `update_interval` tick. The default does nothing.
    fn update(&mut self, _client: &Client) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }

    /// Runs for each received message, optionally returning an [`Interrupt`] to
    /// steer the loop. The default ignores the message and returns `None`.
    fn receive_message(
        &mut self,
        _message: &Message,
        _client: &Client,
    ) -> impl Future<Output = Option<Interrupt>> + Send {
        std::future::ready(None)
    }
}

/// Configuration for a [`run_lifecycle`] loop.
#[derive(Debug, Clone)]
pub struct LifecycleSettings {
    /// The name the underlying [`Client`] registers under.
    pub name: String,
    /// The broker address to connect to.
    pub broker_address: String,
    /// How often [`Lifecycle::update`] is called between messages.
    pub update_interval: Duration,
}

/// Connects a [`Client`], drives `lifecycle` through its hooks, and returns when
/// [`Lifecycle::receive_message`] yields [`Interrupt::Stop`].
///
/// This is a ready-made run loop for a service that reacts to messages and
/// ticks on an interval; construct the [`Client`] and call its methods directly
/// instead if you need finer control over the loop.
pub async fn run_lifecycle(
    mut lifecycle: impl Lifecycle,
    settings: LifecycleSettings,
) -> Result<()> {
    let client = Client::new(&settings.name, ClientSettings::default());
    client.connect(&settings.broker_address).await?;
    lifecycle.initialize(&client).await;
    loop {
        let deadline = Instant::now() + settings.update_interval;
        let interrupt = receive_messages_until_deadline(&mut lifecycle, &client, deadline).await;
        if interrupt == Some(Interrupt::Stop) {
            return Ok(());
        }
        lifecycle.update(&client).await;
    }
}

async fn receive_messages_until_deadline(
    lifecycle: &mut impl Lifecycle,
    client: &Client,
    deadline: Instant,
) -> Option<Interrupt> {
    loop {
        let now = Instant::now();
        if now >= deadline {
            return None;
        }
        let remaining = deadline - now;
        match tokio::time::timeout(remaining, client.next_message()).await {
            Ok(Some(message)) => match lifecycle.receive_message(&message, client).await {
                Some(Interrupt::UpdateImmediately) => {
                    return Some(Interrupt::UpdateImmediately);
                }
                Some(Interrupt::Stop) => return Some(Interrupt::Stop),
                None => {}
            },
            _ => return None,
        }
    }
}
