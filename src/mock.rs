use crate::{
    Error, Message, Publisher, Reception, Result, Route, Subscriber, decode_message,
    wire::{encode_deliver_binary, encode_deliver_text},
};
use bytes::Bytes;
use std::{collections::HashSet, future::Future, sync::Mutex as StdMutex};
use tokio::sync::{
    Mutex,
    mpsc::{self, error::TryRecvError},
};

/// A message the code under test published to a [`MockBus`], captured for
/// assertions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    /// The topic the message was published to.
    pub topic: String,
    /// The raw payload bytes, before any deserialization.
    pub payload: Bytes,
    /// Whether the payload was published as binary rather than text.
    pub binary: bool,
    /// The route the message was published with.
    pub route: Route,
}

/// An in-memory [`Publisher`] and [`Subscriber`] for testing bus logic without
/// a broker.
///
/// Give the code under test a `&MockBus` wherever it expects an
/// `&impl Publisher`, `&impl Subscriber`, or `&impl MessageBus`. It records
/// every publish for inspection with [`MockBus::published`], and lets a test
/// hand messages to the receiving side with [`MockBus::inject`] (or the typed
/// [`MockBus::inject_text`] / [`MockBus::inject_bytes`]) so
/// [`Subscriber::next_message`] returns them in order.
///
/// # Example
///
/// ```
/// use hearsay::{MockBus, Publisher, Subscriber, Route};
///
/// tokio::runtime::Runtime::new().unwrap().block_on(async {
///     let bus = MockBus::new();
///     bus.publish("greetings", &"hello", Route::Global).await.unwrap();
///     assert_eq!(bus.published()[0].topic, "greetings");
///
///     bus.inject_text("greetings", "\"hi\"").unwrap();
///     let message = bus.next_message().await.unwrap();
///     assert_eq!(message.topic(), "greetings");
/// });
/// ```
pub struct MockBus {
    published: StdMutex<Vec<Published>>,
    subscriptions: StdMutex<HashSet<String>>,
    inbound_sender: mpsc::UnboundedSender<Message>,
    inbound: Mutex<mpsc::UnboundedReceiver<Message>>,
}

impl MockBus {
    /// Creates an empty mock bus with nothing published and no pending inbound
    /// messages.
    pub fn new() -> Self {
        let (inbound_sender, inbound_receiver) = mpsc::unbounded_channel();
        Self {
            published: StdMutex::new(Vec::new()),
            subscriptions: StdMutex::new(HashSet::new()),
            inbound_sender,
            inbound: Mutex::new(inbound_receiver),
        }
    }

    /// Queues `message` to be returned by the next call to
    /// [`Subscriber::next_message`] or [`Subscriber::try_next_message`].
    pub fn inject(&self, message: Message) {
        let _ = self.inbound_sender.send(message);
    }

    /// Queues a text message on `topic` carrying `payload`, as if the broker had
    /// delivered it.
    pub fn inject_text(&self, topic: &str, payload: &str) -> Result<()> {
        let frame = encode_deliver_text(topic, payload)?;
        let message = decode_message(frame).ok_or_else(|| Error::from("invalid mock message"))?;
        self.inject(message);
        Ok(())
    }

    /// Queues a binary message on `topic` carrying `payload`, as if the broker
    /// had delivered it.
    pub fn inject_bytes(&self, topic: &str, payload: &[u8]) -> Result<()> {
        let frame = encode_deliver_binary(topic, payload)?;
        let message = decode_message(frame).ok_or_else(|| Error::from("invalid mock message"))?;
        self.inject(message);
        Ok(())
    }

    /// Every message published to this bus so far, in order.
    pub fn published(&self) -> Vec<Published> {
        self.published.lock().unwrap().clone()
    }

    /// The set of topics that have been subscribed to and not unsubscribed.
    pub fn subscriptions(&self) -> HashSet<String> {
        self.subscriptions.lock().unwrap().clone()
    }
}

impl Default for MockBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Publisher for MockBus {
    fn publish_encoded(
        &self,
        topic: &str,
        payload: &[u8],
        binary: bool,
        route: Route,
    ) -> impl Future<Output = Result<()>> + Send {
        self.published.lock().unwrap().push(Published {
            topic: topic.to_string(),
            payload: Bytes::copy_from_slice(payload),
            binary,
            route,
        });
        std::future::ready(Ok(()))
    }
}

impl Subscriber for MockBus {
    fn subscribe(&self, topics: &[String]) -> impl Future<Output = Result<()>> + Send {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        for topic in topics {
            subscriptions.insert(topic.clone());
        }
        std::future::ready(Ok(()))
    }

    fn unsubscribe(&self, topics: &[String]) -> impl Future<Output = Result<()>> + Send {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        for topic in topics {
            subscriptions.remove(topic);
        }
        std::future::ready(Ok(()))
    }

    async fn next_message(&self) -> Option<Message> {
        self.inbound.lock().await.recv().await
    }

    async fn try_next_message(&self) -> Reception {
        match self.inbound.lock().await.try_recv() {
            Ok(message) => Reception::Message(message),
            Err(TryRecvError::Empty) => Reception::Empty,
            Err(TryRecvError::Disconnected) => Reception::Disconnected,
        }
    }
}
