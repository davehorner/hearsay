use crate::{Client, Message, Reception, Result, Route};
use serde::Serialize;
use std::future::Future;

/// The publish half of a message bus.
///
/// Code that only needs to send can take `&impl Publisher` instead of a
/// concrete [`Client`], so the same logic runs against a real broker
/// connection or against [`crate::MockBus`] in a test. Implementors provide the
/// one low-level method [`Publisher::publish_encoded`]; the typed and binary
/// conveniences are derived from it.
pub trait Publisher: Sync {
    /// Publishes an already-encoded `payload` to `topic`. `binary` marks the
    /// payload as bytes rather than text, and `route` selects local or global
    /// delivery. This is the single operation an implementor must supply.
    fn publish_encoded(
        &self,
        topic: &str,
        payload: &[u8],
        binary: bool,
        route: Route,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Publishes `value` to `topic`, serialized with postcard into a binary
    /// message. This is the default typed path; use [`Publisher::publish_json`]
    /// when a self-describing, schema-tolerant encoding is needed instead.
    fn publish<T: Serialize + Sync>(
        &self,
        topic: &str,
        value: &T,
        route: Route,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = postcard::to_allocvec(value)?;
            self.publish_encoded(topic, &payload, true, route).await
        }
    }

    /// Publishes `value` to `topic`, serialized as JSON text. The second-class
    /// counterpart to [`Publisher::publish`], for peers that need a
    /// self-describing format.
    fn publish_json<T: Serialize + Sync>(
        &self,
        topic: &str,
        value: &T,
        route: Route,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = serde_json::to_vec(value)?;
            self.publish_encoded(topic, &payload, false, route).await
        }
    }

    /// Publishes raw `payload` bytes to `topic` as a binary message.
    fn publish_bytes(
        &self,
        topic: &str,
        payload: &[u8],
        route: Route,
    ) -> impl Future<Output = Result<()>> + Send {
        self.publish_encoded(topic, payload, true, route)
    }

    /// Publishes an already-serialized text `payload` to `topic` as a text
    /// message.
    fn publish_text(
        &self,
        topic: &str,
        payload: &str,
        route: Route,
    ) -> impl Future<Output = Result<()>> + Send {
        self.publish_encoded(topic, payload.as_bytes(), false, route)
    }
}

/// The subscribe-and-receive half of a message bus.
///
/// Like [`Publisher`], this lets receiving logic be written against
/// `&impl Subscriber` and exercised with [`crate::MockBus`]. Topics passed to
/// [`Subscriber::subscribe`] may be literal names or MQTT-style patterns.
pub trait Subscriber: Sync {
    /// Subscribes to each topic in `topics`.
    fn subscribe(&self, topics: &[String]) -> impl Future<Output = Result<()>> + Send;

    /// Unsubscribes from each topic in `topics`.
    fn unsubscribe(&self, topics: &[String]) -> impl Future<Output = Result<()>> + Send;

    /// Waits for and returns the next message, or `None` when the bus has
    /// closed.
    fn next_message(&self) -> impl Future<Output = Option<Message>> + Send;

    /// Returns the next message without waiting.
    fn try_next_message(&self) -> impl Future<Output = Reception> + Send;
}

/// A bus that can both publish and receive. Blanket-implemented for every type
/// that is both a [`Publisher`] and a [`Subscriber`], so generic code can take
/// a single `&impl MessageBus` bound.
pub trait MessageBus: Publisher + Subscriber {}

impl<T: Publisher + Subscriber> MessageBus for T {}

impl Publisher for Client {
    fn publish_encoded(
        &self,
        topic: &str,
        payload: &[u8],
        binary: bool,
        route: Route,
    ) -> impl Future<Output = Result<()>> + Send {
        Client::publish_encoded(self, topic, payload, binary, route)
    }
}

impl Subscriber for Client {
    fn subscribe(&self, topics: &[String]) -> impl Future<Output = Result<()>> + Send {
        Client::subscribe(self, topics)
    }

    fn unsubscribe(&self, topics: &[String]) -> impl Future<Output = Result<()>> + Send {
        Client::unsubscribe(self, topics)
    }

    fn next_message(&self) -> impl Future<Output = Option<Message>> + Send {
        Client::next_message(self)
    }

    fn try_next_message(&self) -> impl Future<Output = Reception> + Send {
        Client::try_next_message(self)
    }
}
