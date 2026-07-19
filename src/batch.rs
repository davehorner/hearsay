#[cfg(not(target_arch = "wasm32"))]
use crate::{Client, Result, Route};
#[cfg(not(target_arch = "wasm32"))]
use serde::Serialize;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{Duration, Instant};

/// A buffer that coalesces many small values into one binary frame.
///
/// High-frequency traffic pays the per-message framing and routing overhead
/// once per flush instead of once per item. Items accumulate through
/// [`Batch::push`] and are flushed automatically when the buffer reaches
/// `max_items` or `flush_interval` has elapsed since the last flush, or on
/// demand with [`Batch::flush`]. A subscriber reconstructs the items with
/// [`crate::Message::read_batch`].
///
/// The batch holds no connection of its own; every operation takes the
/// [`Client`] to publish through, so a batch is plain data that can be moved
/// and stored freely.
#[cfg(not(target_arch = "wasm32"))]
pub struct Batch<T> {
    /// The topic every flush publishes to.
    pub topic: String,
    /// The route every flush uses.
    pub route: Route,
    /// The items buffered since the last flush.
    pub items: Vec<T>,
    /// The item count that triggers an automatic flush on [`Batch::push`].
    pub max_items: usize,
    /// The maximum age of buffered items before [`Batch::push`] flushes them.
    pub flush_interval: Duration,
    /// When the batch last flushed, used to enforce `flush_interval`.
    pub last_flush: Instant,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> Batch<T> {
    /// Creates an empty batch that flushes to `topic` on `route` once it holds
    /// `max_items` items or `flush_interval` has passed since the last flush.
    pub fn new(topic: &str, route: Route, max_items: usize, flush_interval: Duration) -> Batch<T> {
        Batch {
            topic: topic.to_string(),
            route,
            items: Vec::new(),
            max_items,
            flush_interval,
            last_flush: Instant::now(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Serialize> Batch<T> {
    /// Appends `item`, flushing through `client` first if the batch has reached
    /// `max_items` or `flush_interval` has elapsed.
    pub async fn push(&mut self, client: &Client, item: T) -> Result<()> {
        self.items.push(item);
        if self.items.len() >= self.max_items || self.last_flush.elapsed() >= self.flush_interval {
            self.flush(client).await?;
        }
        Ok(())
    }

    /// Publishes the buffered items as a single binary frame through `client`
    /// and empties the batch. Does nothing if the batch is empty.
    pub async fn flush(&mut self, client: &Client) -> Result<()> {
        if self.items.is_empty() {
            return Ok(());
        }
        let payload = postcard::to_allocvec(&self.items)?;
        client
            .publish_bytes(&self.topic, &payload, self.route)
            .await?;
        self.items.clear();
        self.last_flush = Instant::now();
        Ok(())
    }
}
