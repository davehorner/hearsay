use hearsay::{Batch, Broker, Client, ClientSettings, Route};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct EntityUpdate {
    entity: u64,
    position: (f32, f32, f32),
}

fn test_settings() -> ClientSettings {
    ClientSettings {
        autoreconnect: false,
        ..Default::default()
    }
}

fn sample_updates(count: u64) -> Vec<EntityUpdate> {
    (0..count)
        .map(|index| EntityUpdate {
            entity: index,
            position: (index as f32, 0.0, 0.0),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_flushes_at_max_items() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9945";
    let _broker = Broker::start(broker_address).await?;
    let subscriber = Client::new("subscriber", test_settings());
    subscriber.connect(broker_address).await?;
    subscriber.subscribe(&["updates/size"]).await?;
    let publisher = Client::new("publisher", test_settings());
    publisher.connect(broker_address).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let mut batch = Batch::new("updates/size", Route::Global, 3, Duration::from_secs(60));
    for update in sample_updates(3) {
        batch.push(&publisher, update).await?;
    }
    assert!(batch.items.is_empty());

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await?
        .expect("expected a batch message");
    assert_eq!(message.topic(), "updates/size");
    let updates: Vec<EntityUpdate> = message.read_batch()?;
    assert_eq!(updates, sample_updates(3));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_flushes_after_interval() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9946";
    let _broker = Broker::start(broker_address).await?;
    let subscriber = Client::new("subscriber", test_settings());
    subscriber.connect(broker_address).await?;
    subscriber.subscribe(&["updates/interval"]).await?;
    let publisher = Client::new("publisher", test_settings());
    publisher.connect(broker_address).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let mut batch = Batch::new(
        "updates/interval",
        Route::Global,
        100,
        Duration::from_millis(100),
    );
    batch.push(&publisher, sample_updates(1)[0].clone()).await?;
    assert_eq!(batch.items.len(), 1);

    tokio::time::sleep(Duration::from_millis(150)).await;
    batch.push(&publisher, sample_updates(2)[1].clone()).await?;
    assert!(batch.items.is_empty());

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await?
        .expect("expected a batch message");
    let updates: Vec<EntityUpdate> = message.read_batch()?;
    assert_eq!(updates.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_flush_sends_pending_items() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9947";
    let _broker = Broker::start(broker_address).await?;
    let subscriber = Client::new("subscriber", test_settings());
    subscriber.connect(broker_address).await?;
    subscriber.subscribe(&["updates/manual"]).await?;
    let publisher = Client::new("publisher", test_settings());
    publisher.connect(broker_address).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let mut batch = Batch::new(
        "updates/manual",
        Route::Global,
        100,
        Duration::from_secs(60),
    );
    for update in sample_updates(2) {
        batch.push(&publisher, update).await?;
    }
    assert_eq!(batch.items.len(), 2);
    batch.flush(&publisher).await?;
    assert!(batch.items.is_empty());

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await?
        .expect("expected a batch message");
    let updates: Vec<EntityUpdate> = message.read_batch()?;
    assert_eq!(updates, sample_updates(2));
    Ok(())
}
