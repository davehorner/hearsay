use hearsay::{Broker, Client, ClientSettings, Route};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TestPayload {
    name: String,
    age: u8,
}

fn test_settings() -> ClientSettings {
    ClientSettings {
        autoreconnect: false,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn round_trip() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9931";
    let _broker = Broker::start(broker_address).await?;

    let subscriber = Client::new("subscriber", test_settings());
    subscriber.connect(broker_address).await?;

    let publisher = Client::new("publisher", test_settings());
    publisher.connect(broker_address).await?;

    let topic = "test/data";
    subscriber.subscribe(&[topic]).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let payload = TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    };
    publisher.publish(topic, &payload, Route::Global).await?;

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await?
        .expect("expected a message");
    assert_eq!(message.topic(), topic);
    let received: TestPayload = message.read()?;
    assert_eq!(received, payload);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn binary_round_trip() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9932";
    let _broker = Broker::start(broker_address).await?;

    let subscriber = Client::new("subscriber", test_settings());
    subscriber.connect(broker_address).await?;

    let publisher = Client::new("publisher", test_settings());
    publisher.connect(broker_address).await?;

    let topic = "test/binary";
    subscriber.subscribe(&[topic]).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let payload = vec![1_u8, 2, 3, 4, 5];
    publisher
        .publish_bytes(topic, &payload, Route::Global)
        .await?;

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await?
        .expect("expected a message");
    assert_eq!(message.topic(), topic);
    assert_eq!(message.bytes(), Some(payload.as_slice()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stopped_broker_shuts_down_and_disconnects_peers() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9948";
    let broker = Broker::start(broker_address).await?;

    let subscriber = Client::new("subscriber", test_settings());
    subscriber.connect(broker_address).await?;
    subscriber.subscribe(&["test/shutdown"]).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(broker.is_running());
    broker.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!broker.is_running());

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message()).await?;
    assert!(message.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn localhost_binds_loopback() -> hearsay::Result<()> {
    let _broker = Broker::start("localhost:9949").await?;

    let subscriber = Client::new("subscriber", test_settings());
    subscriber.connect("localhost:9949").await?;
    subscriber.subscribe(&["test/localhost"]).await?;

    let publisher = Client::new("publisher", test_settings());
    publisher.connect("127.0.0.1:9949").await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let payload = TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    };
    publisher
        .publish("test/localhost", &payload, Route::Global)
        .await?;

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await?
        .expect("expected a message");
    assert_eq!(message.topic(), "test/localhost");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_unsubscribe_does_not_resurrect_on_reconnect() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9952";
    let topic = "test/resurrect";
    let broker = Broker::start(broker_address).await?;

    let subscriber = Client::new("subscriber", ClientSettings::default());
    subscriber.connect(broker_address).await?;
    subscriber.subscribe(&[topic]).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    broker.stop();
    drop(broker);
    while subscriber.next_message().await.is_some() {}
    assert!(!subscriber.is_connected().await);

    subscriber.unsubscribe(&[topic]).await?;

    let _broker = Broker::start(broker_address).await?;
    while !subscriber.is_connected().await {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    let publisher = Client::new("publisher", test_settings());
    publisher.connect(broker_address).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let payload = TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    };
    publisher.publish(topic, &payload, Route::Global).await?;

    let message = tokio::time::timeout(Duration::from_secs(2), subscriber.next_message()).await;
    assert!(message.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_subscriptions_apply_on_connect() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9933";
    let _broker = Broker::start(broker_address).await?;

    let topic = "test/pending";
    let subscriber = Client::new("subscriber", test_settings());
    subscriber.subscribe(&[topic]).await?;
    subscriber.connect(broker_address).await?;

    let publisher = Client::new("publisher", test_settings());
    publisher.connect(broker_address).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let payload = TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    };
    publisher.publish(topic, &payload, Route::Global).await?;

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await?
        .expect("expected a message");
    assert_eq!(message.topic(), topic);
    Ok(())
}
