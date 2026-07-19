#![cfg(feature = "udp")]

use hearsay::{Broker, Client, ClientSettings, Delivery, Route, UdpClient};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TestPayload {
    name: String,
    age: u8,
}

fn sample() -> TestPayload {
    TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    }
}

fn test_settings() -> ClientSettings {
    ClientSettings {
        autoreconnect: false,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn udp_client_and_tcp_peer_interoperate() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9963";
    let udp_address = "127.0.0.1:9964";
    let broker = Broker::start(broker_address).await?;
    broker.start_udp_listener(udp_address).await?;

    let tcp_client = Client::new("tcp_peer", test_settings());
    tcp_client.connect(broker_address).await?;
    tcp_client.subscribe(&["from/udp"]).await?;

    let udp = UdpClient::connect("udp_peer", udp_address).await?;
    udp.subscribe(&["from/tcp"]).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let payload = sample();
    udp.publish("from/udp", &payload, Delivery::ReliableOrdered)
        .await?;

    let message = tokio::time::timeout(Duration::from_secs(5), tcp_client.next_message())
        .await?
        .expect("tcp peer should receive the udp publish");
    assert_eq!(message.topic(), "from/udp");
    let received: TestPayload = message.read()?;
    assert_eq!(received, payload);

    tcp_client
        .publish("from/tcp", &payload, Route::Global)
        .await?;

    let message = tokio::time::timeout(Duration::from_secs(5), udp.next_message())
        .await?
        .expect("udp peer should receive the tcp publish");
    assert_eq!(message.topic(), "from/tcp");
    let received: TestPayload = message.read()?;
    assert_eq!(received, payload);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn reliable_and_unreliable_publishes_both_deliver() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9965";
    let udp_address = "127.0.0.1:9966";
    let broker = Broker::start(broker_address).await?;
    broker.start_udp_listener(udp_address).await?;

    let tcp_client = Client::new("tcp_peer", test_settings());
    tcp_client.connect(broker_address).await?;
    tcp_client.subscribe(&["updates", "events"]).await?;

    let udp = UdpClient::connect("udp_peer", udp_address).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    udp.publish("updates", &sample(), Delivery::Unreliable)
        .await?;
    udp.publish("events", &sample(), Delivery::ReliableOrdered)
        .await?;

    let mut seen_updates = false;
    let mut seen_events = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while (!seen_updates || !seen_events) && tokio::time::Instant::now() < deadline {
        let Ok(Some(message)) =
            tokio::time::timeout(Duration::from_secs(2), tcp_client.next_message()).await
        else {
            break;
        };
        match message.topic() {
            "updates" => seen_updates = true,
            "events" => seen_events = true,
            _ => {}
        }
    }
    assert!(seen_updates, "unreliable publish should arrive on loopback");
    assert!(seen_events, "reliable publish should arrive");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_udp_clients_each_receive() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9967";
    let udp_address = "127.0.0.1:9968";
    let broker = Broker::start(broker_address).await?;
    broker.start_udp_listener(udp_address).await?;

    let publisher = Client::new("publisher", test_settings());
    publisher.connect(broker_address).await?;

    let first = UdpClient::connect("first", udp_address).await?;
    let second = UdpClient::connect("second", udp_address).await?;
    first.subscribe(&["shared"]).await?;
    second.subscribe(&["shared"]).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    publisher
        .publish("shared", &sample(), Route::Global)
        .await?;

    let first_message = tokio::time::timeout(Duration::from_secs(5), first.next_message())
        .await?
        .expect("first udp client should receive");
    let second_message = tokio::time::timeout(Duration::from_secs(5), second.next_message())
        .await?
        .expect("second udp client should receive");
    assert_eq!(first_message.topic(), "shared");
    assert_eq!(second_message.topic(), "shared");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_reliable_message_fragments_and_reassembles_over_udp() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9969";
    let udp_address = "127.0.0.1:9970";
    let broker = Broker::start(broker_address).await?;
    broker.start_udp_listener(udp_address).await?;

    let subscriber = Client::new("subscriber", test_settings());
    subscriber.connect(broker_address).await?;
    subscriber.subscribe(&["big"]).await?;

    let udp = UdpClient::connect("udp_peer", udp_address).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    // A payload many fragments long: the client fragments it on the reliable
    // channel and the broker's endpoint reassembles it before delivery.
    let big: Vec<u32> = (0..4000).collect();
    udp.publish("big", &big, Delivery::ReliableOrdered).await?;

    let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next_message())
        .await?
        .expect("large reliable message should reassemble");
    assert_eq!(message.topic(), "big");
    let received: Vec<u32> = message.read()?;
    assert_eq!(received, big);
    Ok(())
}
