#![cfg(feature = "udp")]

use hearsay::{
    ClientSettings, Delivery, Route, connect, create_client, create_udp_client, next_message,
    next_udp_message, publish, start_broker, start_udp_listener, subscribe, udp_publish,
    udp_subscribe,
};
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
    let broker = start_broker(broker_address).await?;
    start_udp_listener(&broker, udp_address).await?;

    let tcp_client = create_client("tcp_peer", test_settings());
    connect(&tcp_client, broker_address).await?;
    subscribe(&tcp_client, &["from/udp"]).await?;

    let udp = create_udp_client("udp_peer", udp_address).await?;
    udp_subscribe(&udp, &["from/tcp"]).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let payload = sample();
    udp_publish(&udp, "from/udp", &payload, Delivery::ReliableOrdered).await?;

    let message = tokio::time::timeout(Duration::from_secs(5), next_message(&tcp_client))
        .await?
        .expect("tcp peer should receive the udp publish");
    assert_eq!(message.topic(), "from/udp");
    let received: TestPayload = serde_json::from_str(message.text().unwrap())?;
    assert_eq!(received, payload);

    publish(&tcp_client, "from/tcp", &payload, Route::Global).await?;

    let message = tokio::time::timeout(Duration::from_secs(5), next_udp_message(&udp))
        .await?
        .expect("udp peer should receive the tcp publish");
    assert_eq!(message.topic(), "from/tcp");
    let received: TestPayload = serde_json::from_str(message.text().unwrap())?;
    assert_eq!(received, payload);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn reliable_and_unreliable_publishes_both_deliver() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9965";
    let udp_address = "127.0.0.1:9966";
    let broker = start_broker(broker_address).await?;
    start_udp_listener(&broker, udp_address).await?;

    let tcp_client = create_client("tcp_peer", test_settings());
    connect(&tcp_client, broker_address).await?;
    subscribe(&tcp_client, &["updates", "events"]).await?;

    let udp = create_udp_client("udp_peer", udp_address).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    udp_publish(&udp, "updates", &sample(), Delivery::Unreliable).await?;
    udp_publish(&udp, "events", &sample(), Delivery::ReliableOrdered).await?;

    let mut seen_updates = false;
    let mut seen_events = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while (!seen_updates || !seen_events) && tokio::time::Instant::now() < deadline {
        let Ok(Some(message)) =
            tokio::time::timeout(Duration::from_secs(2), next_message(&tcp_client)).await
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
    let broker = start_broker(broker_address).await?;
    start_udp_listener(&broker, udp_address).await?;

    let publisher = create_client("publisher", test_settings());
    connect(&publisher, broker_address).await?;

    let first = create_udp_client("first", udp_address).await?;
    let second = create_udp_client("second", udp_address).await?;
    udp_subscribe(&first, &["shared"]).await?;
    udp_subscribe(&second, &["shared"]).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    publish(&publisher, "shared", &sample(), Route::Global).await?;

    let first_message = tokio::time::timeout(Duration::from_secs(5), next_udp_message(&first))
        .await?
        .expect("first udp client should receive");
    let second_message = tokio::time::timeout(Duration::from_secs(5), next_udp_message(&second))
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
    let broker = start_broker(broker_address).await?;
    start_udp_listener(&broker, udp_address).await?;

    let subscriber = create_client("subscriber", test_settings());
    connect(&subscriber, broker_address).await?;
    subscribe(&subscriber, &["big"]).await?;

    let udp = create_udp_client("udp_peer", udp_address).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    // A payload many fragments long: the client fragments it on the reliable
    // channel and the broker's endpoint reassembles it before delivery.
    let big: Vec<u32> = (0..4000).collect();
    udp_publish(&udp, "big", &big, Delivery::ReliableOrdered).await?;

    let message = tokio::time::timeout(Duration::from_secs(5), next_message(&subscriber))
        .await?
        .expect("large reliable message should reassemble");
    assert_eq!(message.topic(), "big");
    let received: Vec<u32> = serde_json::from_str(message.text().unwrap())?;
    assert_eq!(received, big);
    Ok(())
}
