#![cfg(feature = "secure")]

use hearsay::{
    ClientSettings, Delivery, connect, create_client, create_udp_client, create_udp_client_secure,
    next_message, start_broker, start_udp_listener_secure, subscribe, udp_publish,
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
async fn a_matching_passphrase_joins_and_exchanges() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9971";
    let udp_address = "127.0.0.1:9972";
    let broker = start_broker(broker_address).await?;
    start_udp_listener_secure(&broker, udp_address, "swordfish").await?;

    let tcp_client = create_client("tcp_peer", test_settings());
    connect(&tcp_client, broker_address).await?;
    subscribe(&tcp_client, &["secure/topic"]).await?;

    let udp = create_udp_client_secure("udp_peer", udp_address, "swordfish").await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let payload = sample();
    udp_publish(&udp, "secure/topic", &payload, Delivery::ReliableOrdered).await?;

    let message = tokio::time::timeout(Duration::from_secs(5), next_message(&tcp_client))
        .await?
        .expect("a matching passphrase should deliver");
    assert_eq!(message.topic(), "secure/topic");
    let received: TestPayload = serde_json::from_str(message.text().unwrap())?;
    assert_eq!(received, payload);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_without_the_passphrase_is_not_heard() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9973";
    let udp_address = "127.0.0.1:9974";
    let broker = start_broker(broker_address).await?;
    start_udp_listener_secure(&broker, udp_address, "swordfish").await?;

    let tcp_client = create_client("tcp_peer", test_settings());
    connect(&tcp_client, broker_address).await?;
    subscribe(&tcp_client, &["secure/topic"]).await?;

    // A plaintext client (no passphrase): its datagrams do not decrypt at the
    // broker, so they are dropped and never reach the subscriber.
    let intruder = create_udp_client("intruder", udp_address).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;
    udp_publish(
        &intruder,
        "secure/topic",
        &sample(),
        Delivery::ReliableOrdered,
    )
    .await?;

    let heard = tokio::time::timeout(Duration::from_secs(2), next_message(&tcp_client)).await;
    assert!(
        heard.is_err(),
        "a peer without the passphrase must not be heard"
    );
    Ok(())
}
