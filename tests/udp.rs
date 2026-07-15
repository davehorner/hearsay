#![cfg(feature = "udp")]

use hearsay::{
    Bytes, ClientSettings, Message, Route, connect, create_client, decode_message, encode_hello,
    encode_publish_text, encode_subscribe, next_message, publish, start_broker, start_udp_listener,
    subscribe,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::net::UdpSocket;

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

async fn send_datagram(socket: &UdpSocket, broker: &str, frame: &[u8]) -> hearsay::Result<()> {
    socket.send_to(frame, broker).await?;
    Ok(())
}

async fn receive_message(socket: &UdpSocket, within: Duration) -> Option<Message> {
    tokio::time::timeout(within, async {
        let mut buffer = vec![0_u8; 65_536];
        loop {
            let Ok((length, _address)) = socket.recv_from(&mut buffer).await else {
                continue;
            };
            if let Some(message) = decode_message(Bytes::copy_from_slice(&buffer[..length])) {
                return message;
            }
        }
    })
    .await
    .ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn udp_and_tcp_peers_interoperate() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9963";
    let udp_address = "127.0.0.1:9964";
    let broker = start_broker(broker_address).await?;
    start_udp_listener(&broker, udp_address).await?;

    let tcp_client = create_client("tcp_peer", test_settings());
    connect(&tcp_client, broker_address).await?;
    subscribe(&tcp_client, &["from/udp"]).await?;

    let udp_socket = UdpSocket::bind("127.0.0.1:0").await?;
    send_datagram(&udp_socket, udp_address, &encode_hello("udp_peer")?).await?;
    send_datagram(&udp_socket, udp_address, &encode_subscribe("from/tcp")?).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let payload = sample();
    send_datagram(
        &udp_socket,
        udp_address,
        &encode_publish_text("from/udp", &serde_json::to_string(&payload)?, false)?,
    )
    .await?;

    let message = tokio::time::timeout(Duration::from_secs(5), next_message(&tcp_client))
        .await?
        .expect("expected a message from the udp peer");
    assert_eq!(message.topic(), "from/udp");
    let received: TestPayload = serde_json::from_str(message.text().unwrap())?;
    assert_eq!(received, payload);

    publish(&tcp_client, "from/tcp", &payload, Route::Global).await?;

    let message = receive_message(&udp_socket, Duration::from_secs(5))
        .await
        .expect("expected a message from the tcp peer");
    assert_eq!(message.topic(), "from/tcp");
    let received: TestPayload = serde_json::from_str(message.text().unwrap())?;
    assert_eq!(received, payload);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn datagrams_before_hello_do_not_register_a_peer() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9965";
    let udp_address = "127.0.0.1:9966";
    let broker = start_broker(broker_address).await?;
    start_udp_listener(&broker, udp_address).await?;

    let publisher = create_client("publisher", test_settings());
    connect(&publisher, broker_address).await?;

    let udp_socket = UdpSocket::bind("127.0.0.1:0").await?;
    // Subscribe without a preceding Hello: the address must not be registered,
    // so this subscription is never established.
    send_datagram(&udp_socket, udp_address, &encode_subscribe("topic")?).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    publish(&publisher, "topic", &sample(), Route::Global).await?;
    assert!(
        receive_message(&udp_socket, Duration::from_millis(750))
            .await
            .is_none(),
        "an unregistered peer must not receive messages"
    );

    // After a proper Hello, the same address subscribes and receives.
    send_datagram(&udp_socket, udp_address, &encode_hello("late_peer")?).await?;
    send_datagram(&udp_socket, udp_address, &encode_subscribe("topic")?).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    publish(&publisher, "topic", &sample(), Route::Global).await?;
    let message = receive_message(&udp_socket, Duration::from_secs(5))
        .await
        .expect("registered peer should receive");
    assert_eq!(message.topic(), "topic");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_udp_peers_demux_by_address() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9967";
    let udp_address = "127.0.0.1:9968";
    let broker = start_broker(broker_address).await?;
    start_udp_listener(&broker, udp_address).await?;

    let publisher = create_client("publisher", test_settings());
    connect(&publisher, broker_address).await?;

    let first = UdpSocket::bind("127.0.0.1:0").await?;
    let second = UdpSocket::bind("127.0.0.1:0").await?;
    for (index, socket) in [&first, &second].into_iter().enumerate() {
        send_datagram(
            socket,
            udp_address,
            &encode_hello(&format!("peer_{index}"))?,
        )
        .await?;
        send_datagram(socket, udp_address, &encode_subscribe("shared")?).await?;
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    publish(&publisher, "shared", &sample(), Route::Global).await?;

    let first_message = receive_message(&first, Duration::from_secs(5))
        .await
        .expect("first peer should receive");
    let second_message = receive_message(&second, Duration::from_secs(5))
        .await
        .expect("second peer should receive");
    assert_eq!(first_message.topic(), "shared");
    assert_eq!(second_message.topic(), "shared");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_datagram_does_not_disconnect_the_peer() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9969";
    let udp_address = "127.0.0.1:9970";
    let broker = start_broker(broker_address).await?;
    start_udp_listener(&broker, udp_address).await?;

    let publisher = create_client("publisher", test_settings());
    connect(&publisher, broker_address).await?;

    let udp_socket = UdpSocket::bind("127.0.0.1:0").await?;
    send_datagram(&udp_socket, udp_address, &encode_hello("resilient")?).await?;
    send_datagram(&udp_socket, udp_address, &encode_subscribe("topic")?).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    // A length prefix claiming a 50-byte body with no body: parsing rejects it.
    send_datagram(&udp_socket, udp_address, &[0, 0, 0, 50]).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The peer must still be registered and receiving after the bad datagram.
    publish(&publisher, "topic", &sample(), Route::Global).await?;
    let message = receive_message(&udp_socket, Duration::from_secs(5))
        .await
        .expect("peer should survive a malformed datagram");
    assert_eq!(message.topic(), "topic");
    Ok(())
}
