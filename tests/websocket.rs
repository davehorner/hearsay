#![cfg(feature = "websockets")]

use futures_util::{SinkExt, StreamExt};
use hearsay::{
    Bytes, ClientSettings, Route, connect, create_client, decode_message, encode_hello,
    encode_publish_text, encode_subscribe, next_message, publish, start_broker,
    start_websocket_listener, subscribe,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TestPayload {
    name: String,
    age: u8,
}

type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn send_frame(websocket: &mut WebSocket, frame: Bytes) -> hearsay::Result<()> {
    websocket.send(tungstenite::Message::Binary(frame)).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_and_tcp_peers_interoperate() -> hearsay::Result<()> {
    let broker_address = "127.0.0.1:9939";
    let websocket_address = "127.0.0.1:9940";
    let broker = start_broker(broker_address).await?;
    start_websocket_listener(&broker, websocket_address).await?;

    let tcp_client = create_client(
        "tcp_peer",
        ClientSettings {
            autoreconnect: false,
            ..Default::default()
        },
    );
    connect(&tcp_client, broker_address).await?;
    subscribe(&tcp_client, &["from/websocket"]).await?;

    let (mut websocket, _response) =
        tokio_tungstenite::connect_async(format!("ws://{websocket_address}")).await?;
    send_frame(&mut websocket, encode_hello("websocket_peer")?).await?;
    send_frame(&mut websocket, encode_subscribe("from/tcp")?).await?;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let payload = TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    };
    send_frame(
        &mut websocket,
        encode_publish_text("from/websocket", &serde_json::to_string(&payload)?, false)?,
    )
    .await?;

    let message = tokio::time::timeout(Duration::from_secs(5), next_message(&tcp_client))
        .await?
        .expect("expected a message from the websocket peer");
    assert_eq!(message.topic(), "from/websocket");
    let received: TestPayload = serde_json::from_str(message.text().unwrap())?;
    assert_eq!(received, payload);

    publish(&tcp_client, "from/tcp", &payload, Route::Global).await?;

    let message = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame = websocket
                .next()
                .await
                .expect("websocket closed unexpectedly")
                .expect("websocket read failed");
            if let tungstenite::Message::Binary(data) = frame {
                return decode_message(data).expect("failed to decode broker message");
            }
        }
    })
    .await?;
    assert_eq!(message.topic(), "from/tcp");
    let received: TestPayload = serde_json::from_str(message.text().unwrap())?;
    assert_eq!(received, payload);
    Ok(())
}
