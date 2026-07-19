use hearsay::{
    BridgeCreatedPayload, Broker, BrokerContract, Client, ClientSettings, ReportBridgesPayload,
    Route,
};
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

async fn count_bridges(broker_address: &str) -> usize {
    let inspector = Client::new("inspector", test_settings());
    inspector.connect(broker_address).await.unwrap();
    inspector
        .subscribe(&[&BrokerContract::report_bridges_topic()])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (request_topic, request_payload) = BrokerContract::request_bridges();
    inspector
        .publish(&request_topic, &request_payload, Route::Local)
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let Some(message) = inspector.next_message().await else {
                continue;
            };
            if message.topic() == BrokerContract::report_bridges_topic() {
                return message;
            }
        }
    })
    .await
    .unwrap();
    ReportBridgesPayload::from_json(message.text().unwrap())
        .unwrap()
        .bridges
        .len()
}

#[tokio::test(flavor = "multi_thread")]
async fn bridged_brokers_deliver_messages() -> hearsay::Result<()> {
    let first_broker_address = "127.0.0.1:9934";
    let second_broker_address = "127.0.0.1:9935";
    let _first_broker = Broker::start(first_broker_address).await?;
    let _second_broker = Broker::start(second_broker_address).await?;

    let topic = "test/bridged";
    let first_client = Client::new("first", test_settings());
    first_client.connect(first_broker_address).await?;
    first_client.subscribe(&[topic]).await?;

    let bridge_requester = Client::new("bridge_requester", test_settings());
    bridge_requester.connect(second_broker_address).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    bridge_requester
        .open_bridge(second_broker_address, first_broker_address)
        .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let second_client = Client::new("second", test_settings());
    second_client.connect(second_broker_address).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload = TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    };
    second_client
        .publish(topic, &payload, Route::Global)
        .await?;

    let message = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let Some(message) = first_client.next_message().await else {
                continue;
            };
            if message.topic() == topic {
                return message;
            }
        }
    })
    .await?;
    let received: TestPayload = message.read()?;
    assert_eq!(received, payload);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn local_route_does_not_cross_bridges() -> hearsay::Result<()> {
    let first_broker_address = "127.0.0.1:9953";
    let second_broker_address = "127.0.0.1:9954";
    let _first_broker = Broker::start(first_broker_address).await?;
    let _second_broker = Broker::start(second_broker_address).await?;

    let topic = "test/local";
    let remote_subscriber = Client::new("remote", test_settings());
    remote_subscriber.connect(first_broker_address).await?;
    remote_subscriber.subscribe(&[topic]).await?;

    let local_subscriber = Client::new("local", test_settings());
    local_subscriber.connect(second_broker_address).await?;
    local_subscriber.subscribe(&[topic]).await?;

    let bridge_requester = Client::new("bridge_requester", test_settings());
    bridge_requester.connect(second_broker_address).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    bridge_requester
        .open_bridge(second_broker_address, first_broker_address)
        .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let payload = TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    };
    bridge_requester
        .publish(topic, &payload, Route::Local)
        .await?;

    let local_message = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let Some(message) = local_subscriber.next_message().await else {
                continue;
            };
            if message.topic() == topic {
                return message;
            }
        }
    })
    .await?;
    let received: TestPayload = local_message.read()?;
    assert_eq!(received, payload);

    let remote_message = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let Some(message) = remote_subscriber.next_message().await else {
                continue;
            };
            if message.topic() == topic {
                return message;
            }
        }
    })
    .await;
    assert!(remote_message.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn bidirectional_bridge_delivers_each_message_once() -> hearsay::Result<()> {
    let first_broker_address = "127.0.0.1:9955";
    let second_broker_address = "127.0.0.1:9956";
    let _first_broker = Broker::start(first_broker_address).await?;
    let _second_broker = Broker::start(second_broker_address).await?;

    let topic = "test/once";
    let first_client = Client::new("first", test_settings());
    first_client.connect(first_broker_address).await?;
    first_client.subscribe(&[topic]).await?;

    let bridge_requester = Client::new("bridge_requester", test_settings());
    bridge_requester.connect(second_broker_address).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    bridge_requester
        .open_bridge(second_broker_address, first_broker_address)
        .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let second_client = Client::new("second", test_settings());
    second_client.connect(second_broker_address).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload = TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    };
    second_client
        .publish(topic, &payload, Route::Global)
        .await?;

    let mut deliveries = 0;
    let collect = async {
        loop {
            let Some(message) = first_client.next_message().await else {
                continue;
            };
            if message.topic() == topic {
                deliveries += 1;
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(2), collect).await;
    assert_eq!(
        deliveries, 1,
        "expected exactly one delivery, saw {deliveries}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_a_bridge_tears_down_both_directions() -> hearsay::Result<()> {
    let first_broker_address = "127.0.0.1:9957";
    let second_broker_address = "127.0.0.1:9958";
    let _first_broker = Broker::start(first_broker_address).await?;
    let _second_broker = Broker::start(second_broker_address).await?;

    let bridge_requester = Client::new("bridge_requester", test_settings());
    bridge_requester.connect(second_broker_address).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    bridge_requester
        .open_bridge(second_broker_address, first_broker_address)
        .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_eq!(count_bridges(first_broker_address).await, 1);
    assert_eq!(count_bridges(second_broker_address).await, 1);

    bridge_requester.close_bridge(first_broker_address).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(count_bridges(second_broker_address).await, 0);
    assert_eq!(count_bridges(first_broker_address).await, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mesh_paths_deliver_each_message_once() -> hearsay::Result<()> {
    let a_address = "127.0.0.1:9960";
    let b_address = "127.0.0.1:9961";
    let c_address = "127.0.0.1:9962";
    let _a_broker = Broker::start(a_address).await?;
    let _b_broker = Broker::start(b_address).await?;
    let _c_broker = Broker::start(c_address).await?;

    let topic = "test/mesh";
    let consumer = Client::new("consumer", test_settings());
    consumer.connect(c_address).await?;
    consumer.subscribe(&[topic]).await?;

    let opener_ab = Client::new("opener_ab", test_settings());
    opener_ab.connect(a_address).await?;
    let opener_bc = Client::new("opener_bc", test_settings());
    opener_bc.connect(b_address).await?;
    let opener_ac = Client::new("opener_ac", test_settings());
    opener_ac.connect(a_address).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    opener_ab.open_bridge(a_address, b_address).await?;
    opener_bc.open_bridge(b_address, c_address).await?;
    opener_ac.open_bridge(a_address, c_address).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let publisher = Client::new("publisher", test_settings());
    publisher.connect(a_address).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload = TestPayload {
        name: "Matthew".to_string(),
        age: 30,
    };
    publisher.publish(topic, &payload, Route::Global).await?;

    let mut deliveries = 0;
    let collect = async {
        loop {
            let Some(message) = consumer.next_message().await else {
                continue;
            };
            if message.topic() == topic {
                deliveries += 1;
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(3), collect).await;
    assert_eq!(
        deliveries, 1,
        "expected exactly one delivery across redundant mesh paths, saw {deliveries}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn bridge_creation_is_announced() -> hearsay::Result<()> {
    let first_broker_address = "127.0.0.1:9936";
    let second_broker_address = "127.0.0.1:9937";
    let _first_broker = Broker::start(first_broker_address).await?;
    let _second_broker = Broker::start(second_broker_address).await?;

    let first_client = Client::new("first", test_settings());
    first_client.connect(first_broker_address).await?;
    first_client
        .subscribe(&[&BrokerContract::bridge_created_topic()])
        .await?;

    let bridge_requester = Client::new("bridge_requester", test_settings());
    bridge_requester.connect(second_broker_address).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    bridge_requester
        .open_bridge(second_broker_address, first_broker_address)
        .await?;

    let message = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let Some(message) = first_client.next_message().await else {
                continue;
            };
            if message.topic() == BrokerContract::bridge_created_topic() {
                return message;
            }
        }
    })
    .await?;
    assert!(BridgeCreatedPayload::from_json(message.text().unwrap()).is_ok());
    Ok(())
}
