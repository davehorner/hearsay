use hearsay::{MessageBus, MockBus, Publisher, Reception, Route, Subscriber};
use std::collections::HashSet;

async fn echo_service(bus: &impl MessageBus) -> Option<String> {
    let message = bus.next_message().await?;
    let topic = message.topic().to_string();
    bus.publish(&format!("{topic}/ack"), &"ok", Route::Global)
        .await
        .ok()?;
    Some(topic)
}

#[tokio::test]
async fn records_publishes() {
    let bus = MockBus::new();
    bus.publish("greetings", &"hello", Route::Global)
        .await
        .unwrap();
    bus.publish_bytes("blob", &[1, 2, 3], Route::Local)
        .await
        .unwrap();

    let published = bus.published();
    assert_eq!(published.len(), 2);
    assert_eq!(published[0].topic, "greetings");
    assert!(published[0].binary);
    assert_eq!(published[0].route, Route::Global);
    assert_eq!(published[1].topic, "blob");
    assert!(published[1].binary);
    assert_eq!(published[1].route, Route::Local);
    assert_eq!(published[1].payload.to_vec(), vec![1, 2, 3]);
}

#[tokio::test]
async fn delivers_injected_messages_in_order() {
    let bus = MockBus::new();
    bus.inject_text("events", "\"first\"").unwrap();
    bus.inject_text("events", "\"second\"").unwrap();

    let first = bus.next_message().await.unwrap();
    assert_eq!(first.topic(), "events");
    assert_eq!(first.text(), Some("\"first\""));
    let second = bus.next_message().await.unwrap();
    assert_eq!(second.text(), Some("\"second\""));
}

#[tokio::test]
async fn service_logic_runs_against_the_mock() {
    let bus = MockBus::new();
    bus.inject_text("commands/run", "\"go\"").unwrap();

    let handled = echo_service(&bus).await;
    assert_eq!(handled.as_deref(), Some("commands/run"));

    let published = bus.published();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].topic, "commands/run/ack");
}

#[tokio::test]
async fn tracks_subscriptions_and_polls_without_blocking() {
    let bus = MockBus::new();
    bus.subscribe(&["a".to_string(), "b".to_string()])
        .await
        .unwrap();
    bus.unsubscribe(&["a".to_string()]).await.unwrap();
    assert_eq!(bus.subscriptions(), HashSet::from(["b".to_string()]));

    assert!(matches!(bus.try_next_message().await, Reception::Empty));
    bus.inject_bytes("b", &[9]).unwrap();
    match bus.try_next_message().await {
        Reception::Message(message) => assert_eq!(message.bytes(), Some(&[9][..])),
        other => panic!("expected a message, got {other:?}"),
    }
}
