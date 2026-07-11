//! The page's hearsay peer: a WebSocket session against the broker's
//! websocket listener, speaking the hearsay wire format directly. Each
//! frame in both directions is one length-prefixed hearsay frame, encoded
//! and decoded with the `hearsay::encode_*` / `hearsay::decode_message`
//! helpers.

use leptos::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use web_sys::{BinaryType, MessageEvent, WebSocket};

use crate::bridge::Bridge;
use crate::shell;
use crate::state::DemoState;

pub const BROKER_WEBSOCKET_URL: &str = "ws://127.0.0.1:9613";
pub const TEXT_TOPIC: &str = "template/text";
pub const BINARY_TOPIC: &str = "template/binary";
pub const SPAWN_TOPIC: &str = "template/spawn";
const RECONNECT_DELAY_MILLISECONDS: i32 = 2000;

#[derive(Clone)]
pub struct HearsayLink {
    pub socket: WebSocket,
    pub client_id: String,
}

pub type HearsaySlot = StoredValue<Option<HearsayLink>, LocalStorage>;
pub type BridgeSlot = StoredValue<Option<Bridge>, LocalStorage>;

/// Opens the websocket session and keeps reconnecting until a broker is
/// listening. On open it sends `Hello`, subscribes to the demo and shell
/// topics, and runs the shell's connection hook.
pub fn connect(state: DemoState, slot: HearsaySlot, bridge: BridgeSlot) {
    let Ok(socket) = WebSocket::new(BROKER_WEBSOCKET_URL) else {
        schedule_reconnect(state, slot, bridge);
        return;
    };
    socket.set_binary_type(BinaryType::Arraybuffer);
    let client_id = format!(
        "leptos-{:08x}",
        (js_sys::Math::random() * u32::MAX as f64) as u32
    );
    state.hearsay_client_id.set(client_id.clone());

    let open_socket = socket.clone();
    let open_id = client_id.clone();
    let onopen = Closure::<dyn FnMut()>::new(move || {
        state.hearsay_connected.set(true);
        send_frame(&open_socket, hearsay::encode_hello(&open_id));
        let mut topics = vec![
            TEXT_TOPIC.to_string(),
            BINARY_TOPIC.to_string(),
            SPAWN_TOPIC.to_string(),
        ];
        topics.extend(shell::shell_topics(state));
        for topic in topics {
            send_frame(&open_socket, hearsay::encode_subscribe(&topic));
        }
        shell::on_connected(state, slot);
    });
    socket.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    onopen.forget();

    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
        let Ok(buffer) = event.data().dyn_into::<js_sys::ArrayBuffer>() else {
            return;
        };
        let bytes = js_sys::Uint8Array::new(&buffer).to_vec();
        let Some(message) = hearsay::decode_message(hearsay::Bytes::from(bytes)) else {
            return;
        };
        if !shell::handle_shell_topic(state, slot, &message) {
            shell::handle_panel_topics(state, bridge, &message);
        }
    });
    socket.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();

    let onclose = Closure::<dyn FnMut()>::new(move || {
        state.hearsay_connected.set(false);
        slot.set_value(None);
        schedule_reconnect(state, slot, bridge);
    });
    socket.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    onclose.forget();

    slot.set_value(Some(HearsayLink { socket, client_id }));
}

fn schedule_reconnect(state: DemoState, slot: HearsaySlot, bridge: BridgeSlot) {
    shell::set_page_timeout(
        move || connect(state, slot, bridge),
        RECONNECT_DELAY_MILLISECONDS,
    );
}

fn send_frame(socket: &WebSocket, frame: hearsay::Result<hearsay::Bytes>) {
    if socket.ready_state() != WebSocket::OPEN {
        return;
    }
    if let Ok(frame) = frame {
        let _ = socket.send_with_u8_array(&frame);
    }
}

fn with_link(slot: HearsaySlot, body: impl FnOnce(&HearsayLink)) {
    slot.with_value(|link| {
        if let Some(link) = link {
            body(link);
        }
    });
}

pub fn publish_text(slot: HearsaySlot, topic: &str, payload: &str) {
    with_link(slot, |link| {
        send_frame(
            &link.socket,
            hearsay::encode_publish_text(topic, payload, false),
        );
    });
}

pub fn publish_binary(slot: HearsaySlot, topic: &str, payload: Vec<u8>) {
    with_link(slot, |link| {
        send_frame(
            &link.socket,
            hearsay::encode_publish_binary(topic, &payload, false),
        );
    });
}
