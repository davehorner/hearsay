use crate::{
    Result,
    arena::PeerId,
    broker::{Broker, BrokerEvent, PeerWriter, process_incoming_frame, resolve_address},
};
use bytes::BytesMut;
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc::Sender, watch},
};
use tokio_tungstenite::tungstenite;

impl Broker {
    /// Starts a WebSocket transport on `address`, so browser and WASM peers can
    /// join this broker's bus. WebSocket peers speak the same wire protocol as
    /// TCP peers and share the broker's subscription table.
    pub async fn start_websocket_listener(&self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(resolve_address(address).await?).await?;
        tokio::spawn(websocket_accept_loop(
            listener,
            self.sender.clone(),
            self.shutdown_sender.subscribe(),
        ));
        Ok(())
    }
}

async fn websocket_accept_loop(
    listener: TcpListener,
    event_sender: Sender<BrokerEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _address)) => {
                    tokio::spawn(websocket_connection_task(event_sender.clone(), stream));
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            },
            _ = shutdown.changed() => break,
        }
    }
}

async fn websocket_connection_task(event_sender: Sender<BrokerEvent>, stream: TcpStream) {
    if let Ok(address) = stream.peer_addr()
        && !address.ip().is_loopback()
    {
        let _ = stream.set_nodelay(true);
    }
    let Ok(websocket) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };
    let (sink, mut messages) = websocket.split();
    let mut writer: Option<PeerWriter> = Some(PeerWriter::WebSocket(sink));
    let mut peer: Option<(PeerId, Arc<str>)> = None;
    let mut shutdown_signal = None;
    while let Some(Ok(message)) = messages.next().await {
        let frame = match message {
            tungstenite::Message::Binary(data) => match data.try_into_mut() {
                Ok(frame) => frame,
                Err(shared) => BytesMut::from(&shared[..]),
            },
            tungstenite::Message::Close(_) => break,
            _ => continue,
        };
        if !process_incoming_frame(
            frame,
            &event_sender,
            &mut writer,
            &mut peer,
            &mut shutdown_signal,
        )
        .await
        {
            break;
        }
    }
    drop(shutdown_signal);
}
