//! Topic-based pub/sub messaging over TCP, built on tokio.

#[cfg(not(target_arch = "wasm32"))]
mod arena;
mod batch;
#[cfg(not(target_arch = "wasm32"))]
mod bridge;
#[cfg(not(target_arch = "wasm32"))]
mod broker;
#[cfg(not(target_arch = "wasm32"))]
mod bus;
#[cfg(not(target_arch = "wasm32"))]
mod client;
mod contract;
#[cfg(not(target_arch = "wasm32"))]
mod lifecycle;
#[cfg(not(target_arch = "wasm32"))]
mod mock;
#[cfg(all(feature = "nat", not(target_arch = "wasm32")))]
mod nat;
#[cfg(all(feature = "udp", not(target_arch = "wasm32")))]
mod reliability;
#[cfg(all(feature = "spawn", not(target_arch = "wasm32")))]
mod spawn;
#[cfg(all(feature = "udp", not(target_arch = "wasm32")))]
mod udp;
#[cfg(all(feature = "udp", not(target_arch = "wasm32")))]
mod udp_client;
#[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
mod websocket;
mod wire;

pub use bytes::Bytes;
pub use enum2contract;

pub use self::{
    contract::*,
    wire::{
        Message, decode_message, encode_hello, encode_publish_binary, encode_publish_text,
        encode_subscribe, encode_unsubscribe,
    },
};

#[cfg(not(target_arch = "wasm32"))]
pub use self::{batch::*, broker::*, bus::*, client::*, lifecycle::*, mock::*};

#[cfg(all(feature = "spawn", not(target_arch = "wasm32")))]
pub use self::spawn::*;

#[cfg(all(feature = "udp", not(target_arch = "wasm32")))]
pub use self::{reliability::Delivery, udp_client::*};

#[cfg(all(feature = "nat", not(target_arch = "wasm32")))]
pub use self::nat::*;

#[derive(Debug)]
pub enum Error {
    NotConnected,
    MaxConnectionAttempts,
    AddressResolution,
    FrameTooLarge,
    TopicTooLong,
    AppNotFound(String),
    AppAlreadyRunning(String),
    Serialization(String),
    Transport(String),
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotConnected => write!(formatter, "client is not connected"),
            Error::MaxConnectionAttempts => {
                write!(formatter, "maximum connection attempts reached")
            }
            Error::AddressResolution => write!(formatter, "could not resolve address"),
            Error::FrameTooLarge => write!(formatter, "frame length exceeds maximum"),
            Error::TopicTooLong => write!(formatter, "topic or identifier exceeds maximum length"),
            Error::AppNotFound(name) => write!(formatter, "app '{name}' not found"),
            Error::AppAlreadyRunning(name) => write!(formatter, "app '{name}' is already running"),
            Error::Serialization(message) => write!(formatter, "serialization error: {message}"),
            Error::Transport(message) => write!(formatter, "transport error: {message}"),
            Error::Other(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Error::Other(message)
    }
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Error::Other(message.to_string())
    }
}

impl From<postcard::Error> for Error {
    fn from(error: postcard::Error) -> Self {
        Error::Serialization(error.to_string())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Transport(error.to_string())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Serialization(error.to_string())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<tokio::time::error::Elapsed> for Error {
    fn from(error: tokio::time::error::Elapsed) -> Self {
        Error::Transport(error.to_string())
    }
}

#[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Error::Transport(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
