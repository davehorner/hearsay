use crate::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use serde::de::DeserializeOwned;
use std::ops::Range;

pub(crate) const MAX_FRAME_LENGTH: usize = 64 * 1024 * 1024;

const KIND_OFFSET: usize = 4;
const HEADER_LENGTH: usize = 8;

pub(crate) const FLAG_BINARY: u8 = 1;
pub(crate) const FLAG_LOCAL_ONLY: u8 = 2;

const KIND_HELLO: u8 = 1;
const KIND_SUBSCRIBE: u8 = 2;
const KIND_UNSUBSCRIBE: u8 = 3;
const KIND_PUBLISH: u8 = 4;
const KIND_DELIVER: u8 = 5;

/// A message delivered to a subscriber, backed by the received wire frame.
///
/// Cloning is cheap: the frame buffer is reference-counted and shared.
#[derive(Clone)]
pub struct Message {
    frame: Bytes,
    topic: Range<usize>,
    payload: Range<usize>,
    binary: bool,
}

impl Message {
    pub fn topic(&self) -> &str {
        std::str::from_utf8(&self.frame[self.topic.clone()]).unwrap_or_default()
    }

    /// The text payload, or `None` if this message carries binary data.
    pub fn text(&self) -> Option<&str> {
        if self.binary {
            return None;
        }
        std::str::from_utf8(&self.frame[self.payload.clone()]).ok()
    }

    /// The binary payload, or `None` if this message carries text.
    pub fn bytes(&self) -> Option<&[u8]> {
        if self.binary {
            Some(&self.frame[self.payload.clone()])
        } else {
            None
        }
    }

    /// Decodes the binary payload as a single [postcard]-encoded value of type
    /// `T`, the receive half of the default [`crate::Client::publish`].
    ///
    /// Returns an error if the message carries text rather than binary, or if
    /// the bytes do not decode into `T`.
    ///
    /// [postcard]: https://docs.rs/postcard
    pub fn read<T: DeserializeOwned>(&self) -> Result<T> {
        let bytes = self
            .bytes()
            .ok_or_else(|| Error::from("message has no binary payload"))?;
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Decodes the text payload as a single JSON value of type `T`, the receive
    /// half of [`crate::Client::publish_json`].
    ///
    /// Returns an error if the message carries binary rather than text, or if
    /// the text does not decode into `T`.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn read_json<T: DeserializeOwned>(&self) -> Result<T> {
        let text = self
            .text()
            .ok_or_else(|| Error::from("message has no text payload"))?;
        Ok(serde_json::from_str(text)?)
    }

    /// Decodes the binary payload as a batch of `T` values, the receive half of
    /// a [`crate::Batch`] flush. Equivalent to reading a `Vec<T>` with
    /// [`Message::read`].
    pub fn read_batch<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.read()
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = formatter.debug_struct("Message");
        builder.field("topic", &self.topic());
        match self.text() {
            Some(text) => builder.field("text", &text),
            None => builder.field("bytes", &self.frame[self.payload.clone()].len()),
        };
        builder.finish()
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.binary == other.binary
            && self.frame[self.topic.clone()] == other.frame[other.topic.clone()]
            && self.frame[self.payload.clone()] == other.frame[other.payload.clone()]
    }
}

/// Decodes a broker-to-subscriber frame into a [`Message`].
///
/// This is the receive half of the wire protocol for clients that speak it
/// directly (browser and WASM peers); native clients decode internally.
pub fn decode_message(frame: Bytes) -> Option<Message> {
    if frame.len() < HEADER_LENGTH {
        return None;
    }
    let declared = read_u32(&frame[..4])? as usize;
    if declared + 4 != frame.len() {
        return None;
    }
    if frame[KIND_OFFSET] != KIND_DELIVER {
        return None;
    }
    let flags = frame[5];
    let topic_length = u16::from_be_bytes([frame[6], frame[7]]) as usize;
    let topic_end = HEADER_LENGTH + topic_length;
    if frame.len() < topic_end {
        return None;
    }
    let topic = HEADER_LENGTH..topic_end;
    let payload = topic_end..frame.len();
    std::str::from_utf8(&frame[topic.clone()]).ok()?;
    let binary = flags & FLAG_BINARY != 0;
    if !binary {
        std::str::from_utf8(&frame[payload.clone()]).ok()?;
    }
    Some(Message {
        frame,
        topic,
        payload,
        binary,
    })
}

fn read_u32(bytes: &[u8]) -> Option<u32> {
    let mut buffer = [0_u8; 4];
    buffer.copy_from_slice(bytes.get(..4)?);
    Some(u32::from_be_bytes(buffer))
}

fn put_string_field(buffer: &mut BytesMut, value: &[u8]) -> Result<()> {
    if value.len() > u16::MAX as usize {
        return Err(Error::TopicTooLong);
    }
    buffer.put_u16(value.len() as u16);
    buffer.put_slice(value);
    Ok(())
}

pub(crate) fn finish_frame(mut buffer: BytesMut) -> Result<Bytes> {
    let body_length = buffer.len() - 4;
    if body_length > MAX_FRAME_LENGTH {
        return Err(Error::FrameTooLarge);
    }
    buffer[..4].copy_from_slice(&(body_length as u32).to_be_bytes());
    Ok(buffer.freeze())
}

fn encode_string_frame(kind: u8, value: &str) -> Result<Bytes> {
    let mut buffer = BytesMut::with_capacity(7 + value.len());
    buffer.put_u32(0);
    buffer.put_u8(kind);
    put_string_field(&mut buffer, value.as_bytes())?;
    finish_frame(buffer)
}

/// Encodes the greeting every peer must send first, announcing its identity.
pub fn encode_hello(id: &str) -> Result<Bytes> {
    encode_string_frame(KIND_HELLO, id)
}

/// Encodes a subscription request for one topic.
pub fn encode_subscribe(topic: &str) -> Result<Bytes> {
    encode_string_frame(KIND_SUBSCRIBE, topic)
}

/// Encodes an unsubscription request for one topic.
pub fn encode_unsubscribe(topic: &str) -> Result<Bytes> {
    encode_string_frame(KIND_UNSUBSCRIBE, topic)
}

pub(crate) fn begin_publish_frame(topic: &str, flags: u8) -> Result<BytesMut> {
    let mut buffer = BytesMut::with_capacity(HEADER_LENGTH + topic.len() + 256);
    buffer.put_u32(0);
    buffer.put_u8(KIND_PUBLISH);
    buffer.put_u8(flags);
    put_string_field(&mut buffer, topic.as_bytes())?;
    Ok(buffer)
}

pub(crate) fn route_flags(local_only: bool) -> u8 {
    if local_only { FLAG_LOCAL_ONLY } else { 0 }
}

/// Encodes a text publish frame.
pub fn encode_publish_text(topic: &str, payload: &str, local_only: bool) -> Result<Bytes> {
    let mut buffer = begin_publish_frame(topic, route_flags(local_only))?;
    buffer.put_slice(payload.as_bytes());
    finish_frame(buffer)
}

/// Encodes a binary publish frame.
pub fn encode_publish_binary(topic: &str, payload: &[u8], local_only: bool) -> Result<Bytes> {
    let mut buffer = begin_publish_frame(topic, FLAG_BINARY | route_flags(local_only))?;
    buffer.put_slice(payload);
    finish_frame(buffer)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use transport::*;

#[cfg(not(target_arch = "wasm32"))]
mod transport {
    use super::*;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    const KIND_FORWARD: u8 = 6;
    const KIND_OPEN_BRIDGE: u8 = 7;
    const KIND_CLOSE_BRIDGE: u8 = 8;

    const READ_CHUNK: usize = 64 * 1024;
    const MINIMUM_READ_SPARE: usize = 4 * 1024;
    pub(crate) const MAX_WRITE_BATCH: usize = 64;
    pub(crate) const DELIVER_PAYLOAD_OFFSET: usize = HEADER_LENGTH;

    /// An outbound frame: a header buffer plus an optional shared payload
    /// slice, written with a vectored write so the payload is never copied.
    #[derive(Clone)]
    pub(crate) struct Frame {
        pub(crate) header: Bytes,
        pub(crate) payload: Bytes,
    }

    impl Frame {
        pub(crate) fn single(bytes: Bytes) -> Self {
            Self {
                header: bytes,
                payload: Bytes::new(),
            }
        }

        #[cfg(any(feature = "websockets", feature = "udp"))]
        pub(crate) fn to_contiguous(&self) -> Bytes {
            if self.payload.is_empty() {
                return self.header.clone();
            }
            let mut combined = BytesMut::with_capacity(self.header.len() + self.payload.len());
            combined.put_slice(&self.header);
            combined.put_slice(&self.payload);
            combined.freeze()
        }
    }

    pub(crate) enum FrameView {
        Hello {
            id: Range<usize>,
        },
        Subscribe {
            topic: Range<usize>,
        },
        Unsubscribe {
            topic: Range<usize>,
        },
        Publish {
            flags: u8,
            topic: Range<usize>,
            payload: Range<usize>,
        },
        Forward {
            flags: u8,
            sequence: u64,
            visited: Range<usize>,
            topic: Range<usize>,
            payload: Range<usize>,
        },
        OpenBridge {
            ack: bool,
            source_address: Range<usize>,
            target_address: Range<usize>,
        },
        CloseBridge {
            ack: bool,
            target_address: Range<usize>,
        },
    }

    struct FrameCursor<'frame> {
        frame: &'frame [u8],
        position: usize,
    }

    impl FrameCursor<'_> {
        fn read_u8(&mut self) -> Option<u8> {
            let value = *self.frame.get(self.position)?;
            self.position += 1;
            Some(value)
        }

        fn read_u16(&mut self) -> Option<u16> {
            let bytes = self.frame.get(self.position..self.position + 2)?;
            self.position += 2;
            Some(u16::from_be_bytes([bytes[0], bytes[1]]))
        }

        fn read_u64(&mut self) -> Option<u64> {
            let bytes = self.frame.get(self.position..self.position + 8)?;
            let mut buffer = [0_u8; 8];
            buffer.copy_from_slice(bytes);
            self.position += 8;
            Some(u64::from_be_bytes(buffer))
        }

        fn read_range(&mut self, length: usize) -> Option<Range<usize>> {
            let end = self.position.checked_add(length)?;
            if end > self.frame.len() {
                return None;
            }
            let range = self.position..end;
            self.position = end;
            Some(range)
        }

        fn read_string_range(&mut self) -> Option<Range<usize>> {
            let length = self.read_u16()? as usize;
            self.read_range(length)
        }

        fn rest(&self) -> Range<usize> {
            self.position..self.frame.len()
        }

        fn finished(&self) -> Option<()> {
            (self.position == self.frame.len()).then_some(())
        }
    }

    pub(crate) fn parse_frame(frame: &[u8]) -> Option<FrameView> {
        if frame.len() < 5 {
            return None;
        }
        let declared = read_u32(&frame[..4])? as usize;
        if declared + 4 != frame.len() {
            return None;
        }
        let kind = frame[KIND_OFFSET];
        let mut cursor = FrameCursor { frame, position: 5 };
        match kind {
            KIND_HELLO => {
                let id = cursor.read_string_range()?;
                cursor.finished()?;
                Some(FrameView::Hello { id })
            }
            KIND_SUBSCRIBE => {
                let topic = cursor.read_string_range()?;
                cursor.finished()?;
                Some(FrameView::Subscribe { topic })
            }
            KIND_UNSUBSCRIBE => {
                let topic = cursor.read_string_range()?;
                cursor.finished()?;
                Some(FrameView::Unsubscribe { topic })
            }
            KIND_PUBLISH => {
                let flags = cursor.read_u8()?;
                let topic = cursor.read_string_range()?;
                Some(FrameView::Publish {
                    flags,
                    topic,
                    payload: cursor.rest(),
                })
            }
            KIND_FORWARD => {
                let flags = cursor.read_u8()?;
                let sequence = cursor.read_u64()?;
                let visited_count = cursor.read_u8()? as usize;
                let visited = cursor.read_range(visited_count * 16)?;
                let topic = cursor.read_string_range()?;
                Some(FrameView::Forward {
                    flags,
                    sequence,
                    visited,
                    topic,
                    payload: cursor.rest(),
                })
            }
            KIND_OPEN_BRIDGE => {
                let ack = cursor.read_u8()? != 0;
                let source_address = cursor.read_string_range()?;
                let target_address = cursor.read_string_range()?;
                cursor.finished()?;
                Some(FrameView::OpenBridge {
                    ack,
                    source_address,
                    target_address,
                })
            }
            KIND_CLOSE_BRIDGE => {
                let ack = cursor.read_u8()? != 0;
                let target_address = cursor.read_string_range()?;
                cursor.finished()?;
                Some(FrameView::CloseBridge {
                    ack,
                    target_address,
                })
            }
            _ => None,
        }
    }

    pub(crate) fn patch_publish_to_deliver(frame: &mut BytesMut) {
        frame[KIND_OFFSET] = KIND_DELIVER;
    }

    pub(crate) fn encode_deliver_text(topic: &str, payload: &str) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(HEADER_LENGTH + topic.len() + payload.len());
        buffer.put_u32(0);
        buffer.put_u8(KIND_DELIVER);
        buffer.put_u8(0);
        put_string_field(&mut buffer, topic.as_bytes())?;
        buffer.put_slice(payload.as_bytes());
        finish_frame(buffer)
    }

    pub(crate) fn encode_deliver_binary(topic: &str, payload: &[u8]) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(HEADER_LENGTH + topic.len() + payload.len());
        buffer.put_u32(0);
        buffer.put_u8(KIND_DELIVER);
        buffer.put_u8(FLAG_BINARY);
        put_string_field(&mut buffer, topic.as_bytes())?;
        buffer.put_slice(payload);
        finish_frame(buffer)
    }

    pub(crate) fn encode_deliver_header(
        topic: &[u8],
        flags: u8,
        payload_length: usize,
    ) -> Result<Bytes> {
        let body_length = 4 + topic.len() + payload_length;
        if body_length > MAX_FRAME_LENGTH {
            return Err(Error::FrameTooLarge);
        }
        let mut buffer = BytesMut::with_capacity(HEADER_LENGTH + topic.len());
        buffer.put_u32(body_length as u32);
        buffer.put_u8(KIND_DELIVER);
        buffer.put_u8(flags);
        put_string_field(&mut buffer, topic)?;
        Ok(buffer.freeze())
    }

    pub(crate) fn encode_forward_header(
        topic: &[u8],
        flags: u8,
        sequence: u64,
        visited: &[u128],
        payload_length: usize,
    ) -> Result<Bytes> {
        if visited.len() > u8::MAX as usize {
            return Err(Error::Other("too many bridge hops".to_string()));
        }
        let header_length = 4 + 1 + 1 + 8 + 1 + visited.len() * 16 + 2 + topic.len();
        let body_length = header_length - 4 + payload_length;
        if body_length > MAX_FRAME_LENGTH {
            return Err(Error::FrameTooLarge);
        }
        let mut buffer = BytesMut::with_capacity(header_length);
        buffer.put_u32(body_length as u32);
        buffer.put_u8(KIND_FORWARD);
        buffer.put_u8(flags);
        buffer.put_u64(sequence);
        buffer.put_u8(visited.len() as u8);
        for entry in visited {
            buffer.put_u128(*entry);
        }
        put_string_field(&mut buffer, topic)?;
        Ok(buffer.freeze())
    }

    pub(crate) fn encode_open_bridge(
        ack: bool,
        source_address: &str,
        target_address: &str,
    ) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(10 + source_address.len() + target_address.len());
        buffer.put_u32(0);
        buffer.put_u8(KIND_OPEN_BRIDGE);
        buffer.put_u8(u8::from(ack));
        put_string_field(&mut buffer, source_address.as_bytes())?;
        put_string_field(&mut buffer, target_address.as_bytes())?;
        finish_frame(buffer)
    }

    pub(crate) fn encode_close_bridge(ack: bool, target_address: &str) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(8 + target_address.len());
        buffer.put_u32(0);
        buffer.put_u8(KIND_CLOSE_BRIDGE);
        buffer.put_u8(u8::from(ack));
        put_string_field(&mut buffer, target_address.as_bytes())?;
        finish_frame(buffer)
    }

    /// Reads length-prefixed frames through one reusable buffer, so allocation
    /// is amortized across frames instead of paid per frame.
    pub(crate) struct FrameReader<Reader> {
        reader: Reader,
        buffer: BytesMut,
    }

    impl<Reader: AsyncRead + Unpin> FrameReader<Reader> {
        pub(crate) fn new(reader: Reader) -> Self {
            Self {
                reader,
                buffer: BytesMut::with_capacity(READ_CHUNK),
            }
        }

        pub(crate) async fn next_frame(&mut self) -> Result<BytesMut> {
            loop {
                if self.buffer.len() >= 4 {
                    let declared =
                        read_u32(&self.buffer[..4]).ok_or(Error::FrameTooLarge)? as usize;
                    if declared > MAX_FRAME_LENGTH {
                        return Err(Error::FrameTooLarge);
                    }
                    let total = declared + 4;
                    if self.buffer.len() >= total {
                        return Ok(self.buffer.split_to(total));
                    }
                    self.buffer.reserve(total - self.buffer.len());
                } else if self.buffer.capacity() - self.buffer.len() < MINIMUM_READ_SPARE {
                    self.buffer.reserve(READ_CHUNK);
                }
                if self.reader.read_buf(&mut self.buffer).await? == 0 {
                    return Err(Error::Transport("connection closed".to_string()));
                }
            }
        }
    }

    pub(crate) async fn write_frames(
        writer: &mut (impl AsyncWrite + Unpin),
        frames: &[Frame],
    ) -> Result<()> {
        let mut slices: smallvec::SmallVec<[std::io::IoSlice<'_>; 16]> = smallvec::SmallVec::new();
        for frame in frames {
            slices.push(std::io::IoSlice::new(&frame.header));
            if !frame.payload.is_empty() {
                slices.push(std::io::IoSlice::new(&frame.payload));
            }
        }
        let mut remaining: &mut [std::io::IoSlice<'_>] = &mut slices;
        while !remaining.is_empty() {
            let written = writer.write_vectored(remaining).await?;
            if written == 0 {
                return Err(Error::Transport("connection closed".to_string()));
            }
            std::io::IoSlice::advance_slices(&mut remaining, written);
        }
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use bytes::Bytes;
    use proptest::prelude::*;

    const TOPIC_PATTERN: &str = "[a-zA-Z0-9/_+#. -]{0,64}";
    const TEXT_PATTERN: &str = "[a-zA-Z0-9/_+#. -]{0,256}";

    proptest! {
        #[test]
        fn decode_message_never_panics_on_arbitrary_bytes(bytes: Vec<u8>) {
            let _ = decode_message(Bytes::from(bytes));
        }

        #[test]
        fn parse_frame_never_panics_on_arbitrary_bytes(bytes: Vec<u8>) {
            let _ = parse_frame(&bytes);
        }

        #[test]
        fn publish_text_roundtrips(
            topic in TOPIC_PATTERN,
            payload in TEXT_PATTERN,
            local in any::<bool>(),
        ) {
            let frame = encode_publish_text(&topic, &payload, local).unwrap();
            let view = parse_frame(&frame).expect("a freshly built frame parses");
            let FrameView::Publish { flags, topic: topic_range, payload: payload_range } = view else {
                return Err(TestCaseError::fail("expected a Publish frame"));
            };
            prop_assert_eq!(&frame[topic_range], topic.as_bytes());
            prop_assert_eq!(&frame[payload_range], payload.as_bytes());
            prop_assert_eq!(flags & FLAG_LOCAL_ONLY != 0, local);
            prop_assert_eq!(flags & FLAG_BINARY, 0);
        }

        #[test]
        fn publish_binary_roundtrips(
            topic in TOPIC_PATTERN,
            payload: Vec<u8>,
            local in any::<bool>(),
        ) {
            let frame = encode_publish_binary(&topic, &payload, local).unwrap();
            let view = parse_frame(&frame).expect("a freshly built frame parses");
            let FrameView::Publish { flags, topic: topic_range, payload: payload_range } = view else {
                return Err(TestCaseError::fail("expected a Publish frame"));
            };
            prop_assert_eq!(&frame[topic_range], topic.as_bytes());
            prop_assert_eq!(&frame[payload_range], payload.as_slice());
            prop_assert!(flags & FLAG_BINARY != 0);
        }

        #[test]
        fn hello_subscribe_unsubscribe_roundtrip(value in TOPIC_PATTERN) {
            let hello = encode_hello(&value).unwrap();
            let FrameView::Hello { id } = parse_frame(&hello).unwrap() else {
                return Err(TestCaseError::fail("expected a Hello frame"));
            };
            prop_assert_eq!(&hello[id], value.as_bytes());

            let subscribe = encode_subscribe(&value).unwrap();
            let FrameView::Subscribe { topic } = parse_frame(&subscribe).unwrap() else {
                return Err(TestCaseError::fail("expected a Subscribe frame"));
            };
            prop_assert_eq!(&subscribe[topic], value.as_bytes());

            let unsubscribe = encode_unsubscribe(&value).unwrap();
            let FrameView::Unsubscribe { topic } = parse_frame(&unsubscribe).unwrap() else {
                return Err(TestCaseError::fail("expected an Unsubscribe frame"));
            };
            prop_assert_eq!(&unsubscribe[topic], value.as_bytes());
        }

        #[test]
        fn deliver_text_decodes_back(topic in TOPIC_PATTERN, payload in TEXT_PATTERN) {
            let frame = encode_deliver_text(&topic, &payload).unwrap();
            let message = decode_message(frame).expect("a deliver frame decodes");
            prop_assert_eq!(message.topic(), topic.as_str());
            prop_assert_eq!(message.text(), Some(payload.as_str()));
        }

        #[test]
        fn deliver_binary_decodes_back(topic in TOPIC_PATTERN, payload: Vec<u8>) {
            let frame = encode_deliver_binary(&topic, &payload).unwrap();
            let message = decode_message(frame).expect("a deliver frame decodes");
            prop_assert_eq!(message.topic(), topic.as_str());
            prop_assert_eq!(message.bytes(), Some(payload.as_slice()));
        }
    }
}
