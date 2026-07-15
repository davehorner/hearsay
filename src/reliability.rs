use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;

/// The delivery guarantee for a message sent over a datagram transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Fire and forget. Lost datagrams are gone; reordered ones arrive out of
    /// order. Cheapest, for high-frequency state where the next message
    /// supersedes this one.
    Unreliable,
    /// Like [`Delivery::Unreliable`], but the receiver drops anything older
    /// than the newest it has seen on this stream, so stale state is never
    /// delivered after fresh state.
    UnreliableSequenced,
    /// Retransmitted until acknowledged and delivered to the application in
    /// send order. For events that must arrive exactly once, in order.
    ReliableOrdered,
}

const CHANNEL_UNRELIABLE: u8 = 0;
const CHANNEL_SEQUENCED: u8 = 1;
const CHANNEL_RELIABLE: u8 = 2;
const KIND_ACK_ONLY: u8 = 3;

const PACKET_HEADER: usize = 8;
/// Largest message-fragment payload carried in one packet. Kept well under a
/// typical path MTU so a packet does not rely on IP fragmentation.
pub(crate) const MAX_FRAGMENT_PAYLOAD: usize = 1024;

const RESEND_TIMEOUT_MILLIS: u64 = 100;
const ACK_HISTORY: u16 = 32;

fn sequence_greater(lhs: u16, rhs: u16) -> bool {
    ((lhs > rhs) && (lhs - rhs <= u16::MAX / 2)) || ((lhs < rhs) && (rhs - lhs > u16::MAX / 2))
}

struct OutgoingFragment {
    message_id: u32,
    fragment_index: u16,
    fragment_count: u16,
    payload: Bytes,
    last_sent_millis: Option<u64>,
}

struct Reassembly {
    fragment_count: u16,
    fragments: Vec<Option<Bytes>>,
    received: u16,
}

/// A transport-agnostic reliability layer over an unordered, lossy datagram
/// channel. It knows nothing about sockets: it is fed inbound datagrams and a
/// clock, and it yields delivered messages and the datagrams to send. This is
/// what makes it testable against simulated loss and reordering.
pub(crate) struct Endpoint {
    next_packet_sequence: u16,
    next_reliable_id: u32,
    outgoing_unreliable: Vec<(u8, Option<u16>, Bytes)>,
    next_sequenced: u16,
    outgoing_reliable: Vec<OutgoingFragment>,
    sent_reliable_packets: HashMap<u16, (u32, u16)>,
    remote_latest: Option<u16>,
    remote_ack_bits: u32,
    received_sequenced: HashMap<u8, u16>,
    reassembly: HashMap<u32, Reassembly>,
    delivered_reliable_through: Option<u32>,
    completed_reliable: HashMap<u32, Bytes>,
    dirty: bool,
}

impl Endpoint {
    pub(crate) fn new() -> Self {
        Self {
            next_packet_sequence: 0,
            next_reliable_id: 0,
            outgoing_unreliable: Vec::new(),
            next_sequenced: 0,
            outgoing_reliable: Vec::new(),
            sent_reliable_packets: HashMap::new(),
            remote_latest: None,
            remote_ack_bits: 0,
            received_sequenced: HashMap::new(),
            reassembly: HashMap::new(),
            delivered_reliable_through: None,
            completed_reliable: HashMap::new(),
            dirty: false,
        }
    }

    /// Queues a message for delivery under the given guarantee.
    pub(crate) fn send(&mut self, delivery: Delivery, payload: &[u8]) {
        self.dirty = true;
        match delivery {
            Delivery::Unreliable => {
                self.outgoing_unreliable.push((
                    CHANNEL_UNRELIABLE,
                    None,
                    Bytes::copy_from_slice(payload),
                ));
            }
            Delivery::UnreliableSequenced => {
                let sequence = self.next_sequenced;
                self.next_sequenced = self.next_sequenced.wrapping_add(1);
                self.outgoing_unreliable.push((
                    CHANNEL_SEQUENCED,
                    Some(sequence),
                    Bytes::copy_from_slice(payload),
                ));
            }
            Delivery::ReliableOrdered => {
                let message_id = self.next_reliable_id;
                self.next_reliable_id += 1;
                let chunks = payload.chunks(MAX_FRAGMENT_PAYLOAD);
                let fragment_count = chunks.len().max(1) as u16;
                if payload.is_empty() {
                    self.outgoing_reliable.push(OutgoingFragment {
                        message_id,
                        fragment_index: 0,
                        fragment_count: 1,
                        payload: Bytes::new(),
                        last_sent_millis: None,
                    });
                } else {
                    for (index, chunk) in payload.chunks(MAX_FRAGMENT_PAYLOAD).enumerate() {
                        self.outgoing_reliable.push(OutgoingFragment {
                            message_id,
                            fragment_index: index as u16,
                            fragment_count,
                            payload: Bytes::copy_from_slice(chunk),
                            last_sent_millis: None,
                        });
                    }
                }
            }
        }
    }

    /// Whether there is anything to send: queued messages, unacked reliable
    /// fragments, or a pending acknowledgement.
    pub(crate) fn has_outgoing(&self) -> bool {
        self.dirty || !self.outgoing_unreliable.is_empty() || !self.outgoing_reliable.is_empty()
    }

    /// Produces the datagrams to transmit now: fresh messages, retransmissions
    /// of unacked reliable fragments, and — if a peer is waiting on an ack with
    /// nothing else outbound — a standalone ack packet.
    pub(crate) fn drain_outgoing(&mut self, now_millis: u64, out: &mut Vec<Bytes>) {
        for (channel, sequence, payload) in std::mem::take(&mut self.outgoing_unreliable) {
            let mut packet = self.begin_packet();
            packet.put_u8(channel);
            if let Some(sequence) = sequence {
                packet.put_u16(sequence);
            }
            packet.put_u16(payload.len() as u16);
            packet.put_slice(&payload);
            out.push(packet.freeze());
        }

        let mut resent: Vec<(u16, u32, u16)> = Vec::new();
        for fragment in &mut self.outgoing_reliable {
            let due = match fragment.last_sent_millis {
                None => true,
                Some(sent) => now_millis.saturating_sub(sent) >= RESEND_TIMEOUT_MILLIS,
            };
            if !due {
                continue;
            }
            let sequence = self.next_packet_sequence;
            self.next_packet_sequence = self.next_packet_sequence.wrapping_add(1);
            let mut packet = BytesMut::with_capacity(PACKET_HEADER + 16 + fragment.payload.len());
            write_header(
                &mut packet,
                sequence,
                self.remote_latest,
                self.remote_ack_bits,
            );
            packet.put_u8(CHANNEL_RELIABLE);
            packet.put_u32(fragment.message_id);
            packet.put_u16(fragment.fragment_index);
            packet.put_u16(fragment.fragment_count);
            packet.put_u16(fragment.payload.len() as u16);
            packet.put_slice(&fragment.payload);
            fragment.last_sent_millis = Some(now_millis);
            resent.push((sequence, fragment.message_id, fragment.fragment_index));
            out.push(packet.freeze());
        }
        for (sequence, message_id, fragment_index) in resent {
            self.sent_reliable_packets
                .insert(sequence, (message_id, fragment_index));
        }

        if out.is_empty() && self.dirty {
            let mut packet = self.begin_packet();
            packet.put_u8(KIND_ACK_ONLY);
            out.push(packet.freeze());
        }
        self.dirty = false;
    }

    fn begin_packet(&mut self) -> BytesMut {
        let sequence = self.next_packet_sequence;
        self.next_packet_sequence = self.next_packet_sequence.wrapping_add(1);
        let mut packet = BytesMut::with_capacity(PACKET_HEADER + MAX_FRAGMENT_PAYLOAD);
        write_header(
            &mut packet,
            sequence,
            self.remote_latest,
            self.remote_ack_bits,
        );
        packet
    }

    /// Consumes an inbound datagram, appending any messages it completes to
    /// `delivered` (reliable messages in send order). Malformed datagrams are
    /// ignored.
    pub(crate) fn receive(&mut self, datagram: &[u8], delivered: &mut Vec<Bytes>) {
        if datagram.len() < PACKET_HEADER {
            return;
        }
        let mut cursor = datagram;
        let sequence = cursor.get_u16();
        let ack = cursor.get_u16();
        let ack_bits = cursor.get_u32();
        self.process_acks(ack, ack_bits);
        self.record_received(sequence);

        let Some(kind) = read_u8(&mut cursor) else {
            return;
        };
        match kind {
            KIND_ACK_ONLY => {}
            CHANNEL_UNRELIABLE => {
                if let Some(payload) = read_block(&mut cursor) {
                    delivered.push(payload);
                }
            }
            CHANNEL_SEQUENCED => {
                let Some(message_sequence) = read_u16(&mut cursor) else {
                    return;
                };
                let Some(payload) = read_block(&mut cursor) else {
                    return;
                };
                let latest = self.received_sequenced.get(&CHANNEL_SEQUENCED).copied();
                if latest.is_none_or(|latest| sequence_greater(message_sequence, latest)) {
                    self.received_sequenced
                        .insert(CHANNEL_SEQUENCED, message_sequence);
                    delivered.push(payload);
                }
            }
            CHANNEL_RELIABLE => {
                self.receive_reliable_fragment(&mut cursor, delivered);
            }
            _ => {}
        }
    }

    fn receive_reliable_fragment(&mut self, cursor: &mut &[u8], delivered: &mut Vec<Bytes>) {
        let (Some(message_id), Some(fragment_index), Some(fragment_count)) =
            (read_u32(cursor), read_u16(cursor), read_u16(cursor))
        else {
            return;
        };
        let Some(payload) = read_block(cursor) else {
            return;
        };
        if fragment_count == 0 || fragment_index >= fragment_count {
            return;
        }
        // Already delivered (a retransmit of an acked message): ignore.
        if self
            .delivered_reliable_through
            .is_some_and(|through| message_id <= through)
        {
            return;
        }
        if self.completed_reliable.contains_key(&message_id) {
            return;
        }
        let entry = self
            .reassembly
            .entry(message_id)
            .or_insert_with(|| Reassembly {
                fragment_count,
                fragments: vec![None; fragment_count as usize],
                received: 0,
            });
        if entry.fragment_count != fragment_count {
            return;
        }
        if entry.fragments[fragment_index as usize].is_none() {
            entry.fragments[fragment_index as usize] = Some(payload);
            entry.received += 1;
        }
        if entry.received == entry.fragment_count {
            let entry = self.reassembly.remove(&message_id).expect("just present");
            let mut message = BytesMut::new();
            for fragment in entry.fragments {
                message.extend_from_slice(&fragment.expect("all fragments present"));
            }
            self.completed_reliable.insert(message_id, message.freeze());
            self.flush_ordered(delivered);
        }
    }

    fn flush_ordered(&mut self, delivered: &mut Vec<Bytes>) {
        loop {
            let next = match self.delivered_reliable_through {
                None => 0,
                Some(through) => through + 1,
            };
            let Some(message) = self.completed_reliable.remove(&next) else {
                break;
            };
            delivered.push(message);
            self.delivered_reliable_through = Some(next);
        }
    }

    fn process_acks(&mut self, ack: u16, ack_bits: u32) {
        self.acknowledge(ack);
        for offset in 0..ACK_HISTORY {
            if ack_bits & (1 << offset) != 0 {
                self.acknowledge(ack.wrapping_sub(offset + 1));
            }
        }
    }

    fn acknowledge(&mut self, packet_sequence: u16) {
        let Some((message_id, fragment_index)) =
            self.sent_reliable_packets.remove(&packet_sequence)
        else {
            return;
        };
        self.outgoing_reliable.retain(|fragment| {
            !(fragment.message_id == message_id && fragment.fragment_index == fragment_index)
        });
    }

    fn record_received(&mut self, sequence: u16) {
        self.dirty = true;
        match self.remote_latest {
            None => {
                self.remote_latest = Some(sequence);
                self.remote_ack_bits = 0;
            }
            Some(latest) if sequence_greater(sequence, latest) => {
                let shift = sequence.wrapping_sub(latest);
                self.remote_ack_bits = if shift >= ACK_HISTORY {
                    0
                } else {
                    (self.remote_ack_bits << shift) | (1 << (shift - 1))
                };
                self.remote_latest = Some(sequence);
            }
            Some(latest) => {
                let distance = latest.wrapping_sub(sequence);
                if (1..=ACK_HISTORY).contains(&distance) {
                    self.remote_ack_bits |= 1 << (distance - 1);
                }
            }
        }
    }
}

fn write_header(packet: &mut BytesMut, sequence: u16, ack: Option<u16>, ack_bits: u32) {
    packet.put_u16(sequence);
    packet.put_u16(ack.unwrap_or(0));
    packet.put_u32(ack_bits);
}

fn read_u8(cursor: &mut &[u8]) -> Option<u8> {
    (cursor.remaining() >= 1).then(|| cursor.get_u8())
}

fn read_u16(cursor: &mut &[u8]) -> Option<u16> {
    (cursor.remaining() >= 2).then(|| cursor.get_u16())
}

fn read_u32(cursor: &mut &[u8]) -> Option<u32> {
    (cursor.remaining() >= 4).then(|| cursor.get_u32())
}

fn read_block(cursor: &mut &[u8]) -> Option<Bytes> {
    let length = read_u16(cursor)? as usize;
    if cursor.remaining() < length {
        return None;
    }
    Some(Bytes::copy_from_slice(&cursor[..length]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic pseudo-random source, so loss and reordering are
    /// reproducible across runs without a dependency.
    struct Prng(u64);

    impl Prng {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }

        fn chance(&mut self, percent: u64) -> bool {
            self.next() % 100 < percent
        }
    }

    /// A lossy, reordering, duplicating link between two endpoints. Datagrams
    /// are dropped at `loss_percent`, held back to arrive out of order, and
    /// occasionally duplicated, then the endpoints are driven over logical
    /// time until reliable traffic settles.
    struct Link {
        prng: Prng,
        loss_percent: u64,
        in_flight: Vec<(u64, bool, Bytes)>,
    }

    impl Link {
        fn new(seed: u64, loss_percent: u64) -> Self {
            Self {
                prng: Prng(seed),
                loss_percent,
                in_flight: Vec::new(),
            }
        }

        fn enqueue(&mut self, now: u64, forward: bool, datagrams: Vec<Bytes>) {
            for datagram in datagrams {
                if self.prng.chance(self.loss_percent) {
                    continue;
                }
                let jitter = self.prng.next() % 40;
                self.in_flight
                    .push((now + jitter, forward, datagram.clone()));
                if self.prng.chance(5) {
                    self.in_flight.push((now + jitter, forward, datagram));
                }
            }
        }

        fn deliver_due(
            &mut self,
            now: u64,
            forward_endpoint: &mut Endpoint,
            backward_endpoint: &mut Endpoint,
            forward_delivered: &mut Vec<Bytes>,
            backward_delivered: &mut Vec<Bytes>,
        ) {
            let due: Vec<(bool, Bytes)> = self
                .in_flight
                .iter()
                .filter(|(at, _, _)| *at <= now)
                .map(|(_, forward, datagram)| (*forward, datagram.clone()))
                .collect();
            self.in_flight.retain(|(at, _, _)| *at > now);
            for (forward, datagram) in due {
                if forward {
                    forward_endpoint.receive(&datagram, forward_delivered);
                } else {
                    backward_endpoint.receive(&datagram, backward_delivered);
                }
            }
        }
    }

    #[test]
    fn reliable_ordered_survives_heavy_loss_and_reordering() {
        for seed in [0x1234_5678, 0xdead_beef, 0x0bad_c0de, 0xfeed_face] {
            for loss_percent in [20, 40, 60] {
                let mut sender = Endpoint::new();
                let mut receiver = Endpoint::new();
                let mut link = Link::new(seed, loss_percent);

                let total = 200;
                for index in 0..total {
                    sender.send(Delivery::ReliableOrdered, &index_payload(index));
                }

                let mut delivered: Vec<Bytes> = Vec::new();
                let mut discard: Vec<Bytes> = Vec::new();
                let mut now = 0;
                while delivered.len() < total as usize && now < 1_000_000 {
                    let mut from_sender = Vec::new();
                    sender.drain_outgoing(now, &mut from_sender);
                    link.enqueue(now, true, from_sender);

                    let mut from_receiver = Vec::new();
                    receiver.drain_outgoing(now, &mut from_receiver);
                    link.enqueue(now, false, from_receiver);

                    link.deliver_due(
                        now,
                        &mut receiver,
                        &mut sender,
                        &mut delivered,
                        &mut discard,
                    );
                    now += 15;
                }

                assert_eq!(
                    delivered.len(),
                    total as usize,
                    "every reliable message arrives (seed {seed:#x}, loss {loss_percent}%)"
                );
                for (index, message) in delivered.iter().enumerate() {
                    assert_eq!(
                        message.as_ref(),
                        index_payload(index as u32).as_slice(),
                        "reliable messages arrive in send order (seed {seed:#x}, loss {loss_percent}%)"
                    );
                }
            }
        }
    }

    #[test]
    fn reliable_fragmented_message_reassembles() {
        let mut sender = Endpoint::new();
        let mut receiver = Endpoint::new();
        let mut link = Link::new(0x9e37_79b9, 35);

        let large: Vec<u8> = (0..(MAX_FRAGMENT_PAYLOAD * 5 + 17))
            .map(|index| (index % 251) as u8)
            .collect();
        sender.send(Delivery::ReliableOrdered, &large);

        let mut delivered: Vec<Bytes> = Vec::new();
        let mut discard: Vec<Bytes> = Vec::new();
        let mut now = 0;
        while delivered.is_empty() && now < 100_000 {
            let mut from_sender = Vec::new();
            sender.drain_outgoing(now, &mut from_sender);
            link.enqueue(now, true, from_sender);
            let mut from_receiver = Vec::new();
            receiver.drain_outgoing(now, &mut from_receiver);
            link.enqueue(now, false, from_receiver);
            link.deliver_due(
                now,
                &mut receiver,
                &mut sender,
                &mut delivered,
                &mut discard,
            );
            now += 15;
        }

        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].as_ref(), large.as_slice());
    }

    #[test]
    fn unreliable_sequenced_drops_stale_and_never_delivers_older() {
        let mut sender = Endpoint::new();
        let mut receiver = Endpoint::new();

        // Three messages sent in order; deliver the newest first, then an older
        // one, which must be dropped as stale.
        sender.send(Delivery::UnreliableSequenced, b"first");
        sender.send(Delivery::UnreliableSequenced, b"second");
        sender.send(Delivery::UnreliableSequenced, b"third");
        let mut datagrams = Vec::new();
        sender.drain_outgoing(0, &mut datagrams);
        assert_eq!(datagrams.len(), 3);

        let mut delivered = Vec::new();
        receiver.receive(&datagrams[2], &mut delivered); // third
        receiver.receive(&datagrams[0], &mut delivered); // first, now stale
        receiver.receive(&datagrams[1], &mut delivered); // second, still stale

        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].as_ref(), b"third");
    }

    #[test]
    fn unreliable_delivers_what_arrives_and_only_that() {
        let mut sender = Endpoint::new();
        let mut receiver = Endpoint::new();
        sender.send(Delivery::Unreliable, b"a");
        sender.send(Delivery::Unreliable, b"b");
        let mut datagrams = Vec::new();
        sender.drain_outgoing(0, &mut datagrams);

        let mut delivered = Vec::new();
        // Drop the first datagram entirely; unreliable never recovers it.
        receiver.receive(&datagrams[1], &mut delivered);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].as_ref(), b"b");
    }

    #[test]
    fn a_duplicated_reliable_packet_is_delivered_once() {
        let mut sender = Endpoint::new();
        let mut receiver = Endpoint::new();
        sender.send(Delivery::ReliableOrdered, b"only-once");
        let mut datagrams = Vec::new();
        sender.drain_outgoing(0, &mut datagrams);

        let mut delivered = Vec::new();
        receiver.receive(&datagrams[0], &mut delivered);
        receiver.receive(&datagrams[0], &mut delivered);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].as_ref(), b"only-once");
    }

    fn index_payload(index: u32) -> Vec<u8> {
        format!("message-{index}").into_bytes()
    }
}
