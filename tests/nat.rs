#![cfg(feature = "nat")]

use hearsay::{Rendezvous, punch};
use std::time::Duration;
use tokio::net::UdpSocket;

async fn recv_nonempty(socket: &UdpSocket) -> Vec<u8> {
    let mut buffer = vec![0_u8; 128];
    loop {
        let (length, _from) = socket.recv_from(&mut buffer).await.expect("recv");
        if length > 0 {
            return buffer[..length].to_vec();
        }
    }
}

// A single machine cannot reproduce two real NATs, so this validates the
// coordination path: both peers register with the rendezvous, each learns the
// other's observed address, and once they do they exchange datagrams directly.
#[tokio::test(flavor = "multi_thread")]
async fn peers_meet_through_the_rendezvous_and_exchange_directly() -> hearsay::Result<()> {
    let rendezvous = Rendezvous::start("127.0.0.1:0").await?;
    let rendezvous_address = rendezvous.address();

    let alice = UdpSocket::bind("127.0.0.1:0").await?;
    let bob = UdpSocket::bind("127.0.0.1:0").await?;
    let alice_local = alice.local_addr()?;
    let bob_local = bob.local_addr()?;

    let session = 0x1234;
    let (alice_learned, bob_learned) = tokio::join!(
        punch(&alice, rendezvous_address, session),
        punch(&bob, rendezvous_address, session)
    );
    let alice_learned = alice_learned?;
    let bob_learned = bob_learned?;

    // Each side learned the other's observed address.
    assert_eq!(alice_learned, bob_local);
    assert_eq!(bob_learned, alice_local);

    // And can now talk directly, skipping the empty hole-punch datagrams.
    alice.send_to(b"hello-bob", alice_learned).await?;
    bob.send_to(b"hello-alice", bob_learned).await?;

    let to_bob = tokio::time::timeout(Duration::from_secs(5), recv_nonempty(&bob)).await?;
    let to_alice = tokio::time::timeout(Duration::from_secs(5), recv_nonempty(&alice)).await?;
    assert_eq!(to_bob, b"hello-bob");
    assert_eq!(to_alice, b"hello-alice");
    Ok(())
}
