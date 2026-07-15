//! Datagram encryption keyed by a shared network passphrase.
//!
//! With the `secure` feature, every UDP datagram is sealed with
//! ChaCha20-Poly1305 under a key derived from a passphrase all legitimate
//! peers share. A peer without the passphrase cannot decrypt the bus or forge
//! a datagram the broker will accept, which closes the "anyone who reaches the
//! port can subscribe and publish" hole for a group that shares a secret. It
//! does not distinguish one key holder from another; per-identity authorization
//! needs server-signed tokens, a further layer this does not provide.

#[cfg(feature = "secure")]
mod implementation {
    use chacha20poly1305::aead::{Aead, KeyInit};
    use chacha20poly1305::{ChaCha20Poly1305, Nonce};
    use sha2::{Digest, Sha256};

    const NONCE_LENGTH: usize = 12;

    #[derive(Clone)]
    pub(crate) struct Cipher {
        cipher: ChaCha20Poly1305,
    }

    impl Cipher {
        pub(crate) fn from_passphrase(passphrase: &str) -> Self {
            let key = Sha256::digest(passphrase.as_bytes());
            let cipher = ChaCha20Poly1305::new_from_slice(key.as_slice())
                .expect("sha-256 digest is a valid 32-byte key");
            Self { cipher }
        }

        /// Encrypts a datagram, returning `nonce || ciphertext`.
        pub(crate) fn seal(&self, plaintext: &[u8]) -> Vec<u8> {
            let mut nonce = [0_u8; NONCE_LENGTH];
            getrandom::getrandom(&mut nonce).expect("system randomness");
            let ciphertext = self
                .cipher
                .encrypt(Nonce::from_slice(&nonce), plaintext)
                .expect("aead encryption does not fail");
            let mut sealed = Vec::with_capacity(NONCE_LENGTH + ciphertext.len());
            sealed.extend_from_slice(&nonce);
            sealed.extend_from_slice(&ciphertext);
            sealed
        }

        /// Decrypts a `nonce || ciphertext` datagram, or returns `None` if it
        /// was not sealed with this key (a stranger, or a tampered datagram).
        pub(crate) fn open(&self, sealed: &[u8]) -> Option<Vec<u8>> {
            if sealed.len() < NONCE_LENGTH {
                return None;
            }
            let (nonce, ciphertext) = sealed.split_at(NONCE_LENGTH);
            self.cipher
                .decrypt(Nonce::from_slice(nonce), ciphertext)
                .ok()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::Cipher;

        #[test]
        fn round_trip_recovers_the_plaintext() {
            let cipher = Cipher::from_passphrase("swordfish");
            let sealed = cipher.seal(b"hello bus");
            assert_eq!(cipher.open(&sealed).as_deref(), Some(&b"hello bus"[..]));
        }

        #[test]
        fn a_tampered_datagram_is_rejected() {
            let cipher = Cipher::from_passphrase("swordfish");
            let mut sealed = cipher.seal(b"hello bus");
            let last = sealed.len() - 1;
            sealed[last] ^= 0xff;
            assert!(cipher.open(&sealed).is_none());
        }

        #[test]
        fn the_wrong_key_cannot_decrypt() {
            let sealed = Cipher::from_passphrase("swordfish").seal(b"hello bus");
            assert!(Cipher::from_passphrase("hunter2").open(&sealed).is_none());
        }

        #[test]
        fn each_datagram_uses_a_fresh_nonce() {
            let cipher = Cipher::from_passphrase("swordfish");
            assert_ne!(cipher.seal(b"same"), cipher.seal(b"same"));
        }
    }
}

#[cfg(feature = "secure")]
pub(crate) use implementation::Cipher;

// Without the `secure` feature the type still exists so the UDP paths can hold
// an `Option<Cipher>` uniformly, but it is uninhabited: only `None` is ever
// constructible, so the sealing code is never reached and no crypto dependency
// is pulled in.
#[cfg(not(feature = "secure"))]
#[derive(Clone)]
pub(crate) enum Cipher {}

#[cfg(not(feature = "secure"))]
impl Cipher {
    pub(crate) fn seal(&self, _plaintext: &[u8]) -> Vec<u8> {
        match *self {}
    }

    pub(crate) fn open(&self, _sealed: &[u8]) -> Option<Vec<u8>> {
        match *self {}
    }
}
