use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};

use aes::Aes128;
use std::mem;

use hex_literal::hex;

pub use block_modes::BlockModeError;

pub const HEADER_SIZE: usize = mem::size_of::<usize>();
pub const BLOCK_SIZE: usize = 16;

// TODO implement key exchange
const AES_KEY: [u8; 16] = hex!("000102030405060708090a0b0c0d0e0f");
const AES_IV: [u8; 16] = hex!("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff");

pub struct Aes {
    inner: Cbc<Aes128, Pkcs7>,
}

impl Aes {
    pub fn new() -> Self {
        let inner = Cbc::new_from_slices(&AES_KEY, &AES_IV).expect("Failed to setup AES");
        Self { inner }
    }

    pub fn encrypt(self, buffer: &mut [u8], pos: usize) -> Result<&[u8], BlockModeError> {
        self.inner.encrypt(buffer, pos)
    }

    pub fn decrypt(self, buffer: &mut [u8]) -> Result<&[u8], BlockModeError> {
        self.inner.decrypt(buffer)
    }
}
