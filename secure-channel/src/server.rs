use crate::encryption::{Aes, BlockModeError, BLOCK_SIZE, HEADER_SIZE};

use std::io::{Error, Write};
use std::result::Result;

use bytes::{Buf, Bytes, BytesMut};

pub struct ServerInput {
    receive_buffer: BytesMut,
    receive_len: usize,
}

pub struct ServerOutput {
    output: Box<dyn Send + Write>,
}

impl Default for ServerInput {
    fn default() -> Self {
        Self {
            receive_buffer: BytesMut::new(),
            receive_len: 0,
        }
    }
}

impl ServerInput {
    pub fn push_data(&mut self, buf: &[u8]) {
        self.receive_buffer.extend_from_slice(buf);
    }

    pub fn get_message(&mut self) -> Option<Result<Bytes, BlockModeError>> {
        let receive_buffer = &mut self.receive_buffer;

        if self.receive_len == 0 {
            if receive_buffer.len() >= HEADER_SIZE {
                // Parse header
                self.receive_len = bincode::deserialize(&receive_buffer[..HEADER_SIZE]).unwrap();
                receive_buffer.advance(HEADER_SIZE);

                if self.receive_len == 0 {
                    panic!("Got invalid receive_len");
                }
            } else {
                return None;
            }
        }

        let offset = self.receive_len % BLOCK_SIZE;
        let padding = BLOCK_SIZE - offset;
        let total_len = self.receive_len + padding;

        if receive_buffer.len() >= total_len {
            // Decrypt data
            let cipher = Aes::new();
            match cipher.decrypt(&mut receive_buffer[..total_len]) {
                Ok(_) => {}
                Err(e) => {
                    return Some(Err(e));
                }
            }
        } else {
            return None;
        }

        let msg = receive_buffer.split_to(self.receive_len);
        receive_buffer.advance(padding);
        self.receive_len = 0;

        Some(Ok(msg.freeze()))
    }
}

impl ServerOutput {
    pub fn new(output: Box<dyn Send + Write>) -> Self {
        Self { output }
    }
}

impl Write for ServerOutput {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let mut header = BytesMut::new();
        let mut payload = BytesMut::new();

        let len: usize = buf.len();
        if len == 0 {
            panic!("Length cannot be zero");
        }

        header.extend_from_slice(&bincode::serialize(&len).unwrap());
        payload.extend_from_slice(buf);

        // Make sure this aligns with 16 byte blocks
        let offset = len % BLOCK_SIZE;
        let padding = BLOCK_SIZE - offset; // even if there is no offset we still need to pad a block

        payload.resize(len + padding, 0);

        let cipher = Aes::new();
        cipher
            .encrypt(&mut payload, len)
            .expect("Failed to encrypt data");

        header.unsplit(payload);
        let send_buffer = header;

        match self.output.write_all(&send_buffer[..]) {
            Ok(()) => Ok(buf.len()),
            Err(e) => Err(e),
        }
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.output.flush()
    }
}
