use crate::encryption::{Aes, BLOCK_SIZE, HEADER_SIZE};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::cmp::Ordering;
use std::io::Error;
use std::marker::Unpin;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::{Buf, BytesMut};

pub struct ClientInput {
    input: Box<dyn Send + AsyncRead + Unpin>,
    receive_buffer: Option<BytesMut>,
    receive_pos: usize,
    receive_len: usize,
}

pub struct ClientOutput {
    output: Box<dyn Send + AsyncWrite + Unpin>,
    send_buffer: Option<BytesMut>,
}

impl ClientInput {
    pub fn new(input: Box<dyn Send + AsyncRead + Unpin>) -> Self {
        Self {
            input,
            receive_buffer: None,
            receive_pos: 0,
            receive_len: 0,
        }
    }
}

impl ClientOutput {
    pub fn new(output: Box<dyn Send + AsyncWrite + Unpin>) -> Self {
        Self {
            output,
            send_buffer: None,
        }
    }
}

impl AsyncWrite for ClientOutput {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let mut send_buffer = if self.send_buffer.is_none() {
            let mut header = BytesMut::new();
            let mut payload = BytesMut::new();

            let len: usize = buf.len();
            header.extend_from_slice(&bincode::serialize(&len).unwrap());
            payload.extend_from_slice(buf);

            // Make sure this aligns with 128bit blocks
            let offset = len % BLOCK_SIZE;
            let padding = BLOCK_SIZE - offset;
            payload.resize(len + padding, 0);

            let cipher = Aes::new();
            cipher
                .encrypt(&mut payload, len)
                .expect("Failed to encrypt data");

            header.unsplit(payload);
            header
        } else {
            self.send_buffer.take().unwrap()
        };

        loop {
            match Pin::new(&mut self.output).poll_write(cx, &send_buffer[..]) {
                Poll::Ready(Ok(n)) => {
                    send_buffer.advance(n);

                    log::trace!(
                        "Written {n} bytes to network socket. {} remaining.",
                        send_buffer.len()
                    );

                    if send_buffer.is_empty() {
                        return Poll::Ready(Ok(buf.len()));
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    self.send_buffer = Some(send_buffer);
                    return Poll::Pending;
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.output).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.output).poll_shutdown(cx)
    }
}

impl AsyncRead for ClientInput {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out_buffer: &mut tokio::io::ReadBuf,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut receive_buffer;

        if self.receive_buffer.is_none() {
            self.receive_pos = 0;
            receive_buffer = BytesMut::new();
            receive_buffer.resize(HEADER_SIZE, 0);
        } else {
            receive_buffer = self.receive_buffer.take().unwrap();
        }

        while self.receive_len == 0 {
            let pos = self.receive_pos;
            assert!(pos < HEADER_SIZE);

            let mut header_buffer = ReadBuf::new(&mut receive_buffer[pos..HEADER_SIZE]);

            match Pin::new(&mut self.input).poll_read(cx, &mut header_buffer) {
                Poll::Ready(Ok(())) => {
                    // Disconnect?
                    if header_buffer.filled().is_empty() {
                        log::trace!("Got disconnect while reading header");
                        return Poll::Ready(Ok(()));
                    }

                    self.receive_pos += header_buffer.filled().len();

                    match self.receive_pos.cmp(&HEADER_SIZE) {
                        Ordering::Equal => {
                            // we got the header
                            self.receive_len =
                                bincode::deserialize(&receive_buffer[..HEADER_SIZE]).unwrap();
                            assert!(self.receive_len > 0);

                            self.receive_pos = 0;

                            let offset = self.receive_len % BLOCK_SIZE;
                            let padding = BLOCK_SIZE - offset;
                            let total_len = self.receive_len + padding;

                            receive_buffer.resize(total_len, 0);
                        }
                        Ordering::Greater => {
                            panic!("Read more than header!");
                        }
                        _ => {}
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    self.receive_buffer = Some(receive_buffer);
                    return Poll::Pending;
                }
            }
        }

        let mut read_buffer = ReadBuf::new(&mut receive_buffer[..]);
        read_buffer.set_filled(self.receive_pos);

        // Is there more data we need to fetch?
        if read_buffer.remaining() > 0 {
            while read_buffer.remaining() > 0 {
                match Pin::new(&mut self.input).poll_read(cx, &mut read_buffer) {
                    Poll::Ready(Ok(())) => {
                        match self.receive_pos.cmp(&read_buffer.filled().len()) {
                            Ordering::Equal => {
                                log::trace!("Got disconnect while receiving data");
                                return Poll::Ready(Ok(()));
                            }
                            Ordering::Less => {
                                self.receive_pos = read_buffer.filled().len();
                            }
                            Ordering::Greater => {
                                panic!("Filled buffer decreased in size");
                            }
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.receive_buffer = Some(receive_buffer);
                        return Poll::Pending;
                    }
                }
            }

            assert!(read_buffer.remaining() == 0);

            // Decrypt data
            let cipher = Aes::new();
            cipher
                .decrypt(read_buffer.filled_mut())
                .expect("Failed to decrypt data");
        }

        let len = read_buffer
            .filled()
            .len()
            .min(self.receive_len)
            .min(out_buffer.remaining());

        assert!(len > 0);

        out_buffer.put_slice(&receive_buffer[..len]);

        if len < self.receive_len {
            receive_buffer.advance(len);
            self.receive_pos -= len;
            self.receive_len -= len;
            self.receive_buffer = Some(receive_buffer);
        } else {
            self.receive_buffer = None;
            self.receive_pos = 0;
            self.receive_len = 0;
        }

        Poll::Ready(Ok(()))
    }
}
