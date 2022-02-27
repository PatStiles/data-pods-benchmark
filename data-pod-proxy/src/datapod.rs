use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};

use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};

use futures::sink::SinkExt;
use futures::stream::StreamExt;

use bytes::Bytes;
use rand::random;

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Mutex as StdMutex;
use std::sync::{Arc, Condvar};
use std::thread;
use std::time::Duration;

use crate::remote_parties::{RemoteParties, RemotePartyConnection};

use data_pods_utils::enclave_protocol::{EnclaveMessage, ProxyMessage};

type ReadSocket = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
type WriteSocket = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

/// In milliseconds
const EPOCH_LENGTH: u64 = 500;

struct MessageHandler {
    remote_parties: Arc<RemoteParties>,
    datapod_connections: Arc<Mutex<DatapodConnections>>,
}

pub struct SyncConnection {
    message_handler: MessageHandler,

    // todo change this to a tokio channel
    is_ready: StdMutex<bool>,
    ready_cond: Condvar,
}

pub struct DatapodConnection {
    //Use a FIFO queue her to ensure messages
    //are forwarded in order
    send_queue: mpsc::UnboundedSender<ProxyMessage>,
}

pub type DatapodConnections = Vec<Arc<DatapodConnection>>;

impl SyncConnection {
    pub async fn new(
        enclave_port: u16,
        blockchain_address: String,
        server_name: String,
        remote_parties: Arc<RemoteParties>,
        datapod_connections: Arc<Mutex<DatapodConnections>>,
    ) -> Result<Arc<Self>, ()> {
        let addr_str = format!("127.0.0.1:{}", enclave_port);
        let addr = addr_str.parse::<SocketAddr>().unwrap();

        let stream = if let Ok(stream) = TcpStream::connect(addr).await {
            stream
        } else {
            return Err(());
        };
        stream.set_nodelay(true).unwrap();

        let (read_socket, write_socket) = stream.into_split();

        let read_framed = FramedRead::new(read_socket, LengthDelimitedCodec::new());
        let mut write_framed = FramedWrite::new(write_socket, LengthDelimitedCodec::new());

        let message_handler = MessageHandler {
            remote_parties,
            datapod_connections,
        };

        let is_ready = StdMutex::new(false);
        let ready_cond = Condvar::new();

        let obj = Arc::new(SyncConnection {
            message_handler,
            is_ready,
            ready_cond,
        });

        let request = ProxyMessage::BlockchainConnect {
            address: blockchain_address,
            name: server_name,
        };

        obj.send_request(&mut write_framed, request).await;

        {
            let obj = obj.clone();

            tokio::spawn(async move {
                obj.send_loop(write_framed).await;
            });
        }

        {
            let obj = obj.clone();

            tokio::spawn(async move {
                obj.receive_loop(read_framed).await;
            });
        }

        Ok(obj)
    }

    #[inline]
    async fn send_request(&self, write_framed: &mut WriteSocket, request: ProxyMessage) {
        let data = bincode::serialize(&request).expect("Serialize ProxyMessage");

        if let Err(err) = write_framed.send(data.into()).await {
            panic!(
                "Failed to write data to socket: {err}. Did we lose connection to the data pod?"
            );
        }
    }

    pub fn wait_enclave_ready(&self) {
        #[allow(clippy::mutex_atomic)]
        let mut ready = self.is_ready.lock().unwrap();

        while !*ready {
            ready = self.ready_cond.wait(ready).unwrap();
        }
    }

    pub async fn send_loop(&self, mut write_framed: WriteSocket) {
        loop {
            let interval = Duration::from_millis(EPOCH_LENGTH);
            thread::sleep(interval);

            log::trace!("Sending sync request to data pod");
            self.send_request(&mut write_framed, ProxyMessage::SyncRequest)
                .await;
        }
    }

    pub async fn receive_loop(&self, mut read_framed: ReadSocket) {
        while let Some(result) = read_framed.next().await {
            match result {
                Ok(data) => {
                    let msg: EnclaveMessage = bincode::deserialize(&data).unwrap();
                    self.handle_message(msg).await;
                }
                Err(err) => {
                    log::error!("error on decoding from socket; error = {err:?}");
                    return;
                }
            }
        }
    }

    async fn handle_message(&self, msg: EnclaveMessage) {
        log::trace!("Got message from enclave");

        match msg {
            EnclaveMessage::EnclaveReady => {
                // this will only be sent to the syn connection...

                #[allow(clippy::mutex_atomic)]
                let mut val = self.is_ready.lock().unwrap();
                *val = true;

                self.ready_cond.notify_all();
            }
            _ => {
                self.message_handler.handle_message(msg).await;
            }
        }
    }
}

impl DatapodConnection {
    pub async fn new(
        enclave_port: u16,
        remote_parties: Arc<RemoteParties>,
        datapod_connections: Arc<Mutex<DatapodConnections>>,
    ) -> Self {
        let addr_str = format!("127.0.0.1:{enclave_port}");
        let addr = addr_str.parse::<SocketAddr>().unwrap();

        let stream = TcpStream::connect(addr)
            .await
            .expect("Connecting to data pod");

        let (read_socket, write_socket) = stream.into_split();

        let mut read_framed = FramedRead::new(read_socket, LengthDelimitedCodec::new());
        let mut write_framed = FramedWrite::new(write_socket, LengthDelimitedCodec::new());

        let message_handler = MessageHandler {
            remote_parties,
            datapod_connections,
        };
        let (send_queue, mut send_queue_out) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(msg) = send_queue_out.recv().await {
                let bin_data = bincode::serialize(&msg).unwrap();
                let bytes = Bytes::from(bin_data);

                match write_framed.send(bytes).await {
                    Ok(()) => {}
                    Err(err) => {
                        log::error!("Forwarding to data pod failed: {err}");
                        return;
                    }
                }
            }
        });

        tokio::spawn(async move {
            while let Some(result) = read_framed.next().await {
                match result {
                    Ok(data) => {
                        let msg = bincode::deserialize(&data).expect("Parse EnclaveMessage");
                        message_handler.handle_message(msg).await;
                    }
                    Err(err) => {
                        log::error!("Failed to read from datapod socket: {err:?}");
                        return;
                    }
                }
            }

            log::info!("Data pod connection terminated");
        });

        Self { send_queue }
    }

    pub async fn forward_message(&self, msg: ProxyMessage) {
        self.send_queue.send(msg).unwrap();
    }
}

impl MessageHandler {
    pub async fn handle_message(&self, msg: EnclaveMessage) {
        log::trace!("Got message from enclave");

        match msg {
            EnclaveMessage::ForwardResponse { rp_id, payload } => {
                let remote_party = match self.remote_parties.get(rp_id).await {
                    Some(rp) => rp,
                    None => {
                        log::error!("Failed to forward response: no such remote_party #{rp_id}");
                        return;
                    }
                };

                remote_party.forward(payload).await;
            }
            EnclaveMessage::RequestConnect { address } => {
                log::info!("Connecting to {address}");

                let socket_addr = address.to_socket_addrs().unwrap().next().unwrap();

                let identifier = self.remote_parties.get_next_id();

                let db_conn = {
                    let db_conns = self.datapod_connections.lock().await;
                    // assign the new remote party to a random connectionn
                    db_conns[random::<usize>() % db_conns.len()].clone()
                };

                let conn = RemotePartyConnection::connect_to(
                    socket_addr,
                    identifier,
                    db_conn.clone(),
                    self.remote_parties.clone(),
                )
                .await
                .expect("Failed to connect to data pod");

                self.remote_parties.insert(identifier, conn.clone()).await;

                let msg = ProxyMessage::NotifyConnect {
                    identifier,
                    initiating: true,
                };
                db_conn.forward_message(msg).await;
            }
            _ => {
                panic!("Got unexpected message!");
            }
        }
    }
}
