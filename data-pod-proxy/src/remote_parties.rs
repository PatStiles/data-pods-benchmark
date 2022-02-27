use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use std::collections::HashMap;

use data_pods_utils::enclave_protocol::ProxyMessage;

use crate::datapod::DatapodConnection;

use serde_bytes::ByteBuf;

pub struct RemotePartyConnection {
    // A FIFO queue is used here to ensure
    // messages are forwarded in order
    send_queue: Mutex<mpsc::UnboundedSender<ByteBuf>>,
    identifier: u32,
    data_pod: Arc<DatapodConnection>,
    remote_parties: Arc<RemoteParties>,
}

pub struct RemoteParties {
    remote_parties: Mutex<HashMap<u32, Arc<RemotePartyConnection>>>,
    next_id: AtomicU32,
}

impl RemotePartyConnection {
    pub async fn new(
        socket: TcpStream,
        identifier: u32,
        data_pod: Arc<DatapodConnection>,
        remote_parties: Arc<RemoteParties>,
    ) -> Arc<Self> {
        log::trace!("Created new remote party with id={identifier}");

        let (send_queue, send_queue_out) = mpsc::unbounded_channel::<ByteBuf>();

        let obj = Arc::new(Self {
            send_queue: Mutex::new(send_queue),
            identifier,
            data_pod,
            remote_parties,
        });

        RemotePartyConnection::start_background_tasks(obj.clone(), send_queue_out, socket).await;

        obj
    }

    pub async fn connect_to(
        address: SocketAddr,
        identifier: u32,
        data_pod: Arc<DatapodConnection>,
        remote_parties: Arc<RemoteParties>,
    ) -> Option<Arc<Self>> {
        let socket = match TcpStream::connect(address).await {
            Ok(s) => s,
            Err(err) => {
                log::error!("Failed to connect to peer at {address}: {err}");
                return None;
            }
        };

        socket.set_nodelay(true).unwrap();

        let (send_queue, send_queue_out) = mpsc::unbounded_channel::<ByteBuf>();
        let obj = Arc::new(Self {
            identifier,
            send_queue: Mutex::new(send_queue),
            remote_parties,
            data_pod,
        });

        RemotePartyConnection::start_background_tasks(obj.clone(), send_queue_out, socket).await;

        Some(obj)
    }

    async fn start_background_tasks(
        self_ptr: Arc<Self>,
        mut send_queue: mpsc::UnboundedReceiver<ByteBuf>,
        socket: TcpStream,
    ) {
        // There is no framed read and write here as we just pass
        // through encrypted data, and it is re-assembled inside the enclave

        socket.set_nodelay(true).unwrap();

        let (mut read_socket, mut write_socket) = socket.into_split();

        tokio::spawn(async move {
            while let Some(msg) = send_queue.recv().await {
                match write_socket.write_all(&msg).await {
                    Ok(()) => {}
                    Err(err) => {
                        log::error!("Forwarding to remote party failed: {err}");
                        return;
                    }
                }
            }
            log::trace!("Send queue for remote party closed");
        });

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            log::trace!("Staring receive loop for remote party");

            loop {
                let len = match read_socket.read(&mut buf[..]).await {
                    Ok(l) => l,
                    Err(err) => {
                        log::warn!(
                            "Failed to read from remote party #{}'s socket: {err}",
                            self_ptr.identifier
                        );
                        self_ptr.notify_close().await;
                        return;
                    }
                };

                if len > 0 {
                    log::trace!("Got {len} bytes from remote party");

                    let mut payload = vec![];
                    payload.extend_from_slice(&buf[..len]);

                    let msg = ProxyMessage::ForwardMessage {
                        identifier: self_ptr.identifier,
                        payload: ByteBuf::from(payload),
                    };
                    self_ptr.data_pod.forward_message(msg).await;
                } else {
                    log::info!(
                        "Peer {} disconnected from data pod proxy",
                        self_ptr.identifier
                    );
                    self_ptr.notify_close().await;
                    return;
                }
            }
        });
    }

    async fn notify_close(&self) {
        log::trace!("Connection to remote party closed");

        let msg = ProxyMessage::NotifyDisconnect {
            identifier: self.identifier,
        };
        self.data_pod.forward_message(msg).await;

        self.remote_parties.remove(self.identifier).await;
    }

    pub async fn forward(&self, msg: ByteBuf) {
        // This might fail during disconnect
        let _ = self.send_queue.lock().await.send(msg);
    }
}

impl RemoteParties {
    pub fn new() -> Self {
        Self {
            remote_parties: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(1),
        }
    }

    pub fn get_next_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn insert(&self, identifier: u32, rp: Arc<RemotePartyConnection>) {
        let mut remote_parties = self.remote_parties.lock().await;
        let result = remote_parties.insert(identifier, rp);

        if result.is_some() {
            panic!("Remote party already existed!");
        }
    }

    pub async fn remove(&self, identifier: u32) {
        let mut remote_parties = self.remote_parties.lock().await;
        remote_parties.remove(&identifier);
    }

    pub async fn get(&self, identifier: u32) -> Option<Arc<RemotePartyConnection>> {
        let remote_parties = self.remote_parties.lock().await;
        remote_parties.get(&identifier).cloned()
    }
}
