use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};

use parking_lot::Mutex;

use std::net::TcpStream as StdTcpStream;
use std::sync::Mutex as StdMutex;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;

use blockchain_simulator::AccountId;

use data_pods_store::merkle::MerkleHash;
use data_pods_store::protocol::{Message, OpId};
use data_pods_store::{Digest, DigestBatch, DigestStatistics};

use crate::requests::{CallRequestResult, GetRequestResult};

cfg_if::cfg_if! {
    if #[ cfg(feature="use-tls") ] {
        use secure_channel::ClientInput as SecureChannelIn;
        use secure_channel::ClientOutput as SecureChannelOut;

        type WriteSocket = FramedWrite<SecureChannelOut, LengthDelimitedCodec>;
        type ReadSocket = FramedRead<SecureChannelIn, LengthDelimitedCodec>;
    } else {
        use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

        type WriteSocket = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;
        type ReadSocket = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
    }
}

#[derive(Debug, Default)]
struct DigestSyncQueue {
    // Transactions to process for which we
    // have not received a digest batch yet
    pending_transactions: HashMap<usize, MerkleHash>,

    // Digest batches to process for which we
    // have not received a transaction yet
    pending_batches: HashMap<usize, DigestBatch>,

    // Is a task waiting for the sync to be done?
    wait_flag: bool,

    // Are we already connected to the data pod?
    connection_established: bool,

    ready_sender: Option<oneshot::Sender<()>>,
}

impl DigestSyncQueue {
    fn new(ready_sender: oneshot::Sender<()>) -> Self {
        let pending_transactions = HashMap::new();
        let pending_batches = HashMap::new();

        Self {
            pending_transactions,
            pending_batches,
            wait_flag: false,
            connection_established: false,
            ready_sender: Some(ready_sender),
        }
    }

    fn check_ready(&mut self) {
        if self.pending_transactions.is_empty()
            && self.pending_batches.is_empty()
            && self.connection_established
        {
            let sender = self.ready_sender.take();
            self.wait_flag = false;

            if let Some(s) = sender {
                s.send(()).unwrap();
            } else {
                panic!("Got wait flag more than once");
            }
        }
    }
}

type GetResultMap = HashMap<OpId, (Instant, Arc<GetRequestResult>)>;
type CallResultMap = HashMap<OpId, (Instant, Arc<CallRequestResult>)>;

pub struct DataPodConnection {
    write_framed: Mutex<WriteSocket>,

    //TODO we shouldn't set this dynamically
    // instead we set it on connect and only verify
    location: Arc<Mutex<Option<AccountId>>>,

    location_ready_receiver: Mutex<Option<oneshot::Receiver<()>>>,
    ready_receiver: Mutex<Option<oneshot::Receiver<()>>>,

    digest: Arc<Digest>,

    get_requests: Arc<StdMutex<GetResultMap>>,
    call_requests: Arc<StdMutex<CallResultMap>>,

    #[allow(dead_code)]
    digest_sync_queue: Arc<Mutex<DigestSyncQueue>>,

    recv_task: Mutex<Option<JoinHandle<()>>>,
}

impl DataPodConnection {
    pub async fn new(stream: StdTcpStream) -> Arc<Self> {
        let tstream = TcpStream::from_std(stream).unwrap();
        let (read_sock, write_sock) = tstream.into_split();

        cfg_if::cfg_if! {
            if #[ cfg(feature="use-tls") ] {
                let read_stream  = SecureChannelIn::new( Box::new( read_sock ) );
                let write_stream = SecureChannelOut::new( Box::new( write_sock ) );
            } else {
                log::warn!("TLS disabled; only use this for debugging!");

                let read_stream = read_sock;
                let write_stream = write_sock;
            }
        }

        let (location_ready_sender, location_ready_receiver) = oneshot::channel();
        let (ready_sender, ready_receiver) = oneshot::channel();

        let digest = Arc::new(Digest::default());
        let read_framed = FramedRead::new(read_stream, LengthDelimitedCodec::new());
        let write_framed = Mutex::new(FramedWrite::new(write_stream, LengthDelimitedCodec::new()));

        let get_requests = Arc::new(Default::default());
        let call_requests = Arc::new(Default::default());

        let location = Arc::new(Mutex::new(None));

        let digest_sync_queue = Arc::new(Mutex::new(DigestSyncQueue::new(ready_sender)));

        let ready_receiver = Mutex::new(Some(ready_receiver));
        let location_ready_receiver = Mutex::new(Some(location_ready_receiver));

        let recv_task = Mutex::new(None);

        let obj = Arc::new(Self {
            write_framed,
            get_requests,
            call_requests,
            digest,
            recv_task,
            location,
            ready_receiver,
            location_ready_receiver,
            digest_sync_queue,
        });

        {
            let obj = obj.clone();
            let obj2 = obj.clone();

            let recv_task = tokio::spawn(async move {
                DataPodConnection::handle_messages(obj2, read_framed, location_ready_sender).await;
            });

            *obj.recv_task.lock() = Some(recv_task);
        }

        obj
    }

    async fn handle_messages(
        dpc: Arc<DataPodConnection>,
        mut read_framed: ReadSocket,
        location_ready_sender: oneshot::Sender<()>,
    ) {
        let digest = &dpc.digest;
        let get_requests = &dpc.get_requests;
        let call_requests = &dpc.call_requests;
        let location_handle = &dpc.location;
        let digest_sync_queue = &dpc.digest_sync_queue;

        let mut location_ready_sender = Some(location_ready_sender);

        while let Some(result) = read_framed.next().await {
            let msg = match result {
                Ok(data) => bincode::deserialize(&data).expect("Parse request"),
                Err(err) => {
                    log::error!("Failed to read from socket; error = {err:?}");
                    return;
                }
            };

            match msg {
                Message::CallResponse { op_id, result } => {
                    let mut requests = call_requests.lock().unwrap();

                    match requests.remove(&op_id) {
                        Some((start, hdl)) => {
                            let now = Instant::now();
                            let latency = now.duration_since(start);
                            hdl.set_inner(result, latency);
                        }
                        None => {
                            // Can happen upon reconnect
                            log::warn!("Got unexpected response: No such call request #{op_id}");
                        }
                    }
                }
                Message::GetResponse { op_id, result } => {
                    let mut requests = get_requests.lock().unwrap();

                    match requests.remove(&op_id) {
                        Some((start, hdl)) => {
                            let now = Instant::now();
                            let latency = now.duration_since(start);
                            hdl.set_inner(result, latency);
                        }
                        None => {
                            // Can happen upon reconnect
                            log::warn!("Got unexpected response: No such get request #{op_id}");
                        }
                    }
                }
                Message::SendLocation { location } => {
                    let sender = location_ready_sender.take();

                    if let Some(s) = sender {
                        log::trace!("Got location from data pod");
                        *location_handle.lock() = Some(location);

                        s.send(()).unwrap();
                    } else {
                        panic!("Got location more than once");
                    }
                }
                Message::ConnectionEstablished {} => {
                    let mut digest_sync_queue = digest_sync_queue.lock();
                    digest_sync_queue.connection_established = true;

                    if digest_sync_queue.wait_flag {
                        digest_sync_queue.check_ready();
                    }
                }
                Message::SendDigestBatch { position, batch } => {
                    let mut digest_sync_queue = digest_sync_queue.lock();

                    if let Some(_tx) = digest_sync_queue.pending_transactions.remove(&position) {
                        digest.insert_batch(position, batch);

                        if digest_sync_queue.wait_flag {
                            digest_sync_queue.check_ready();
                        }
                    } else {
                        // we might receive duplicates while establishing a new connection
                        if !digest.has_batch(position) {
                            digest_sync_queue.pending_batches.insert(position, batch);
                        }
                    }
                }
                _ => {
                    panic!("Got unexpected message from data pod!");
                }
            }
        }

        log::debug!("Disconnected from data pod");
    }

    pub async fn wait_location_ready(&self) {
        let receiver = self.location_ready_receiver.lock().take();
        receiver.unwrap().await.expect("Location wait failed!");
    }

    pub async fn wait_ready(&self) {
        {
            let mut dsq = self.digest_sync_queue.lock();

            if dsq.pending_transactions.is_empty()
                && dsq.pending_batches.is_empty()
                && dsq.connection_established
            {
                //up to date
                return;
            } else {
                dsq.wait_flag = true;
            }
        }

        log::trace!("Waiting for data pod to be ready");
        let receiver = { self.ready_receiver.lock().take() };

        receiver.unwrap().await.expect("Failed to get ready-state");
    }

    pub async fn disconnect(&self) {
        log::trace!("Disconnecting from data pod");
        self.recv_task.lock().take().unwrap().abort();

        let mut framed = self.write_framed.lock();
        let sock = framed.get_mut();

        if sock.shutdown().await.is_err() {
            panic!("Closing connection failed. Did you call close() more than once?");
        }
    }

    pub fn add_sync_transaction(&self, position: usize, hash: MerkleHash) {
        let mut dsq = self.digest_sync_queue.lock();

        if let Some(batch) = dsq.pending_batches.remove(&position) {
            self.digest.insert_batch(position, batch);

            if dsq.wait_flag {
                dsq.check_ready();
            }
        } else {
            dsq.pending_transactions.insert(position, hash);
        }
    }

    pub async fn get_location(&self) -> AccountId {
        self.location
            .lock()
            .expect("Data pod does not have a location yet")
    }

    pub async fn get_statistics(&self, start: usize, end: usize) -> DigestStatistics {
        self.digest
            .get_statistics(self.get_location().await, start, end)
    }

    pub async fn send_location(&self, location: AccountId) {
        let message = Message::SendLocation { location };
        self.send(message).await;
    }

    pub async fn send(&self, message: Message) {
        let data = bincode::serialize(&message).expect("Serialize message");
        let mut sock = self.write_framed.lock();

        match sock.send(data.into()).await {
            Ok(()) => log::trace!("Sent message to data pod."),
            Err(err) => log::error!("Failed to write data to socket: {err}"),
        }
    }

    pub fn insert_get_request(&self, op_id: OpId, result: Arc<GetRequestResult>) {
        let now = Instant::now();
        let mut requests = self.get_requests.lock().unwrap();
        requests.insert(op_id, (now, result));
    }

    pub fn insert_call_request(&self, op_id: OpId, result: Arc<CallRequestResult>) {
        let now = Instant::now();
        let mut requests = self.call_requests.lock().unwrap();
        requests.insert(op_id, (now, result));
    }
}
