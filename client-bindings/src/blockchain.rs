use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};

use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;

use parking_lot::Mutex;

use std::collections::HashMap;
use std::fs::File;
use std::net::TcpStream as StdTcpStream;
use std::sync::Arc;

use data_pods_store::protocol::LedgerMessage;

use blockchain_simulator::protocol::EpochId;
use blockchain_simulator::protocol::Message as BlockchainMessage;
use blockchain_simulator::{
    generate_key_pair, to_account_id, AccountId, Ledger, PrivateKey, PublicKey, Transaction,
    TxPayload,
};

use crate::data_pod::DataPodConnection;

type WriteSocket = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

/// Manages the connection from a client to the global ledger
pub struct BlockchainConnection {
    #[allow(dead_code)]
    public_key: PublicKey,
    private_key: PrivateKey,
    account_id: AccountId,
    write_framed: Mutex<WriteSocket>,
    recv_task: Mutex<JoinHandle<()>>,
    #[allow(dead_code)]
    ledger: Arc<Ledger<LedgerMessage>>,
    ready_receiver: Mutex<Option<oneshot::Receiver<()>>>,
    data_pods: Arc<TokioMutex<HashMap<AccountId, Arc<DataPodConnection>>>>,
}

impl BlockchainConnection {
    pub async fn new(identifier: u32, stream: StdTcpStream) -> Self {
        let wallet_name = format!("client{identifier}.wallet");

        let tstream = TcpStream::from_std(stream).unwrap();
        let (read_stream, write_stream) = tstream.into_split();

        let ledger = Arc::new(Ledger::default());
        let data_pods = Arc::new(TokioMutex::new(Default::default()));

        let (ready_sender, ready_receiver) = oneshot::channel();

        let (private_key, public_key) = match File::open(&wallet_name) {
            Ok(f) => {
                // load keys from disk
                bincode::deserialize_from(f).unwrap()
            }
            Err(_) => {
                // generate new keys
                let pair = generate_key_pair();
                let f = File::create(&wallet_name).unwrap();

                // save to disk
                bincode::serialize_into(f, &pair).unwrap();

                pair
            }
        };

        let account_id = to_account_id(&public_key);

        let mut read_framed = FramedRead::new(read_stream, LengthDelimitedCodec::new());
        let write_framed = Mutex::new(FramedWrite::new(write_stream, LengthDelimitedCodec::new()));

        let l2 = ledger.clone();
        let dp2 = data_pods.clone();

        let recv_task = tokio::spawn(async move {
            let mut ready_sender = Some(ready_sender);

            while let Some(res) = read_framed.next().await {
                match res {
                    Ok(data) => {
                        let msg = bincode::deserialize(&data).unwrap();
                        BlockchainConnection::handle_message(&*l2, msg, &mut ready_sender, &*dp2)
                            .await;
                    }
                    Err(err) => {
                        panic!("Failed to receive data from blockchain: {err}");
                    }
                }
            }
        });

        let ready_receiver = Mutex::new(Some(ready_receiver));
        let recv_task = Mutex::new(recv_task);

        Self {
            account_id,
            private_key,
            public_key,
            write_framed,
            ledger,
            ready_receiver,
            data_pods,
            recv_task,
        }
    }

    pub async fn disconnect(&self) {
        {
            let mut sock = self.write_framed.lock();
            sock.close().await.expect("Failed to close socket");
        }

        {
            let recv_task = self.recv_task.lock();
            recv_task.abort();
        }
    }

    pub fn get_ledger(&self) -> Arc<Ledger<LedgerMessage>> {
        self.ledger.clone()
    }

    #[inline]
    fn check_sync(ledger: &Ledger<LedgerMessage>, ready_sender: &mut Option<oneshot::Sender<()>>) {
        if ledger.has_gaps() {
            return;
        }

        let eid = (ledger.num_epochs() as EpochId) - 1;
        let epoch_timestamp = ledger.get_epoch_timestamp(eid);

        // Check if we got a recent-ish block
        let now = chrono::offset::Utc::now();
        let local_timestamp = now.timestamp();

        let diff = if local_timestamp > epoch_timestamp {
            local_timestamp - epoch_timestamp
        } else {
            0
        };

        // We are in sync!
        if diff < 10 {
            let sender = ready_sender.take().unwrap();
            sender.send(()).unwrap();
        }
    }

    pub async fn register_data_pod(&self, location: AccountId, data_pod: Arc<DataPodConnection>) {
        let mut data_pods = self.data_pods.lock().await;

        // Check if there are old message we should
        // notify the data pod about
        for i in 0..self.ledger.num_epochs() {
            let epoch = self.ledger.get_epoch(i as u32);

            for tx in epoch.get_transactions() {
                if tx.get_source() == &location {
                    if let TxPayload::Operation {
                        operation:
                            LedgerMessage::SyncState {
                                position,
                                state_hash,
                                ..
                            },
                    } = tx.get_payload()
                    {
                        data_pod.add_sync_transaction(*position, *state_hash);
                    }
                }
            }
        }

        data_pods.insert(location, data_pod);
    }

    async fn handle_message(
        ledger: &Ledger<LedgerMessage>,
        message: BlockchainMessage<LedgerMessage>,
        ready_sender: &mut Option<oneshot::Sender<()>>,
        data_pods: &TokioMutex<HashMap<AccountId, Arc<DataPodConnection>>>,
    ) {
        match message {
            BlockchainMessage::NewEpochStarted {
                identifier,
                timestamp,
            } => {
                ledger.create_new_epoch(identifier, timestamp);

                if ready_sender.is_some() {
                    BlockchainConnection::check_sync(ledger, ready_sender);
                }
            }
            BlockchainMessage::SyncEpoch { identifier, epoch } => {
                for transaction in epoch.get_transactions() {
                    let source = *transaction.get_source();

                    if let TxPayload::Operation { operation } = transaction.clone().into_payload() {
                        BlockchainConnection::handle_ledger_message(source, operation, data_pods)
                            .await;
                    }
                }

                ledger.synchronize_epoch(identifier, epoch);

                if ready_sender.is_some() {
                    BlockchainConnection::check_sync(ledger, ready_sender);
                }
            }

            BlockchainMessage::LedgerUpdate { transaction } => {
                ledger.insert(transaction.clone());
                let source = *transaction.get_source();

                if let TxPayload::Operation { operation } = transaction.clone().into_payload() {
                    BlockchainConnection::handle_ledger_message(source, operation, data_pods).await;
                }
            }
            _ => {
                panic!("Got unexpected message from blockchain: {message:?}");
            }
        }
    }

    async fn handle_ledger_message(
        source: AccountId,
        message: LedgerMessage,
        data_pods: &TokioMutex<HashMap<AccountId, Arc<DataPodConnection>>>,
    ) {
        if let LedgerMessage::SyncState {
            position,
            state_hash,
            ..
        } = message
        {
            if let Some(data_pod) = data_pods.lock().await.get(&source) {
                data_pod.add_sync_transaction(position, state_hash);
            }
        } else {
            //ignore
        }
    }

    pub async fn send(&self, message: LedgerMessage) {
        let transaction = Transaction::new(self.account_id, message, &self.private_key);

        let request = BlockchainMessage::TransactionRequest { transaction };
        let data = bincode::serialize(&request).expect("Failed to serialize message");

        let mut sock = self.write_framed.lock();

        if let Err(err) = sock.send(data.into()).await {
            log::error!("Failed to write data to socket: {err}");
        }
    }

    // Blocks until we have received the most recent-ish block
    pub async fn wait_ready(&self) {
        let receiver = self.ready_receiver.lock().take();

        if let Some(r) = receiver {
            r.await.unwrap();
        } else {
            panic!("Unexpected state!");
        }
    }

    #[inline]
    pub fn get_account_id(&self) -> AccountId {
        self.account_id
    }

    pub async fn get_digest_start_end(
        &self,
        location: AccountId,
        start: EpochId,
        end: EpochId,
    ) -> (usize, usize) {
        let spos = {
            let mut eid = start;
            let mut result = None;

            while result.is_none() {
                if eid > end {
                    panic!(
                        "Failed to find digest start for epoch {start} and data pod {location:x}"
                    );
                }

                let epoch = self.ledger.get_epoch(eid);

                for tx in epoch.get_transactions() {
                    if *tx.get_source() != location {
                        continue;
                    }

                    if let TxPayload::Operation {
                        operation: LedgerMessage::SyncState { position, .. },
                    } = tx.get_payload()
                    {
                        result = Some(*position);
                        break;
                    }
                }

                log::debug!("Data pod did not publish a digest in epoch #{eid}");
                eid += 1;
            }

            result.unwrap()
        };

        let epos = {
            let mut eid = end;
            let mut result = None;

            while result.is_none() {
                if eid < start {
                    panic!("Failed to find digest end for epoch {end} and data pod {location:x}");
                }

                let epoch = self.ledger.get_epoch(eid);

                for tx in epoch.get_transactions() {
                    if *tx.get_source() != location {
                        continue;
                    }

                    if let TxPayload::Operation {
                        operation: LedgerMessage::SyncState { position, .. },
                    } = tx.get_payload()
                    {
                        result = Some(*position);
                        break;
                    }
                }

                log::warn!("Data pod did not publish a digest in epoch #{eid}");
                eid -= 1;
            }

            result.unwrap()
        };

        (spos, epos)
    }
}
