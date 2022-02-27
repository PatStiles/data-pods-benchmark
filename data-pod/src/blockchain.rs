use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::exit;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use bytes::{Bytes, BytesMut};

use tokio::runtime::Runtime as TokioRuntime;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{Decoder, Encoder};

use blockchain_simulator::protocol::{EpochId, Message};
use blockchain_simulator::{
    generate_key_pair, to_account_id, AccountId, Ledger, PrivateKey, PublicKey, Transaction,
    TxPayload,
};

use data_pods_store::merkle::get_next_pow2;
use data_pods_store::protocol::LedgerMessage;
use data_pods_store::{Datastore, EpochHashes, MetaOperation};

use crate::applications::ApplicationRegistry;
use crate::enclave::Enclave;
use crate::remote_parties::RemoteParties;

struct Notifier {
    counter: AtomicUsize,
    notify: Arc<Semaphore>,
}

/// This manages the connection from the data pod to the global ledger
pub struct BlockchainConnection {
    public_key: PublicKey,
    private_key: PrivateKey,
    account_id: AccountId,
    input: StdMutex<Option<TcpStream>>,
    output: StdMutex<(LengthDelimitedCodec, Option<TcpStream>)>,
    new_digest_watch: Arc<Notifier>,
    tokio_runtime: Arc<TokioRuntime>,
    inner: Arc<BlockchainHandler>,
    datastore: Arc<Datastore>,
    last_merkle_tree: Arc<StdMutex<Option<EpochHashes>>>,
    enclave: Arc<Enclave>,
}

struct BlockchainHandler {
    account_id: AccountId,
    enclave: Arc<Enclave>,
    ledger: Arc<Ledger<LedgerMessage>>,
    datastore: Arc<Datastore>,
    applications: Arc<ApplicationRegistry>,
    global_epoch: AtomicU32,
    remote_parties: Arc<RemoteParties>,
    last_merkle_tree: Arc<StdMutex<Option<EpochHashes>>>,
    epoch_sender: mpsc::UnboundedSender<EpochId>,
    new_digest_watch: Arc<Notifier>,
}

impl BlockchainHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        enclave: Arc<Enclave>,
        ledger: Arc<Ledger<LedgerMessage>>,
        datastore: Arc<Datastore>,
        applications: Arc<ApplicationRegistry>,
        remote_parties: Arc<RemoteParties>,
        last_merkle_tree: Arc<StdMutex<Option<EpochHashes>>>,
        epoch_sender: mpsc::UnboundedSender<EpochId>,
        account_id: AccountId,
        new_digest_watch: Arc<Notifier>,
    ) -> Self {
        let global_epoch = AtomicU32::new(0);

        Self {
            account_id,
            enclave,
            ledger,
            datastore,
            applications,
            remote_parties,
            last_merkle_tree,
            global_epoch,
            epoch_sender,
            new_digest_watch,
        }
    }

    pub fn get_current_global_epoch(&self) -> EpochId {
        self.global_epoch.load(Ordering::SeqCst)
    }

    pub async fn handle_message(&self, message: Message<LedgerMessage>) {
        match message {
            Message::NewEpochStarted {
                identifier,
                timestamp,
            } => {
                log::info!("New global epoch started");

                self.ledger.create_new_epoch(identifier, timestamp);
                self.global_epoch.store(identifier, Ordering::SeqCst);

                self.epoch_sender.send(identifier).unwrap();
            }
            Message::LedgerUpdate { transaction } => {
                log::trace!("Got new global transaction");

                self.ledger.insert(transaction.clone());
                let source = *transaction.get_source();

                let payload = match transaction.into_payload() {
                    TxPayload::Operation { operation } => operation,
                    _ => {
                        return;
                    }
                };

                self.handle_ledger_update(source, payload).await;
            }
            Message::SyncEpoch { identifier, epoch } => {
                self.ledger.synchronize_epoch(identifier, epoch)
            }
            Message::TransactionRequest { transaction: _ } => {
                panic!("Should not have received transaction requests");
            }
        }
    }

    pub async fn handle_ledger_update(&self, source: AccountId, payload: LedgerMessage) {
        match payload {
            LedgerMessage::SyncState {
                state_hash,
                position,
                ..
            } => {
                if source == self.account_id {
                    let digest = self.datastore.get_digest();
                    let batch = digest.get_last_batch();
                    let previous_epoch = digest.num_batches() - 1;

                    if position != previous_epoch {
                        panic!("Got sync state for invalid epoch");
                    }

                    let hashes = Arc::new(self.last_merkle_tree.lock().unwrap().take().unwrap());

                    if hashes.state_tree.get_hash() != &state_hash {
                        panic!("Hashes do not match!");
                    }

                    let notify_count = self.new_digest_watch.counter.load(Ordering::SeqCst);
                    self.new_digest_watch
                        .counter
                        .fetch_sub(notify_count, Ordering::SeqCst);
                    self.new_digest_watch.notify.add_permits(notify_count);

                    let transactions = Arc::new(batch.get_transaction_set());
                    let set_size = get_next_pow2(transactions.len());

                    let num_msgs = self
                        .remote_parties
                        .flush(&hashes, previous_epoch, &batch, &transactions, set_size)
                        .await;

                    self.enclave.flush(hashes, transactions, set_size).await;

                    log::info!(
                        "Digest size was {}. Flushed {num_msgs} messages",
                        batch.len()
                    );
                    log::info!("Local epoch #{} started", (previous_epoch + 1));
                }
            }
            LedgerMessage::CreateApplication { name, definition } => {
                log::info!("Registering new application \"{name}\"");

                self.datastore
                    .register_application(name.clone(), definition.types.clone());
                self.applications.create(name, definition);
            }
            LedgerMessage::ExecuteTxChunk {
                info,
                stage,
                func_id,
                meta_vars,
            } => {
                let global_values = self.applications.get_global_values(&info.application);

                if info.source_location == self.account_id {
                    let tx = self.enclave.get_transaction(info.identifier).await.unwrap();
                    tx.execute_global_chunk(stage, func_id, global_values).await;
                } else {
                    let tx = self.enclave.get_or_create_remote_transaction(&info).await;
                    let meta_fn_name = &info.fn_name;
                    let app_name = &info.application;

                    let fn_name;

                    let application = self.applications.get_app_definition(app_name);

                    let args = if let Some(f) = application.functions.get(meta_fn_name) {
                        if stage != 0 {
                            panic!("Invalid stage!");
                        }

                        let mut flat_args = Vec::new();

                        for (name, _) in f.0.iter() {
                            flat_args.push(name.clone());
                        }

                        fn_name = meta_fn_name.to_string();
                        flat_args
                    } else if let Some(f) = application.meta_fns.get(meta_fn_name) {
                        let op = &f.get_stage(stage)[func_id];

                        if let MetaOperation::Call {
                            args,
                            fn_name: _fn_name,
                            ..
                        } = op
                        {
                            fn_name = _fn_name.to_string();
                            args.clone()
                        } else {
                            panic!("Invalid state!");
                        }
                    } else {
                        panic!("Got invalid transaction: no such function \"{app_name}::{meta_fn_name}\"");
                    };

                    tx.execute_global_chunk(global_values, args, fn_name, meta_vars, application)
                        .await;
                }
            }
            LedgerMessage::FinalizeTransaction { info, abort } => {
                let global_values = self.applications.get_global_values(&info.application);

                if info.source_location == self.account_id {
                    let tx = self.enclave.get_transaction(info.identifier).await.unwrap();

                    let mut global_vals = global_values.lock().unwrap();
                    tx.finalize_global_ops(abort, &mut global_vals);
                } else {
                    let tx = match self.enclave.get_remote_transaction(info.identifier).await {
                        Some(tx) => tx,
                        None => {
                            if abort {
                                return;
                            } else {
                                panic!("No transaction with id={}", info.identifier);
                            }
                        }
                    };

                    let is_done = {
                        let mut global_vals = global_values.lock().unwrap();
                        tx.finalize_global_ops(abort, &mut global_vals)
                    };

                    if is_done {
                        self.enclave
                            .remove_remote_transaction(info.identifier)
                            .await;
                    }
                }
            }
        }
    }
}

impl BlockchainConnection {
    pub fn new(
        enclave: Arc<Enclave>,
        ledger: Arc<Ledger<LedgerMessage>>,
        datastore: Arc<Datastore>,
        applications: Arc<ApplicationRegistry>,
        remote_parties: Arc<RemoteParties>,
        tokio_runtime: Arc<TokioRuntime>,
    ) -> Self {
        let (private_key, public_key) = generate_key_pair();
        let account_id = to_account_id(&public_key);

        log::info!("Generated key pair. Account id is {account_id}.");

        let output = StdMutex::new((LengthDelimitedCodec::new(), None));
        let input = StdMutex::new(None);

        let new_digest_watch = Arc::new(Notifier {
            counter: AtomicUsize::new(0),
            notify: Arc::new(Semaphore::new(0)),
        });

        let last_merkle_tree = Arc::new(StdMutex::new(None));

        let (epoch_sender, mut rx) = mpsc::unbounded_channel();

        {
            let enclave = enclave.clone();

            // Ensures epoch updates are sequential
            tokio_runtime.spawn(async move {
                while let Some(identifier) = rx.recv().await {
                    enclave.notify_new_epoch_started(identifier).await;
                }
            });
        }

        let inner = Arc::new(BlockchainHandler::new(
            enclave.clone(),
            ledger,
            datastore.clone(),
            applications,
            remote_parties,
            last_merkle_tree.clone(),
            epoch_sender,
            account_id,
            new_digest_watch.clone(),
        ));

        Self {
            inner,
            input,
            output,
            tokio_runtime,
            datastore,
            public_key,
            private_key,
            account_id,
            enclave,
            last_merkle_tree,
            new_digest_watch,
        }
    }

    pub fn get_current_global_epoch(&self) -> EpochId {
        self.inner.get_current_global_epoch()
    }

    pub fn connect(&self, server_address: &str) {
        let addr_str = format!("{server_address}:8080");

        let mut output_lock = self.output.lock().unwrap();
        let mut output = &mut *output_lock;
        let &mut (_codec, write_stream) = &mut output;

        let mut read_stream = self.input.lock().unwrap();

        if read_stream.is_some() || write_stream.is_some() {
            panic!("Reached invalid state!");
        }

        let stream = TcpStream::connect(addr_str).expect("Connect to blockchain network");
        stream.set_nodelay(true).unwrap();

        *read_stream = Some(stream.try_clone().unwrap());
        *write_stream = Some(stream);

        log::info!("Data pod is connected to the blockchain network");
    }

    pub fn get_location(&self) -> AccountId {
        self.account_id
    }

    pub fn set_name(&self, name: String) {
        self.enclave.set_name(name);

        let transaction =
            Transaction::new_create_account(self.public_key.clone(), &self.private_key);

        self.send_transaction_inner(transaction);
    }

    pub fn run(&self) {
        let mut buffer = BytesMut::new();

        let mut codec = LengthDelimitedCodec::new();

        while self.enclave.is_okay() {
            let mut data = [0; 1024];
            let sock = self.input.lock().unwrap();

            let len = match sock.as_ref().unwrap().read(&mut data) {
                Ok(l) => l,
                Err(err) => {
                    log::error!("Failed to read message from socket: {err:?}");
                    exit(0);
                }
            };

            if len == 0 {
                log::info!("Lost connection to blockchain. Shutting down...");
                exit(0);
            }

            buffer.extend_from_slice(&data[0..len]);

            loop {
                match codec.decode(&mut buffer) {
                    Ok(Some(data)) => {
                        log::trace!("Got message from blockchain network");
                        let msg = bincode::deserialize(&data).unwrap();
                        let inner = self.inner.clone();

                        self.tokio_runtime.spawn(async move {
                            inner.handle_message(msg).await;
                        });
                    }
                    Ok(None) => {
                        // processed everything
                        break;
                    }
                    Err(err) => {
                        log::error!("Failed to decode message: {err:?}");
                        return;
                    }
                }
            }
        }
    }

    pub async fn wait_sync(&self) {
        self.new_digest_watch.counter.fetch_add(1, Ordering::SeqCst);

        let sem = self.new_digest_watch.notify.clone();
        Semaphore::acquire_owned(sem).await.unwrap().forget();
    }

    pub fn sync(&self) {
        let mut mtree = self.last_merkle_tree.lock().unwrap();

        if mtree.is_some() {
            log::debug!("Previous digest's state not synced yet. Will wait until next interval to create a new one.");
            return;
        }

        let htrees = if let Some(res) = self.datastore.update_hashes() {
            res
        } else {
            log::trace!("No pending transactions. Will not create a new digest.");
            return;
        };

        log::debug!("Flushed transactions and requesting new local epoch");

        let position = self.datastore.get_digest().num_batches() - 1;

        self.send_transaction(LedgerMessage::SyncState {
            position,
            state_hash: *htrees.state_tree.get_hash(),
            transaction_hash: *htrees.transaction_tree.get_hash(),
            reservation_hash: *htrees.reservation_tree.get_hash(),
        });

        *mtree = Some(htrees);
    }

    pub fn send_transaction(&self, message: LedgerMessage) {
        let transaction = Transaction::new(self.account_id, message, &self.private_key);

        self.send_transaction_inner(transaction);
    }

    #[inline]
    fn send_transaction_inner(&self, transaction: Transaction<LedgerMessage>) {
        let request = Message::TransactionRequest { transaction };
        let srequest = bincode::serialize(&request).unwrap();

        let mut output_lock = self.output.lock().unwrap();
        let mut output = &mut *output_lock;
        let &mut (codec, write_stream) = &mut output;

        let mut data = BytesMut::new();
        codec
            .encode(Bytes::from(srequest), &mut data)
            .expect("Failed to encode data");

        log::trace!("Sending new transaction request");
        let mut sock = write_stream.as_ref().unwrap();

        if let Err(err) = sock.write_all(&data) {
            log::error!("Failed to send data to blockchain network: {err}");
            exit(-1);
        }
    }
}
