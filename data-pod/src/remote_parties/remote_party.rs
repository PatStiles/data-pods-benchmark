use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use bytes::BytesMut;
use serde_bytes::ByteBuf;

#[cfg(feature = "use-tls")]
use std::io::{Error, Write};

use tokio::sync::mpsc;

use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{Decoder, Encoder};

#[cfg(feature = "use-tls")]
use secure_channel::ServerInput as SecureChannelIn;
#[cfg(feature = "use-tls")]
use secure_channel::ServerOutput as SecureChannelOut;

use data_pods_store::protocol::{
    Message, OpId, TransactionError, TransactionFlag, TxId, TxResult, TxResultValues,
};
use data_pods_store::values::Value;
use data_pods_store::{
    Datastore, DigestAppendResult, DigestBatch, EpochHashes, MetaOperation, TransactionInfo,
};

use crate::requests::TxRequestHandle;

use data_pods_utils::enclave_protocol::EnclaveMessage;
use data_pods_utils::VecSet;

use tokio::sync::{oneshot, Mutex};

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::applications::ApplicationRegistry;
use crate::enclave::Enclave;
use crate::proxy::ProxyOutput;
use crate::remote_parties::RemoteParties;
use crate::transactions::{FederatedTransaction, TransactionChunk};
use crate::BlockchainConnection;

use tokio::runtime::Runtime as TokioRuntime;

use blockchain_simulator::AccountId;

#[derive(Debug)]
pub enum PendingMessage {
    TxResponse {
        op_id: OpId,
        tx_id: TxId,
        results: Result<TxResultValues, TransactionError>,
    },
}

/// Used internally by SecureChannelOut
#[cfg(feature = "use-tls")]
struct RemotePartySender {
    remote_party_id: u32,
    output: Arc<ProxyOutput>,
}

pub struct RemotePartyHandler {
    identifier: u32,
    datastore: Arc<Datastore>,
    applications: Arc<ApplicationRegistry>,
    remote_parties: Arc<RemoteParties>,
    blockchain: Arc<BlockchainConnection>,
    enclave: Arc<Enclave>,
    location: StdMutex<Option<AccountId>>,
    message_sender: mpsc::UnboundedSender<Message>,

    // queued responses (for incoming requests)
    responses: Arc<Mutex<HashMap<TxId, PendingMessage>>>,

    tx_requests: Arc<Mutex<HashMap<OpId, oneshot::Sender<TxResult>>>>,

    #[cfg(feature = "use-tls")]
    output: Mutex<(SecureChannelOut, LengthDelimitedCodec)>,
    #[cfg(not(feature = "use-tls"))]
    output: Mutex<(Arc<ProxyOutput>, LengthDelimitedCodec)>,
}

impl RemotePartyHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identifier: u32,
        datastore: Arc<Datastore>,
        applications: Arc<ApplicationRegistry>,
        remote_parties: Arc<RemoteParties>,
        blockchain: Arc<BlockchainConnection>,
        enclave: Arc<Enclave>,
        tx_requests: Arc<Mutex<HashMap<OpId, oneshot::Sender<TxResult>>>>,
        proxy_output: Arc<ProxyOutput>,
        tokio_runtime: &TokioRuntime,
    ) -> Arc<Self> {
        cfg_if::cfg_if! {
            if #[ cfg(feature="use-tls") ] {
                let sender = RemotePartySender{
                    remote_party_id: identifier, output: proxy_output
                };
                let output = Mutex::new((
                    SecureChannelOut::new(Box::new(sender)), LengthDelimitedCodec::new()
                ));
            } else {
                let output = Mutex::new((proxy_output, LengthDelimitedCodec::new()));
            }
        }

        let responses = Arc::new(Mutex::new(HashMap::new()));
        let location = StdMutex::new(None);

        let (message_sender, mut message_receiver) = mpsc::unbounded_channel();

        let obj = Arc::new(Self {
            identifier,
            datastore,
            applications,
            remote_parties,
            blockchain,
            enclave,
            tx_requests,
            output,
            location,
            responses,
            message_sender,
        });

        {
            let obj = obj.clone();

            // Ensures we only handle one message per connection at once
            // (transactions move to background tasks)
            tokio_runtime.spawn(async move {
                while let Some(msg) = message_receiver.recv().await {
                    obj.handle_message_inner(msg).await;
                }
            });
        }

        obj
    }

    /// Returns the location of this party if it has been set yet
    pub fn try_get_location(&self) -> Option<AccountId> {
        *self.location.lock().unwrap()
    }

    #[inline]
    fn handle_message(&self, message: Message) {
        let _ = self.message_sender.send(message);
    }

    async fn handle_message_inner(&self, message: Message) {
        match message {
            Message::SendDigestBatch { .. } => {
                //TODO
            }
            Message::SendLocation { location } => {
                //FIXME move initial handshake for TLS and name exchange somewhere else
                //FIXME verify signature with what is in the identity register
                log::info!(
                    "Remote party's #{} location is '{location}'",
                    self.identifier
                );

                {
                    let mut loc = self.location.lock().unwrap();

                    if loc.is_some() {
                        panic!("Got SendLocation more than once");
                    } else {
                        *loc = Some(location);
                    }
                }

                self.remote_parties.set_location(self.identifier, location);

                self.send(Message::ConnectionEstablished {}).await;
            }
            Message::GetRequest { op_id, obj_id } => {
                let result = self.datastore.get(obj_id.get_uid());

                let response = match result {
                    Ok(obj) => Message::GetResponse {
                        op_id,
                        result: Ok(obj),
                    },
                    Err(e) => Message::GetResponse {
                        op_id,
                        result: Err(e),
                    },
                };

                // Send immediately because no state is changed
                // TODO make sure we send data that is "stable"
                self.send(response).await;
            }
            Message::CallRequest {
                op_id,
                application,
                key,
                args,
            } => {
                //  Check if transactions are busy and process
                //  no new calls while they are
                //  TODO is this still needed?
                self.enclave.tx_barrier().await;

                if self.datastore.is_digest_full() {
                    log::trace!("Discarded call request due to congestion");
                } else {
                    let apps = self.applications.clone();
                    let location = self.try_get_location().unwrap();
                    let blockchain = self.blockchain.clone();

                    // Move transaction to background task
                    tokio::spawn(async move {
                        apps.call(op_id, location, &application, key, blockchain, args)
                            .await;
                    });
                }
            }
            Message::TxLockRequest {
                op_id,
                tx_info,
                stage,
                calls,
                meta_vars,
            } => {
                let transaction = self
                    .enclave
                    .get_or_create_remote_transaction(&tx_info)
                    .await;

                let application = self.applications.get_app_definition(&tx_info.application);

                let datastore = self.datastore.clone();
                let blockchain = self.blockchain.clone();
                let responses = self.responses.clone();

                tokio::spawn(async move {
                    let tx_id = tx_info.identifier;
                    let meta_fn_name = &tx_info.fn_name;
                    let app_name = &tx_info.application;

                    let mut err_result = None;
                    let mut ok_results = Vec::new();

                    for call in calls {
                        let ralias;
                        let fn_name;

                        let args = if let Some(f) = application.functions.get(meta_fn_name) {
                            if stage != 0 {
                                panic!("Invalid stage!");
                            }

                            let mut flat_args = Vec::new();

                            for (name, _) in f.0.iter() {
                                flat_args.push(name.clone());
                            }

                            ralias = Some("result".to_string());
                            fn_name = meta_fn_name;

                            flat_args
                        } else if let Some(f) = application.meta_fns.get(meta_fn_name) {
                            let op = &f.get_stage(stage)[call];

                            if let MetaOperation::Call {
                                args,
                                alias,
                                fn_name: _fn_name,
                            } = op
                            {
                                fn_name = _fn_name;
                                ralias = alias.clone();
                                args.clone()
                            } else {
                                panic!("Invalid state!");
                            }
                        } else {
                            panic!(
                                "Got invalid transaction: no such function '{}::{}'",
                                app_name, meta_fn_name
                            );
                        };

                        let (fn_args, func, _) = application.functions.get(fn_name).unwrap();
                        let mut arg_map = HashMap::new();

                        for (pos, arg_name) in args.iter().enumerate() {
                            if let Some(val) = meta_vars.get(arg_name) {
                                // Change the name to that in function signature
                                arg_map.insert(fn_args[pos].0.clone(), val.clone());
                            } else {
                                panic!("No such meta variable '{arg_name}'");
                            }
                        }

                        let tx_chunk = Arc::new(TransactionChunk::new(
                            tx_info.identifier,
                            tx_info.source_account,
                            tx_info.source_location,
                            datastore.clone(),
                            transaction.get_reservations(),
                        ));

                        let rval = FederatedTransaction::call_sub_transaction(
                            tx_info.clone(),
                            application.clone(),
                            func.clone(),
                            arg_map,
                            datastore.clone(),
                            tx_chunk,
                        );

                        match rval {
                            Ok(val) => {
                                let res = if let Some(rname) = ralias {
                                    (rname.clone(), val)
                                } else {
                                    ("".to_string(), Value::None)
                                };

                                ok_results.push(res);
                            }
                            Err(e) => {
                                err_result = Some(Err(e));
                            }
                        }
                    }

                    let results = if let Some(err) = err_result {
                        err
                    } else {
                        Ok(ok_results)
                    };

                    let msg = PendingMessage::TxResponse {
                        op_id,
                        tx_id,
                        results,
                    };
                    responses.lock().await.insert(tx_id, msg);

                    let flag = TransactionFlag::Reserve;

                    loop {
                        match datastore.record_transaction(tx_info.source_location, tx_id, flag) {
                            DigestAppendResult::Ok { over_threshold } => {
                                if over_threshold {
                                    blockchain.sync();
                                }
                                break;
                            }
                            DigestAppendResult::ReachedMax => {
                                blockchain.wait_sync().await;
                            }
                        }
                    }
                });
            }
            Message::TxFinalizeRequest {
                op_id,
                tx_id,
                abort,
            } => {
                let datastore = self.datastore.clone();
                let blockchain = self.blockchain.clone();
                let responses = self.responses.clone();
                let enclave = self.enclave.clone();
                let source_location = self.try_get_location().unwrap();

                tokio::spawn(async move {
                    let state_change;

                    let results = match enclave.get_remote_transaction(tx_id).await {
                        Some(tx) => {
                            state_change = true;

                            let is_done = if abort {
                                tx.abort(&*datastore)
                            } else {
                                tx.commit(&*datastore)
                            };

                            if is_done {
                                enclave.remove_remote_transaction(tx_id).await;
                            }

                            Ok(vec![])
                        }
                        None => {
                            // If it is an abort we do not need to have the transaction
                            // (the lock might have failed on this node)
                            if abort {
                                state_change = true;
                                Ok(vec![])
                            } else {
                                log::error!("Got commit for non-existing transaction {tx_id}");
                                state_change = false;
                                Err(TransactionError::NoSuchTx)
                            }
                        }
                    };

                    let msg = PendingMessage::TxResponse {
                        op_id,
                        tx_id,
                        results,
                    };
                    if state_change {
                        {}

                        let flag = if abort {
                            TransactionFlag::Abort
                        } else {
                            TransactionFlag::Commit
                        };

                        loop {
                            match datastore.record_transaction(source_location, tx_id, flag) {
                                DigestAppendResult::Ok { over_threshold } => {
                                    if over_threshold {
                                        blockchain.sync();
                                    }
                                    break;
                                }
                                DigestAppendResult::ReachedMax => {
                                    blockchain.wait_sync().await;
                                }
                            }
                        }
                    }
                    /* fixme else {
                        self.send(msg).await;
                    }*/

                    responses.lock().await.insert(tx_id, msg);
                });
            }
            Message::TxResponse { op_id, result, .. } => {
                let mut requests = self.tx_requests.lock().await;

                match requests.remove(&op_id) {
                    Some(sender) => {
                        if let Err(err) = sender.send(result) {
                            log::trace!("Failed to notify about TxResponse: {err:?}");
                        }
                    }
                    None => panic!("Got unexpected response: No such tx request #{op_id}"),
                }
            }
            Message::PeerConnect { address } => {
                //FIXME access control here
                let msg = EnclaveMessage::RequestConnect { address };
                let conn = self.enclave.get_proxy_connection();

                conn.send(msg);
            }
            Message::ConnectionEstablished {} => {
                //TODO
            }
            _ => panic!("Got unexpected message!"),
        }
    }

    #[inline]
    pub async fn send(&self, msg: Message) {
        let data = bincode::serialize(&msg).expect("Serialize message failed");

        let mut lock = self.output.lock().await;
        let mut tuple = &mut *lock;
        let &mut (sock, codec) = &mut tuple;
        let mut buffer = BytesMut::new();

        if data.len() > codec.max_frame_length() {
            panic!(
                "Cannot send message to remote party: too long (length={})!",
                data.len()
            );
        }

        codec
            .encode(data.into(), &mut buffer)
            .expect("Encoding response to remote party failed");

        cfg_if::cfg_if! {
            if #[ cfg(feature="use-tls") ] {
                sock.write_all(&buffer[..]).expect("Sending message to remote party failed");
            } else {
                let mut payload = ByteBuf::new();
                payload.extend_from_slice(&buffer[..]);

                let msg = EnclaveMessage::ForwardResponse{ rp_id: self.identifier, payload };
                sock.send(msg);
            }
        }
    }

    pub async fn has_transaction_responses(&self) -> bool {
        let responses = self.responses.lock().await;
        !responses.is_empty()
    }

    pub async fn flush(
        rp: &Arc<RemotePartyHandler>,
        hashes: &EpochHashes,
        position: usize,
        batch: Arc<DigestBatch>,
        filter: &VecSet<TxId>,
        set_size: usize,
    ) -> usize {
        let mut responses = rp.responses.lock().await;
        let mut to_process = Vec::new();
        let mut remainder = HashMap::new();

        //FIXME don't deep copy here
        let batch_msg = Message::SendDigestBatch {
            position,
            batch: (&*batch).clone(),
        };

        for (tx_id, msg) in responses.drain() {
            match filter.get_position(&tx_id) {
                Some(pos) => {
                    log::trace!("Sending response to client");

                    match msg {
                        PendingMessage::TxResponse {
                            op_id,
                            tx_id,
                            results,
                        } => {
                            let result = match results {
                                Ok(results) => {
                                    let proof =
                                        hashes.transaction_tree.generate_proof(pos, 2, set_size);

                                    Ok((results, proof))
                                }
                                Err(e) => Err(e),
                            };

                            let sealed = Message::TxResponse {
                                op_id,
                                tx_id,
                                result,
                            };
                            to_process.push(sealed);
                        }
                    }
                }
                None => {
                    remainder.insert(tx_id, msg);
                }
            }
        }

        let count = to_process.len();

        mem::swap(&mut *responses, &mut remainder);
        let rp2 = rp.clone();

        tokio::spawn(async move {
            rp2.send(batch_msg).await;

            for msg in to_process.drain(..) {
                rp2.send(msg).await;
            }
        });

        count
    }
}

/// A "remote party" is either a client or another data pod
pub struct RemoteParty {
    tokio_runtime: Arc<TokioRuntime>,
    connected: AtomicBool,
    inner: Arc<RemotePartyHandler>,
    datastore: Arc<Datastore>,

    #[cfg(feature = "use-tls")]
    input: StdMutex<(SecureChannelIn, LengthDelimitedCodec, BytesMut)>,
    #[cfg(not(feature = "use-tls"))]
    input: StdMutex<(LengthDelimitedCodec, BytesMut)>,

    // pending (outgoing) requests
    tx_requests: Arc<Mutex<HashMap<OpId, oneshot::Sender<TxResult>>>>,

    next_op_id: AtomicU32,
}

impl RemoteParty {
    //TODO clean this up eventually...
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identifier: u32,
        applications: Arc<ApplicationRegistry>,
        datastore: Arc<Datastore>,
        blockchain: Arc<BlockchainConnection>,
        remote_parties: Arc<RemoteParties>,
        proxy_output: Arc<ProxyOutput>,
        enclave: Arc<Enclave>,
        tokio_runtime: Arc<TokioRuntime>,
    ) -> Self {
        #[cfg(feature = "use-tls")]
        let input = StdMutex::new((
            SecureChannelIn::default(),
            LengthDelimitedCodec::new(),
            BytesMut::new(),
        ));

        #[cfg(not(feature = "use-tls"))]
        let input = StdMutex::new((LengthDelimitedCodec::new(), BytesMut::new()));

        let tx_requests = Arc::new(Mutex::new(HashMap::new()));
        let next_op_id = AtomicU32::new(1);

        let connected = AtomicBool::new(true);

        let inner = RemotePartyHandler::new(
            identifier,
            datastore.clone(),
            applications,
            remote_parties,
            blockchain,
            enclave,
            tx_requests.clone(),
            proxy_output,
            &*tokio_runtime,
        );

        RemoteParty {
            connected,
            input,
            tx_requests,
            next_op_id,
            tokio_runtime,
            inner,
            datastore,
        }
    }

    pub fn notify_disconnect(&self) {
        self.connected.store(false, Ordering::SeqCst);
    }

    pub fn initialize(&self, location: AccountId) {
        // Send digests first, then location to indicate "ready"
        let mut digest = self.datastore.get_digest().clone_batches();

        if digest.is_empty() {
            log::debug!("No digests (yet). Sending nothing to remote party.");
        } else {
            log::info!("Sending {} digest batches to remote party.", digest.len());

            for (position, batch) in digest.drain() {
                //FIXME don't deep copy here
                self.send(Message::SendDigestBatch {
                    position,
                    batch: (&*batch).clone(),
                });
            }
        }

        log::debug!("Sending location to remote party");
        self.send(Message::SendLocation { location });
    }

    /// Returns the location of this party if it has been set yet
    pub fn try_get_location(&self) -> Option<AccountId> {
        self.inner.try_get_location()
    }

    pub async fn has_transaction_responses(&self) -> bool {
        self.inner.has_transaction_responses().await
    }

    pub async fn flush(
        &self,
        merkle_tree: &EpochHashes,
        position: usize,
        batch: Arc<DigestBatch>,
        filter: &VecSet<TxId>,
        set_size: usize,
    ) -> usize {
        RemotePartyHandler::flush(&self.inner, merkle_tree, position, batch, filter, set_size).await
    }

    pub async fn lock_transaction(
        &self,
        tx_info: TransactionInfo,
        stage: u32,
        calls: Vec<usize>,
        meta_vars: HashMap<String, Value>,
    ) -> TxRequestHandle {
        let op_id = self.next_op_id.fetch_add(1, Ordering::SeqCst);

        let request = Message::TxLockRequest {
            op_id,
            tx_info,
            stage,
            calls,
            meta_vars,
        };
        let (handle, sender) = TxRequestHandle::new();

        self.tx_requests.lock().await.insert(op_id, sender);
        self.send(request);

        handle
    }

    pub fn send(&self, msg: Message) {
        if !self.connected.load(Ordering::SeqCst) {
            // Already disconnected
            return;
        }

        let inner = self.inner.clone();

        self.tokio_runtime.spawn(async move {
            inner.send(msg).await;
        });
    }

    pub async fn finalize_transaction(&self, tx_id: TxId, abort: bool) -> TxRequestHandle {
        let op_id = self.next_op_id.fetch_add(1, Ordering::SeqCst);

        let request = Message::TxFinalizeRequest {
            op_id,
            tx_id,
            abort,
        };
        let (handle, sender) = TxRequestHandle::new();

        self.tx_requests.lock().await.insert(op_id, sender);
        self.send(request);

        handle
    }

    pub fn handle_messages(&self, payload: &[u8]) {
        #[cfg(not(feature = "use-tls"))]
        {
            let mut lock = self.input.lock().unwrap();
            let mut tuple = &mut *lock;
            let &mut (codec, buffer) = &mut tuple;

            // make sure data gets pushed in order
            buffer.extend_from_slice(payload);

            loop {
                match codec.decode(buffer) {
                    Ok(Some(payload)) => {
                        let msg = bincode::deserialize(&payload).expect("Parse message");
                        self.inner.handle_message(msg);
                    }
                    Ok(None) => {
                        // processed everything
                        break;
                    }
                    Err(e) => {
                        log::error!("Decode error: {}", e);
                    }
                }
            }
        }

        #[cfg(feature = "use-tls")]
        {
            let mut lock = self.input.lock().unwrap();
            let mut tuple = &mut *lock;
            let &mut (sock, codec, buffer) = &mut tuple;

            sock.push_data(payload);

            while let Some(result) = sock.get_message() {
                match result {
                    Ok(data) => {
                        buffer.extend_from_slice(&data);
                    }
                    Err(e) => {
                        log::error!("Failed to decrypt message: {}", e);
                        break;
                    }
                }

                // Data may contain multiple messages
                loop {
                    match codec.decode(buffer) {
                        Ok(Some(payload)) => {
                            let msg = bincode::deserialize(&payload).expect("Parse message");
                            self.inner.handle_message(msg);
                        }
                        Ok(None) => {
                            // processed everything
                            break;
                        }
                        Err(e) => {
                            log::error!("Decode error: {}", e);
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(feature = "use-tls")]
impl Write for RemotePartySender {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        //FIXME avoid copy here
        let mut payload = Vec::new();
        payload.extend_from_slice(data);

        let msg = EnclaveMessage::ForwardResponse {
            rp_id: self.remote_party_id,
            payload: ByteBuf::from(payload),
        };

        // this is async, so failures do no propagate
        self.output.send(msg);
        Ok(data.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
