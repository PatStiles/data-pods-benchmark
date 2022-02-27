use std::collections::{hash_map, HashMap, LinkedList};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;

use std::sync::Mutex as StdMutex;

use data_pods_utils::VecSet;

use data_pods_store::protocol::TxId;
use data_pods_store::{AppDefinition, Datastore, EpochHashes, MetaFnDefinition, TransactionInfo};

use blockchain_simulator::protocol::EpochId;

use blockchain_simulator::AccountId;

use crate::blockchain::BlockchainConnection;
use crate::proxy::ProxyConnection;
use crate::remote_parties::RemoteParties;
use crate::transactions::{FederatedTransaction, RemoteTransaction};

type TransactionBucket = HashMap<TxId, Arc<FederatedTransaction>>;

type TransactionStore = Vec<Arc<Mutex<TransactionBucket>>>;

const NUM_TX_BUCKETS: usize = 10;

pub struct Enclave {
    okay: AtomicBool,
    //TODO move transaction handling somewhere else
    transactions: Mutex<TransactionStore>,
    tx_remove_queue: Mutex<Vec<TxId>>,
    tx_commit_queue: Mutex<Vec<TxId>>,

    remote_transactions: Mutex<HashMap<TxId, Arc<RemoteTransaction>>>,
    remote_tx_remove_queue: Mutex<Vec<TxId>>,

    name: StdMutex<Option<String>>,
    proxies: StdMutex<LinkedList<Arc<ProxyConnection>>>,
}

impl Enclave {
    pub fn new() -> Self {
        let remote_transactions = Mutex::new(HashMap::new());
        let proxies = StdMutex::new(LinkedList::new());
        let name = StdMutex::new(None);
        let tx_commit_queue = Mutex::new(Vec::new());
        let tx_remove_queue = Mutex::new(Vec::new());
        let remote_tx_remove_queue = Mutex::new(Vec::new());

        let mut txs = Vec::new();
        for _ in 0..NUM_TX_BUCKETS {
            txs.push(Arc::new(Mutex::new(TransactionBucket::new())));
        }

        let transactions = Mutex::new(txs);

        Self {
            okay: AtomicBool::new(true),
            remote_transactions,
            transactions,
            name,
            proxies,
            tx_remove_queue,
            remote_tx_remove_queue,
            tx_commit_queue,
        }
    }

    pub fn is_okay(&self) -> bool {
        self.okay.load(Ordering::SeqCst)
    }

    pub fn set_name(&self, name: String) {
        let mut nlock = self.name.lock().unwrap();

        if nlock.is_some() {
            panic!("Name is already set");
        }

        *nlock = Some(name);
    }

    pub async fn notify_new_epoch_started(&self, id: EpochId) {
        // Hold locks so we do not admit new transactions
        // until current ones are processed
        let local_txs = self.transactions.lock().await;
        let mut remote_txs = self.remote_transactions.lock().await;
        //TODO
        /*        for tx in remote_txs.values() {
            tx.notify_new_epoch_started(id).await;
        }*/

        let mut handles = Vec::with_capacity(NUM_TX_BUCKETS);

        for bucket in local_txs.iter() {
            let bucket = bucket.clone();

            let hdl = tokio::spawn(async move {
                let bucket_lock = bucket.lock().await;

                for tx in bucket_lock.values() {
                    tx.notify_new_epoch_started(id).await;
                }
            });

            handles.push(hdl);
        }

        futures::future::join_all(handles).await;

        for id in self.tx_remove_queue.lock().await.drain(..) {
            let bid = (id as usize) % NUM_TX_BUCKETS;
            let mut bucket = local_txs[bid].lock().await;
            bucket.remove(&id).expect("No such local transaction");
        }

        for id in self.remote_tx_remove_queue.lock().await.drain(..) {
            remote_txs.remove(&id).unwrap();
        }
    }

    #[allow(dead_code)]
    pub fn get_name(&self) -> String {
        let nlock = self.name.lock().unwrap();

        match &*nlock {
            Some(s) => s.clone(),
            None => panic!("Name not set yet!"),
        }
    }

    // Get a proxy connection
    // Currently the first registered one...
    pub fn get_proxy_connection(&self) -> Arc<ProxyConnection> {
        let proxies = self.proxies.lock().unwrap();
        proxies.front().unwrap().clone()
    }

    pub async fn remove_remote_transaction(&self, tx_id: TxId) {
        let mut queue = self.remote_tx_remove_queue.lock().await;
        queue.push(tx_id);
    }

    pub async fn get_remote_transaction(&self, tx_id: TxId) -> Option<Arc<RemoteTransaction>> {
        self.remote_transactions.lock().await.get(&tx_id).cloned()
    }

    pub async fn get_or_create_remote_transaction(
        &self,
        tx_info: &TransactionInfo,
    ) -> Arc<RemoteTransaction> {
        let tx_id = tx_info.identifier;
        let mut map = self.remote_transactions.lock().await;

        match map.entry(tx_id) {
            hash_map::Entry::Occupied(o) => o.get().clone(),
            hash_map::Entry::Vacant(e) => {
                let tx = Arc::new(RemoteTransaction::new(tx_info.clone()));

                e.insert(tx.clone());
                tx
            }
        }
    }

    pub async fn notify_commit(&self, tx_id: TxId) {
        self.tx_commit_queue.lock().await.push(tx_id);
    }

    pub async fn flush(
        &self,
        hashes: Arc<EpochHashes>,
        filter: Arc<VecSet<TxId>>,
        set_size: usize,
    ) {
        let commits = {
            let mut clock = self.tx_commit_queue.lock().await;
            std::mem::take(&mut *clock)
        };

        let mut handles = Vec::with_capacity(NUM_TX_BUCKETS);
        let local_txs = self.transactions.lock().await;

        for (pos, bucket) in local_txs.iter().enumerate() {
            let mut commits_cpy = commits.clone();
            let filter_cpy = filter.clone();
            let hashes_cpy = hashes.clone();
            let bucket = bucket.clone();

            let hdl = tokio::spawn(async move {
                let bucket_lock = bucket.lock().await;
                let mut remainder = Vec::new();

                for tx_id in commits_cpy.drain(..) {
                    if (tx_id as usize) % NUM_TX_BUCKETS != pos {
                        continue;
                    }

                    match filter_cpy.get_position(&tx_id) {
                        Some(pos) => {
                            if let Some(tx) = bucket_lock.get(&tx_id) {
                                tx.flush(pos, &*hashes_cpy, set_size).await;
                            } else {
                                panic!("Failed to flush: not such transaction");
                            }
                        }
                        None => {
                            remainder.push(tx_id);
                        }
                    }
                }

                remainder
            });

            handles.push(hdl);
        }

        let mut remainders = futures::future::join_all(handles).await;
        let mut commits = self.tx_commit_queue.lock().await;

        for remainder in remainders.drain(..) {
            commits.append(&mut remainder.unwrap());
        }
    }

    //FIXME
    #[allow(clippy::too_many_arguments)]
    pub async fn create_transaction(
        &self,
        source_account: AccountId,
        app_name: String,
        fn_name: String,
        application: Arc<AppDefinition>,
        datastore: Arc<Datastore>,
        blockchain: Arc<BlockchainConnection>,
        enclave: Arc<Self>,
        remote_parties: Arc<RemoteParties>,
        meta_fn: Arc<MetaFnDefinition>,
    ) -> Arc<FederatedTransaction> {
        let tx_id = rand::random::<TxId>();

        let bid = (tx_id as usize) % NUM_TX_BUCKETS;
        let bucket = self.transactions.lock().await[bid].clone();

        let tx_info = TransactionInfo {
            identifier: tx_id,
            source_account,
            source_location: blockchain.get_location(),
            fn_name,
            application: app_name,
            start: blockchain.get_current_global_epoch(),
        };

        let tx = Arc::new(FederatedTransaction::new(
            tx_info,
            application,
            datastore,
            blockchain,
            enclave,
            remote_parties,
            meta_fn,
        ));

        bucket.lock().await.insert(tx_id, tx.clone());

        tx
    }

    pub async fn tx_barrier(&self) {
        let _ = self.transactions.lock().await;
    }

    pub async fn get_transaction(&self, tx_id: TxId) -> Option<Arc<FederatedTransaction>> {
        let bid = (tx_id as usize) % NUM_TX_BUCKETS;

        let bucket = self.transactions.lock().await[bid].clone();
        let block = bucket.lock().await;

        block.get(&tx_id).cloned()
    }

    pub async fn remove_transaction(&self, tx_id: TxId) {
        let mut queue = self.tx_remove_queue.lock().await;
        queue.push(tx_id);
    }

    pub fn register_proxy_connection(&self, proxy: Arc<ProxyConnection>) {
        let mut proxies = self.proxies.lock().unwrap();
        proxies.push_back(proxy);
    }
}
