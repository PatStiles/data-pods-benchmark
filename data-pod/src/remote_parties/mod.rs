use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use data_pods_store::protocol::TxId;
use data_pods_store::{DigestBatch, EpochHashes};
use data_pods_utils::VecSet;

use blockchain_simulator::AccountId;

mod remote_party;
pub use remote_party::{PendingMessage, RemoteParty};

pub struct RemoteParties {
    id_mapping: RwLock<HashMap<u32, Arc<RemoteParty>>>,
    location_mapping: RwLock<HashMap<AccountId, Arc<RemoteParty>>>,
}

impl RemoteParties {
    pub fn new() -> Self {
        Self {
            id_mapping: RwLock::new(HashMap::new()),
            location_mapping: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, identifier: u32, remote_party: Arc<RemoteParty>) {
        let mut mapping = self.id_mapping.write().unwrap();
        let result = mapping.insert(identifier, remote_party);

        if result.is_some() {
            panic!("Connect failed: remote party with id={identifier} already existed!");
        } else {
            log::debug!("Set up new remote party with id={identifier}");
        }
    }

    pub async fn flush(
        &self,
        hashes: &Arc<EpochHashes>,
        position: usize,
        batch: &Arc<DigestBatch>,
        transactions: &Arc<VecSet<TxId>>,
        set_size: usize,
    ) -> usize {
        let rps = self.id_mapping.read().unwrap().clone();
        let mut count = 0;
        let mut hdls = Vec::new();

        for (_id, remote_party) in rps.iter() {
            if remote_party.has_transaction_responses().await {
                let rp = remote_party.clone();
                let batch = batch.clone();
                let hashes_cpy = hashes.clone();
                let txs = transactions.clone();

                let hdl = tokio::spawn(async move {
                    RemoteParty::flush(&*rp, &*hashes_cpy, position, batch, &*txs, set_size).await
                });

                hdls.push(hdl);
            } else {
                count += RemoteParty::flush(
                    remote_party,
                    hashes,
                    position,
                    batch.clone(),
                    transactions,
                    set_size,
                )
                .await;
            }
        }

        for num in futures::future::join_all(hdls).await {
            count += num.unwrap();
        }

        count
    }

    pub fn unregister(&self, identifier: u32) {
        let result = {
            let mut mapping = self.id_mapping.write().unwrap();
            mapping.remove(&identifier)
        };

        let rp = match result {
            Some(rp) => rp,
            None => panic!("Disconnect failed: no such remote party #{identifier}"),
        };

        if let Some(loc) = rp.try_get_location() {
            let mut location_mapping = self.location_mapping.write().unwrap();
            location_mapping.remove(&loc);
        }

        rp.notify_disconnect();
    }

    pub fn set_location(&self, identifier: u32, location: AccountId) {
        let rp = self.get_by_id(identifier);

        let mut mapping = self.location_mapping.write().unwrap();
        let result = mapping.insert(location, rp);

        if result.is_some() {
            panic!("Connect failed: remote party with location {location} already existed!");
        }
    }

    pub fn get_by_location(&self, location: AccountId) -> Option<Arc<RemoteParty>> {
        let mapping = self.location_mapping.read().unwrap();
        mapping.get(&location).cloned()
    }

    pub fn get_by_id(&self, identifier: u32) -> Arc<RemoteParty> {
        let mapping = self.id_mapping.read().unwrap();
        match mapping.get(&identifier) {
            Some(remote_party) => remote_party.clone(),
            None => panic!("No such remote party #{identifier}"),
        }
    }
}
