use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use blockchain_simulator::AccountId;

use crate::merkle::MerkleHash;
use crate::protocol::{TransactionFlag, TxId};
use data_pods_utils::VecSet;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DigestEntry {
    tx_id: TxId,
    source: AccountId,
    flag: TransactionFlag,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DigestBatch {
    // Meta-Data
    timestamp: i64,
    transaction_hash: MerkleHash,
    state_hash: MerkleHash,
    reservation_hash: MerkleHash,

    // Content
    entries: Vec<DigestEntry>,
}

#[derive(Default, Clone, Debug)]
pub struct PendingDigestBatch {
    pub entries: Vec<DigestEntry>,
}

pub enum DigestAppendResult {
    Ok { over_threshold: bool },
    ReachedMax,
}

impl PendingDigestBatch {
    pub fn seal(
        self,
        transaction_hash: MerkleHash,
        state_hash: MerkleHash,
        reservation_hash: MerkleHash,
    ) -> DigestBatch {
        let now = chrono::offset::Utc::now();
        let timestamp = now.timestamp();

        DigestBatch {
            timestamp,
            transaction_hash,
            state_hash,
            reservation_hash,
            entries: self.entries,
        }
    }

    pub fn append_entry(
        &mut self,
        source: AccountId,
        tx_id: TxId,
        flag: TransactionFlag,
    ) -> DigestAppendResult {
        if self.is_full() {
            log::trace!("Reached maximum batch size!");
            DigestAppendResult::ReachedMax
        } else {
            let entry = DigestEntry {
                source,
                tx_id,
                flag,
            };
            self.entries.push(entry);

            DigestAppendResult::Ok {
                over_threshold: self.entries.len() >= SYNC_THRESHOLD,
            }
        }
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.entries.len() >= MAX_BATCH_LEN
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

const MAX_BATCH_LEN: usize = 10_000;
const SYNC_THRESHOLD: usize = 5_000;

impl DigestBatch {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn get_transaction_set(&self) -> VecSet<TxId> {
        let mut result = VecSet::default();

        for entry in self.entries.iter() {
            if !result.insert(entry.tx_id) {
                panic!("Transaction {} more than once in batch", entry.tx_id);
            }
        }

        result
    }
}

///Keeps track of all operations to a data pod
#[derive(Default)]
pub struct Digest {
    batches: RwLock<(usize, HashMap<usize, Arc<DigestBatch>>)>,
}

#[derive(Default)]
pub struct BatchStatistics {
    pub aborts: usize,
    pub commits: usize,
    pub reservations: usize,
}

#[derive(Default)]
pub struct DigestStatistics {
    pub aborted_txs: usize,
    pub committed_txs: usize,
    pub reservations: usize,
    pub total_ops: usize,

    pub batches: HashMap<usize, BatchStatistics>,
}

impl Digest {
    pub fn append_batch(&self, batch: DigestBatch) {
        let mut lock = self.batches.write().unwrap();
        let (last, batches) = &mut *lock;

        if batches.is_empty() {
            *last = 0;
        } else {
            *last += 1;
        }

        batches.insert(*last, Arc::new(batch));
    }

    pub fn insert_batch(&self, position: usize, batch: DigestBatch) {
        let mut lock = self.batches.write().unwrap();
        let (last, batches) = &mut *lock;

        if position > *last {
            *last = position;
        }

        let result = batches.insert(position, Arc::new(batch));

        if result.is_some() {
            panic!("Got batch with id {} more than once", position);
        }
    }

    pub fn clone_batches(&self) -> HashMap<usize, Arc<DigestBatch>> {
        let lock = self.batches.read().unwrap();

        lock.1.clone()
    }

    pub fn get_last_batch(&self) -> Arc<DigestBatch> {
        let lock = self.batches.read().unwrap();
        let (last, batches) = &*lock;

        if batches.is_empty() {
            panic!("Digest contains no batches");
        }

        batches.get(last).unwrap().clone()
    }

    pub fn num_batches(&self) -> usize {
        let lock = self.batches.read().unwrap();
        let (_, batches) = &*lock;

        batches.len()
    }

    pub fn has_batch(&self, position: usize) -> bool {
        let lock = self.batches.read().unwrap();
        let (_, batches) = &*lock;

        batches.contains_key(&position)
    }

    pub fn get_statistics(
        &self,
        location: AccountId,
        start: usize,
        end: usize,
    ) -> DigestStatistics {
        let mut stats = DigestStatistics::default();

        let lock = self.batches.read().unwrap();
        let (_, batches) = &*lock;

        for (pos, batch) in batches.iter() {
            if *pos > end || *pos < start {
                continue;
            }

            let mut bstats = BatchStatistics::default();

            for entry in batch.entries.iter() {
                // Don't double count commits
                // but count all aborts
                stats.total_ops += 1;

                let is_source = entry.source == location;

                match entry.flag {
                    TransactionFlag::Commit => {
                        if is_source {
                            stats.committed_txs += 1;
                        }

                        bstats.commits += 1;
                    }
                    TransactionFlag::Abort => {
                        if is_source {
                            stats.aborted_txs += 1;
                        }

                        bstats.aborts += 1;
                    }
                    TransactionFlag::Reserve => {
                        stats.reservations += 1;
                        bstats.reservations += 1;
                    }
                }
            }

            stats.batches.insert(*pos, bstats);
        }

        stats
    }

    pub fn count_aborts(&self, location: AccountId, start: usize, end: usize) -> usize {
        let mut result = 0;
        let lock = self.batches.read().unwrap();
        let (_, batches) = &*lock;

        for (pos, batch) in batches.iter() {
            if *pos > end || *pos < start {
                continue;
            }

            for entry in batch.entries.iter() {
                if entry.source == location && entry.flag == TransactionFlag::Abort {
                    result += 1;
                }
            }
        }

        result
    }

    pub fn has_gaps(&self) -> bool {
        let lock = self.batches.read().unwrap();
        let (last, batches) = &*lock;

        !batches.is_empty() && *last != batches.len() - 1
    }
}

#[cfg(test)]
mod test {
    use sha2::Digest as HashDigest;
    use sha2::Sha512;

    use super::*;

    #[test]
    fn has_gaps() {
        let hash = Sha512::new().finalize();

        let digest = Digest::default();

        assert_eq!(false, digest.has_gaps());

        let b = PendingDigestBatch::default().seal(hash, hash, hash);
        digest.append_batch(b);

        assert_eq!(false, digest.has_gaps());

        let b = PendingDigestBatch::default().seal(hash, hash, hash);
        digest.insert_batch(2, b);

        assert_eq!(true, digest.has_gaps());

        let b = PendingDigestBatch::default().seal(hash, hash, hash);
        digest.insert_batch(1, b);

        assert_eq!(false, digest.has_gaps());
    }

    #[test]
    fn count_commits() {
        let hash = Sha512::new().finalize();

        let digest = Digest::default();
        let loc1 = 514;
        let loc2 = 13151;

        let b = PendingDigestBatch::default().seal(hash, hash, hash);
        digest.append_batch(b);

        let stats1 = digest.get_statistics(loc1, 0, 1);
        let stats2 = digest.get_statistics(loc2, 0, 1);

        assert_eq!(0, stats1.committed_txs);
        assert_eq!(0, stats2.committed_txs);

        let mut batch2 = PendingDigestBatch::default();
        let _ = batch2.append_entry(loc1, 5, TransactionFlag::Reserve);
        let _ = batch2.append_entry(loc2, 6, TransactionFlag::Commit);
        let _ = batch2.append_entry(loc2, 7, TransactionFlag::Commit);

        let b = batch2.seal(hash, hash, hash);
        digest.append_batch(b);

        let stats3 = digest.get_statistics(loc1, 0, 2);
        let stats4 = digest.get_statistics(loc2, 0, 2);

        assert_eq!(1, stats3.reservations);
        assert_eq!(0, stats3.committed_txs);
        assert_eq!(2, stats4.committed_txs);

        let mut batch3 = PendingDigestBatch::default();
        let _ = batch3.append_entry(loc1, 5, TransactionFlag::Commit);

        let b = batch3.seal(hash, hash, hash);
        digest.append_batch(b);

        let stats5 = digest.get_statistics(loc1, 0, 3);
        let stats6 = digest.get_statistics(loc2, 0, 3);

        assert_eq!(1, stats5.committed_txs);
        assert_eq!(2, stats6.committed_txs);
    }
}
