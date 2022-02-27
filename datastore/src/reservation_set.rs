use serde::{Deserialize, Serialize};

use std::collections::HashSet;

use crate::objects::ObjectUid;
use crate::protocol::{Operation, TxId};
use crate::{Datastore, ShardId, ShardLockMap};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ReservationSet {
    reservations: Vec<(ObjectUid, Operation)>,
}

impl ReservationSet {
    pub fn insert(&mut self, uid: ObjectUid, op: Operation) {
        self.reservations.push((uid, op));
    }

    pub fn is_empty(&self) -> bool {
        self.reservations.is_empty()
    }

    pub fn abort(&mut self, data_pods_store: &Datastore, tx_id: TxId) {
        if self.is_empty() {
            return;
        }

        let mut shards = data_pods_store.get_shards(self.get_shards());

        for (uid, op) in self.reservations.drain(..) {
            ReservationSet::abort_operation(&mut shards, uid, op, tx_id);
        }
    }

    #[inline]
    fn abort_operation(shards: &mut ShardLockMap, uid: ObjectUid, op: Operation, tx_id: TxId) {
        let sid = Datastore::uid_to_shard(uid);
        let shard = &mut shards.get_mut(&sid).expect("Get shard");

        match op {
            Operation::Read { path, .. } => {
                shard.read_unlock(uid, &path, tx_id);
            }
            Operation::Create { .. } => {
                shard.write_unlock(uid, &[], tx_id);
            }
            Operation::Update { .. } => {
                shard.write_unlock(uid, &[], tx_id);
            }
            Operation::ListAppend { path, .. } => {
                shard.append_unlock(uid, &path, tx_id);
            }
            Operation::MapInsert { .. } => {
                todo!();
            }
        }
    }

    pub fn commit(&mut self, data_pods_store: &Datastore, tx_id: TxId) {
        if self.reservations.is_empty() {
            return;
        }

        let mut shards = data_pods_store.get_shards(self.get_shards());

        for (uid, op) in self.reservations.drain(..) {
            let sid = Datastore::uid_to_shard(uid);
            let shard = &mut shards.get_mut(&sid).expect("Get shard");

            match op {
                Operation::Read { path, .. } => {
                    shard.read_unlock(uid, &path, tx_id);
                }
                Operation::Create {
                    application,
                    owner,
                    typeid,
                    fields,
                } => {
                    shard
                        .create(uid, owner, &application, typeid, fields)
                        .expect("Create object");
                    shard.write_unlock(uid, &[], tx_id);
                }
                Operation::Update { arg } => {
                    shard.update(uid, arg).unwrap();
                    shard.write_unlock(uid, &[], tx_id);
                }
                Operation::ListAppend { path, arg } => {
                    shard.list_append(uid, &path, arg).unwrap();
                    shard.append_unlock(uid, &path, tx_id);
                }
                Operation::MapInsert { .. } => {
                    todo!();
                }
            }
        }
    }

    #[inline]
    pub fn has_writes(&self) -> bool {
        for (_, op) in self.reservations.iter() {
            match op {
                Operation::Read { .. } => {}
                _ => {
                    return true;
                }
            }
        }

        false
    }

    #[inline]
    fn get_shards(&self) -> HashSet<ShardId> {
        let mut result = HashSet::new();

        for (uid, _) in self.reservations.iter() {
            result.insert(Datastore::uid_to_shard(*uid));
        }

        result
    }
}
