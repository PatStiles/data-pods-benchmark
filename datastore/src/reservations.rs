use serde::Serialize;
use sha2::{Digest, Sha512};
use std::collections::{btree_map, BTreeMap};

use data_pods_utils::VecMultiSet;

use crate::merkle::FrozenMerkleTree;
use crate::objects::{field_name_to_uid, FieldUid, ObjectUid};
use crate::protocol::TxId;

#[derive(Serialize)]
pub enum Reservation {
    // a node in the lock tree that does not contain an actual lock but just references to children
    EmptyNode {
        children: BTreeMap<FieldUid, Reservation>,
    },
    // An exclusive write lock
    WriteLock {
        owner: TxId,
    },
    // read lock that allows for concurrent accesses to the same data
    ReadLock {
        owners: VecMultiSet<TxId>,
    },
    // special write lock to allow multiple concurrent appends
    AppendLock {
        owners: VecMultiSet<TxId>,
    },
}

impl Reservation {
    pub fn make_empty_node() -> Reservation {
        Reservation::EmptyNode {
            children: BTreeMap::new(),
        }
    }

    pub fn get_hash_tree(&self) -> FrozenMerkleTree {
        if let Reservation::EmptyNode {
            children: rchildren,
        } = &*self
        {
            let mut children = Vec::new();
            let mut hasher = Sha512::new();

            for reservation in rchildren.values() {
                let child = reservation.get_hash_tree();
                hasher.update(child.get_hash());

                children.push(child);
            }

            let hash = hasher.finalize();
            FrozenMerkleTree::Node { hash, children }
        } else {
            let mut hasher = Sha512::new();
            let bytes = bincode::serialize(&self).unwrap();
            hasher.update(&bytes);
            let hash = hasher.finalize();

            FrozenMerkleTree::Leaf { hash }
        }
    }

    pub fn traverse_path<T: AsRef<str>>(&mut self, path: &[T]) -> Option<&mut Reservation> {
        match path.split_first() {
            Some((first, subpath)) => {
                let uid = field_name_to_uid(first.as_ref());

                match self.get_child(uid) {
                    Some(child) => child.traverse_path(subpath),
                    None => None,
                }
            }
            None => Some(self),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Reservation::EmptyNode { children } => children.is_empty(),
            _ => false,
        }
    }

    pub fn num_children(&self) -> usize {
        match self {
            Reservation::EmptyNode { children } => children.len(),
            _ => 0,
        }
    }

    pub fn get_child(&mut self, key: ObjectUid) -> Option<&mut Reservation> {
        match &mut *self {
            Reservation::EmptyNode { children } => Some(
                children
                    .entry(key)
                    .or_insert_with(Reservation::make_empty_node),
            ),
            _ => None,
        }
    }

    pub fn make_write_lock(&mut self, key: ObjectUid, owner: TxId) -> bool {
        match &mut *self {
            Reservation::EmptyNode { children } => match children.entry(key) {
                btree_map::Entry::Occupied(_) => false,
                btree_map::Entry::Vacant(e) => {
                    e.insert(Reservation::WriteLock { owner });
                    true
                }
            },
            _ => false,
        }
    }

    pub fn remove_write_lock(&mut self, key: ObjectUid, owner: TxId) {
        match &mut *self {
            Reservation::EmptyNode { children } => {
                let res = children.remove(&key);

                match res {
                    None => {
                        panic!("Lock didn't exists");
                    }
                    Some(Reservation::WriteLock { owner: rowner }) => {
                        if owner != rowner {
                            panic!("Lock ownership does not match!");
                        }
                    }
                    Some(_) => {
                        panic!("Not a write lock");
                    }
                }
            }
            _ => {
                panic!("invalid state");
            }
        }
    }

    pub fn make_read_lock(&mut self, key: FieldUid, owner: TxId) -> bool {
        match &mut *self {
            Reservation::EmptyNode { children } => {
                let result = children.get_mut(&key);

                match result {
                    Some(Reservation::ReadLock { owners }) => {
                        owners.insert(owner);
                        true
                    }
                    None => {
                        let mut owners = VecMultiSet::default();
                        owners.insert(owner);

                        children.insert(key, Reservation::ReadLock { owners });
                        true
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    pub fn make_append_lock(&mut self, key: FieldUid, owner: TxId) -> bool {
        match &mut *self {
            Reservation::EmptyNode { children } => match children.get_mut(&key) {
                Some(Reservation::AppendLock { owners }) => {
                    owners.insert(owner);
                    true
                }
                None => {
                    let mut owners = VecMultiSet::default();
                    owners.insert(owner);

                    children.insert(key, Reservation::AppendLock { owners });
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }

    pub fn remove_read_lock(&mut self, key: FieldUid, owner: TxId) {
        match &mut *self {
            Reservation::EmptyNode { children } => {
                let remove = match children.get_mut(&key) {
                    Some(Reservation::ReadLock { owners }) => {
                        if !owners.remove(&owner) {
                            panic!("Lock was not owned by transaction!");
                        }

                        owners.is_empty()
                    }
                    _ => {
                        panic!("No such read lock!");
                    }
                };

                if remove {
                    children.remove(&key).unwrap();
                }
            }
            _ => {
                panic!("Invalid state");
            }
        }
    }

    pub fn remove_child(&mut self, key: FieldUid) {
        match &mut *self {
            Reservation::EmptyNode { children } => {
                children.remove(&key).unwrap();
            }
            _ => {
                panic!("Invalid state: no such child-lock");
            }
        }
    }

    pub fn remove_append_lock(&mut self, key: FieldUid, owner: TxId) {
        match &mut *self {
            Reservation::EmptyNode { children } => {
                let remove = match children.get_mut(&key) {
                    Some(Reservation::AppendLock { owners }) => {
                        if !owners.remove(&owner) {
                            panic!("Lock was not owned by transaction!");
                        }

                        owners.is_empty()
                    }
                    _ => {
                        panic!("No such append lock!");
                    }
                };

                if remove {
                    children.remove(&key).unwrap();
                }
            }
            _ => {
                panic!("Invalid state");
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn append_lock() {
        let mut root = Reservation::make_empty_node();

        let key = 42 as u64;
        let tx1 = 5145143;
        let tx2 = 3143151;

        assert_eq!(true, root.make_append_lock(key, tx1));
        assert_eq!(true, root.make_append_lock(key, tx1));

        assert_eq!(false, root.make_write_lock(key, tx2));

        root.remove_append_lock(key, tx1);
        assert_eq!(false, root.make_write_lock(key, tx2));

        root.remove_append_lock(key, tx1);
        assert_eq!(true, root.make_write_lock(key, tx2));
    }

    #[test]
    fn read_lock() {
        let mut root = Reservation::make_empty_node();

        let key = 42 as u64;
        let tx1 = 5145143;
        let tx2 = 3143151;

        assert_eq!(true, root.make_read_lock(key, tx1));
        assert_eq!(false, root.make_write_lock(key, tx2));
        assert_eq!(true, root.make_read_lock(key, tx2));

        root.remove_read_lock(key, tx1);
        assert_eq!(false, root.make_write_lock(key, tx2));

        root.remove_read_lock(key, tx2);
        assert_eq!(true, root.make_write_lock(key, tx2));
    }
}
