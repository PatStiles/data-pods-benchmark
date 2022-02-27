use std::collections::HashMap;
use std::sync::Arc;

use crate::merkle::FrozenMerkleTree;
use crate::objects::{field_name_to_uid, Object, ObjectTypeId, ObjectUid};
use crate::patricia::{FrozenPatriciaTree, PatriciaTree};
use crate::protocol::TxId;
use crate::reservations::Reservation;
use crate::values::Value;
use crate::{DatastoreError, TypeRegistry};

use blockchain_simulator::AccountId;

use bit_vec::BitVec;

use byte_slice_cast::*;

type ValueMap = HashMap<ObjectUid, Object>;

pub struct DataShard {
    types: Arc<TypeRegistry>,
    objects: ValueMap,
    state_root: Option<PatriciaTree>,
    reservations: Reservation,
}

impl DataShard {
    pub fn new(types: Arc<TypeRegistry>) -> Self {
        let state_root = Some(PatriciaTree::new_node());
        let objects = ValueMap::default();
        let reservations = Reservation::make_empty_node();

        Self {
            types,
            objects,
            state_root,
            reservations,
        }
    }

    pub fn update_hashes(&mut self) -> bool {
        self.state_root.as_mut().unwrap().update_hash()
    }

    pub fn num_locked_objects(&self) -> usize {
        self.reservations.num_children()
    }

    pub fn get_state_hashes(&self) -> FrozenPatriciaTree {
        self.state_root.as_ref().unwrap().clone_frozen()
    }

    pub fn get_reservation_hashes(&self) -> FrozenMerkleTree {
        self.reservations.get_hash_tree()
    }

    pub fn has_object(&self, uid: ObjectUid) -> bool {
        self.objects.contains_key(&uid)
    }

    pub fn get_owner(&self, uid: ObjectUid) -> Result<AccountId, DatastoreError> {
        if uid == 0 {
            return Err(DatastoreError::InvalidKey);
        }

        match self.objects.get(&uid) {
            Some(obj) => Ok(obj.get_owner()),
            None => Err(DatastoreError::NoSuchObject { uid }),
        }
    }

    pub fn get_field<T: AsRef<str>>(
        &self,
        uid: ObjectUid,
        path: &[T],
    ) -> Result<Value, DatastoreError> {
        if uid == 0 {
            return Err(DatastoreError::InvalidKey);
        }

        match self.objects.get(&uid) {
            Some(obj) => {
                if path.is_empty() {
                    Ok(obj.clone_as_value())
                } else {
                    let (start_key, subpath) = path.split_first().unwrap();
                    let mut pos = obj.get(start_key.as_ref()).unwrap();

                    for key in subpath {
                        pos = pos.get(key.as_ref()).unwrap();
                    }

                    Ok(pos.clone())
                }
            }
            None => Err(DatastoreError::NoSuchObject { uid }),
        }
    }

    pub fn map_insert<T: AsRef<str> + ToString>(
        &mut self,
        uid: ObjectUid,
        path: &[T],
        value: Value,
    ) -> Result<(), DatastoreError> {
        if uid == 0 {
            return Err(DatastoreError::InvalidKey);
        }

        if path.len() < 2 {
            return Err(DatastoreError::InvalidArguments);
        }

        let (root_key, subpath) = path.split_first().unwrap();

        let root_obj = match self.objects.get_mut(&uid) {
            Some(obj) => obj,
            None => {
                return Err(DatastoreError::NoSuchObject { uid });
            }
        };

        let mut pos = match root_obj.get_mut(root_key.as_ref()) {
            Some(v) => v,
            None => {
                return Err(DatastoreError::NoSuchField {
                    name: String::from(root_key.as_ref()),
                });
            }
        };

        let (end_key, subpath) = subpath.split_last().unwrap();

        for key in subpath {
            pos = pos.get_mut(key.as_ref()).unwrap();
        }

        match pos.map_insert(end_key.to_string(), value) {
            Ok(()) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_append<T: AsRef<str> + ToString>(
        &mut self,
        uid: ObjectUid,
        path: &[T],
        value: Value,
    ) -> Result<(), DatastoreError> {
        if uid == 0 {
            return Err(DatastoreError::InvalidKey);
        }

        if path.is_empty() {
            return Err(DatastoreError::InvalidArguments);
        }

        let (root_key, path_) = path.split_first().unwrap();

        let root_obj = match self.objects.get_mut(&uid) {
            Some(obj) => obj,
            None => {
                return Err(DatastoreError::NoSuchObject { uid });
            }
        };

        let mut current = match root_obj.get_mut(root_key.as_ref()) {
            Some(v) => v,
            None => {
                return Err(DatastoreError::InvalidArguments);
            }
        };

        // The list can only be created if its not a root field
        if path.len() == 1 {
            match current.list_append(value) {
                Ok(()) => Ok(()),
                Err(e) => Err(e.into()),
            }
        } else {
            let (end_key, subpath) = path_.split_last().unwrap();

            for key in subpath {
                current = current.get_mut(key.as_ref()).unwrap();
            }

            let end_key = end_key.to_string();
            let leaf = current
                .get_or_create_mut(end_key, Value::make_list)
                .unwrap();

            match leaf.list_append(value) {
                Ok(()) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    }

    pub fn create(
        &mut self,
        uid: ObjectUid,
        owner: AccountId,
        application: &str,
        typeid: ObjectTypeId,
        content: HashMap<String, Value>,
    ) -> Result<(), DatastoreError> {
        let obj_type = self.types.get(application, typeid);

        if uid == 0 {
            return Err(DatastoreError::InvalidKey);
        }

        let obj = obj_type.create_object(owner, content);

        if self.objects.contains_key(&uid) {
            return Err(DatastoreError::AlreadyExists { uid });
        }

        self.objects.insert(uid, obj);

        Ok(())
    }

    pub fn update(&mut self, uid: ObjectUid, value: Value) -> Result<(), DatastoreError> {
        //TODO support paths
        if uid == 0 {
            return Err(DatastoreError::InvalidKey);
        }

        let object = match self.objects.get(&uid) {
            Some(o) => o,
            None => {
                panic!("No such object!");
            }
        };

        let object_type = self
            .types
            .get(object.get_application(), object.get_typeid());

        let new_obj = object_type.value_to_object(object.get_owner(), value);

        let hash = new_obj.get_hash();

        let uid_vec = BitVec::from_bytes([uid].as_byte_slice());

        self.state_root = Some(PatriciaTree::set_child_hash(
            self.state_root.take().unwrap(),
            uid_vec,
            hash,
        ));

        self.objects.insert(uid, new_obj);

        Ok(())
    }

    pub fn write_lock(&mut self, uid: ObjectUid, path: &[String], owner: TxId) -> bool {
        if path.is_empty() {
            self.reservations.make_write_lock(uid, owner)
        } else {
            let (end_key, subpath) = path.split_last().unwrap();

            let root = match self.reservations.get_child(uid) {
                Some(child) => child,
                None => {
                    return false;
                }
            };

            if let Some(leaf) = root.traverse_path(subpath) {
                leaf.make_write_lock(field_name_to_uid(end_key), owner)
            } else {
                false
            }
        }
    }

    pub fn write_unlock(&mut self, uid: ObjectUid, path: &[String], owner: TxId) {
        if path.is_empty() {
            self.reservations.remove_write_lock(uid, owner)
        } else {
            let (end_key, subpath) = path.split_last().unwrap();

            let cleanup = {
                let root = &mut self.reservations.get_child(uid).unwrap();
                let leaf = root.traverse_path(subpath).unwrap();

                leaf.remove_write_lock(field_name_to_uid(end_key), owner);
                leaf.is_empty()
            };

            if cleanup {
                self.garbage_collect(uid, subpath);
            }
        }
    }

    pub fn read_lock<T: AsRef<str>>(&mut self, uid: ObjectUid, path: &[T], owner: TxId) -> bool {
        if path.is_empty() {
            self.reservations.make_read_lock(uid, owner)
        } else {
            let (end_key, subpath) = path.split_last().unwrap();

            let current = match self.reservations.get_child(uid) {
                Some(child) => child,
                None => {
                    return false;
                }
            };

            if let Some(leaf) = current.traverse_path(subpath) {
                leaf.make_read_lock(field_name_to_uid(end_key.as_ref()), owner)
            } else {
                false
            }
        }
    }

    pub fn append_lock(&mut self, uid: ObjectUid, path: &[String], owner: TxId) -> bool {
        if path.is_empty() {
            panic!("Cannot append to root!");
        }

        let (end_key, subpath) = path.split_last().unwrap();

        let current = match self.reservations.get_child(uid) {
            Some(child) => child,
            None => {
                return false;
            }
        };

        if let Some(leaf) = current.traverse_path(subpath) {
            leaf.make_append_lock(field_name_to_uid(end_key), owner)
        } else {
            false
        }
    }

    pub fn read_unlock(&mut self, uid: ObjectUid, path: &[String], owner: TxId) {
        if path.is_empty() {
            self.reservations.remove_read_lock(uid, owner);
        } else {
            let (end_key, subpath) = path.split_last().unwrap();

            let cleanup = {
                let root = self.reservations.get_child(uid).unwrap();
                let leaf = root.traverse_path(subpath).unwrap();
                leaf.remove_read_lock(field_name_to_uid(end_key), owner);

                leaf.is_empty()
            };

            if cleanup {
                self.garbage_collect(uid, subpath);
            }
        }
    }

    pub fn append_unlock(&mut self, uid: ObjectUid, path: &[String], owner: TxId) {
        if path.is_empty() {
            panic!("Cannot append to root!");
        }

        let (end_key, subpath) = path.split_last().unwrap();

        let cleanup = {
            let root = self.reservations.get_child(uid).unwrap();
            let leaf = root.traverse_path(subpath).unwrap();

            leaf.remove_append_lock(field_name_to_uid(end_key), owner);
            leaf.is_empty()
        };

        if cleanup {
            self.garbage_collect(uid, subpath);
        }
    }

    #[inline]
    fn garbage_collect(&mut self, uid: ObjectUid, mut subpath: &[String]) {
        let root = self.reservations.get_child(uid).unwrap();
        let mut leaf = root.traverse_path(subpath).unwrap();

        // traverse until we get to the root
        while !subpath.is_empty() {
            if leaf.is_empty() {
                let (end_key, s) = subpath.split_last().unwrap();
                subpath = s;

                // Get parent
                leaf = root.traverse_path(subpath).unwrap();
                leaf.remove_child(field_name_to_uid(end_key));
            } else {
                return;
            }
        }

        if root.is_empty() {
            self.reservations.remove_child(uid);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn lock_unlock() {
        let types = Arc::new(TypeRegistry::default());
        let mut shard = DataShard::new(types);

        let uid = 4131451;
        let path = vec!["foo".to_string(), "bar".to_string()];
        let path2 = vec!["foo".to_string(), "baz".to_string()];

        assert_eq!(true, shard.append_lock(uid, &path, 1));
        assert_eq!(true, shard.append_lock(uid, &path2, 1));
        assert_eq!(shard.num_locked_objects(), 1);

        shard.append_unlock(uid, &path, 1);
        assert_eq!(shard.num_locked_objects(), 1);
        shard.append_unlock(uid, &path2, 1);
        assert_eq!(shard.num_locked_objects(), 0);

        assert_eq!(true, shard.read_lock(uid, &path, 1));
        assert_eq!(shard.num_locked_objects(), 1);
        assert_eq!(true, shard.read_lock(uid, &path2, 1));
        assert_eq!(shard.num_locked_objects(), 1);

        // Write lock on same object should fail
        assert_eq!(false, shard.write_lock(uid, &path, 1));

        shard.read_unlock(uid, &path, 1);
        assert_eq!(shard.num_locked_objects(), 1);
        shard.read_unlock(uid, &path2, 1);
        assert_eq!(shard.num_locked_objects(), 0);
    }
}
