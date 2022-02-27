use data_pods_store::objects::{ObjectId, ObjectTypeId, ObjectUid};
use data_pods_store::protocol::{Operation, TxId};
use data_pods_store::values::Value;
use data_pods_store::{Datastore, DatastoreError, ReservationSet};

use std::collections::HashMap;
use std::sync::Arc;

use std::sync::Mutex as StdMutex;

use blockchain_simulator::AccountId;

pub struct TransactionChunk {
    identifier: TxId,
    owner: AccountId,
    location: AccountId,
    datastore: Arc<Datastore>,
    reservations: Arc<StdMutex<ReservationSet>>,
    error: StdMutex<Option<(ObjectId, Vec<String>)>>,
}

impl TransactionChunk {
    pub fn new(
        identifier: TxId,
        owner: AccountId,
        location: AccountId,
        datastore: Arc<Datastore>,
        reservations: Arc<StdMutex<ReservationSet>>,
    ) -> Self {
        Self {
            identifier,
            owner,
            location,
            datastore,
            reservations,
            error: StdMutex::new(None),
        }
    }

    pub fn create(
        &self,
        oid: &ObjectId,
        application: &str,
        typeid: ObjectTypeId,
        fields: HashMap<String, Value>,
    ) -> Result<(), DatastoreError> {
        let uid = oid.get_uid();
        let path = &[];

        if self.datastore.write_lock(uid, path, self.identifier) {
            let mut reservations = self.reservations.lock().unwrap();
            let op = Operation::Create {
                owner: self.owner,
                application: application.to_string(),
                typeid,
                fields,
            };
            reservations.insert(uid, op);
        } else {
            let mut err = self.error.lock().unwrap();
            *err = Some((oid.clone(), path.to_vec()));
        }

        Ok(())
    }

    pub fn has_error(&self) -> Option<(ObjectId, Vec<String>)> {
        let mut err = self.error.lock().unwrap();
        err.take()
    }

    pub fn list_append(
        &self,
        oid: &ObjectId,
        path: &[String],
        value: Value,
    ) -> Result<(), DatastoreError> {
        if self
            .datastore
            .append_lock(oid.get_uid(), path, self.identifier)
        {
            let mut reservations = self.reservations.lock().unwrap();

            let op = Operation::ListAppend {
                path: path.to_vec(),
                arg: value,
            };
            reservations.insert(oid.get_uid(), op);
        } else {
            let mut err = self.error.lock().unwrap();
            *err = Some((oid.clone(), path.to_vec()));
        }

        Ok(())
    }

    pub fn make_id(&self, uid: ObjectUid) -> ObjectId {
        ObjectId::new(uid, self.location)
    }

    pub fn get_field(&self, oid: &ObjectId, path: Vec<String>) -> Result<Value, DatastoreError> {
        let uid = oid.get_uid();

        //FIXME do more efficient in one call
        if self.datastore.read_lock(uid, &path, self.identifier) {
            let mut locks = self.reservations.lock().unwrap();
            let op = Operation::Read { path: path.clone() };
            locks.insert(uid, op);
        } else {
            let mut err = self.error.lock().unwrap();
            *err = Some((oid.clone(), path.clone()));
        }

        self.datastore.get_field(uid, &path)
    }

    pub fn get(&self, oid: &ObjectId) -> Result<Value, DatastoreError> {
        self.get_field(oid, vec![])
    }
}
