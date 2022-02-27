use cowlang::Value;

use cowlang_derive::cow_module;

use data_pods_store::objects::{parse_path, random_object_uid, ObjectId};
use data_pods_store::protocol::{Operation, TransactionError};
use data_pods_store::{AppDefinition, Datastore};

use std::convert::TryInto;
use std::sync::Arc;

use std::sync::Mutex as StdMutex;

use crate::applications::{GlobalReservationSet, GlobalValuesMap};
use crate::transactions::TransactionChunk;

#[cfg(feature = "tvm")]
use bytes::Bytes;

use blockchain_simulator::AccountId;

use rand::Rng;

#[cfg(feature = "tvm")]
pub use super::tvm_bindings::TvmBindings;

pub struct RandomBindings {}

impl RandomBindings {
    pub fn new() -> Self {
        Self {}
    }
}

#[cow_module]
impl RandomBindings {
    fn randint(&self, start: Value, end: Value) -> i64 {
        let start: i64 = start.try_into().unwrap();
        let end: i64 = end.try_into().unwrap();

        let mut rng = rand::thread_rng();
        rng.gen_range(start..end)
    }
}

pub struct AssertBindings {
    source_account: AccountId,
    datastore: Arc<Datastore>,
    error: StdMutex<Option<TransactionError>>,
}

impl AssertBindings {
    pub fn new(source_account: AccountId, datastore: Arc<Datastore>) -> Self {
        Self {
            source_account,
            datastore,
            error: StdMutex::new(None),
        }
    }

    pub fn has_error(&self) -> Option<TransactionError> {
        let mut err = self.error.lock().unwrap();
        err.take()
    }
}

#[cow_module]
impl AssertBindings {
    fn owns_object(&self, oid: Value) {
        let oid: ObjectId = oid.try_into().unwrap();
        let account = self.source_account;
        let owner = self.datastore.get_owner(oid.get_uid());

        let access_denied = match owner {
            Ok(o) => account != o,
            Err(_) => true,
        };

        if access_denied {
            let mut err = self.error.lock().unwrap();
            *err = Some(TransactionError::AccessControlFailure { object_id: oid });
        }
    }
}

/// Bindings for a single global value
pub struct GlobalBindings {
    name: String,
    #[allow(dead_code)]
    value_map: Arc<StdMutex<GlobalValuesMap>>,
    reservations: Arc<StdMutex<GlobalReservationSet>>,
}

impl GlobalBindings {
    pub fn new(
        name: String,
        reservations: Arc<StdMutex<GlobalReservationSet>>,
        value_map: Arc<StdMutex<GlobalValuesMap>>,
    ) -> Self {
        Self {
            name,
            reservations,
            value_map,
        }
    }
}

#[cow_module]
impl GlobalBindings {
    pub fn insert(&self, path_val: Value, arg: Value) {
        let mut reservations = self.reservations.lock().unwrap();
        let path_str: String = path_val.try_into().unwrap();

        let path = parse_path(&path_str);

        reservations.insert(self.name.clone(), Operation::MapInsert { path, arg });

        //TODO actually store all global reservations somewhere
        //and check for conflicts before locking
    }
}

pub struct DatastoreBindings {
    app_name: String,
    app_definition: Arc<AppDefinition>,
    transaction: Arc<TransactionChunk>,
}

impl DatastoreBindings {
    pub fn new(
        app_name: String,
        app_definition: Arc<AppDefinition>,
        transaction: Arc<TransactionChunk>,
    ) -> Self {
        Self {
            app_name,
            app_definition,
            transaction,
        }
    }
}

#[cow_module]
impl DatastoreBindings {
    fn new(&self, type_name: Value, mapping: Value) -> ObjectId {
        let uid = random_object_uid();
        let type_name: String = type_name.try_into().unwrap();

        let (typeid, _) = self.app_definition.types.get(&type_name).unwrap();

        let mapping = mapping.into_map().unwrap();
        let obj_id = self.transaction.make_id(uid);

        self.transaction
            .create(&obj_id, &self.app_name, *typeid, mapping)
            .unwrap();

        obj_id
    }

    #[cfg(feature = "tvm")]
    #[returns_object]
    fn load_tvm_model(&self, oid: Value) -> TvmBindings {
        let oid: ObjectId = oid
            .try_into()
            .expect("load_tvm_module failed: first argument is not an object_id");

        let model: String = self
            .transaction
            .get_field(&oid, vec!["model".to_string()])
            .unwrap()
            .try_into()
            .unwrap();
        let params: Bytes = self
            .transaction
            .get_field(&oid, vec!["parameters".to_string()])
            .unwrap()
            .try_into()
            .unwrap();

        TvmBindings::new(model, params)
    }

    fn list_append(&self, oid: Value, path: Value, value: Value) {
        let mut path_vec: Vec<String> = Vec::new();
        let oid: ObjectId = oid
            .try_into()
            .expect("list_append failed: first argument is not an object_id");

        for p in path.into_vec().unwrap().drain(..) {
            path_vec.push(p.try_into().unwrap());
        }

        self.transaction
            .list_append(&oid, &path_vec, value)
            .unwrap();
    }

    fn get(&self, oid: Value) -> Value {
        let oid: ObjectId = oid.try_into().unwrap();

        self.transaction.get(&oid).unwrap()
    }

    fn get_field(&self, oid: Value, path: Value) -> Value {
        let mut path_vec: Vec<String> = Vec::new();
        let oid: ObjectId = oid
            .try_into()
            .expect("get_field failed: first argument is not an object_id");

        for p in path.into_vec().unwrap().drain(..) {
            path_vec.push(p.try_into().unwrap());
        }

        self.transaction.get_field(&oid, path_vec).unwrap()
    }
}
