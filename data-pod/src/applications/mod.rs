mod application;

pub mod bindings;

#[cfg(feature = "tvm")]
mod tvm_bindings;

mod global_reservations;
pub use global_reservations::GlobalReservationSet;

use application::Application;
pub use application::GlobalValuesMap;

use cowlang::Value;

use data_pods_store::protocol::{Message, OpId};
use data_pods_store::{AppDefinition, Datastore};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::blockchain::BlockchainConnection;
use crate::enclave::Enclave;
use crate::remote_parties::RemoteParties;

use blockchain_simulator::AccountId;

pub struct ApplicationRegistry {
    enclave: Arc<Enclave>,
    datastore: Arc<Datastore>,
    remote_parties: Arc<RemoteParties>,
    applications: Mutex<HashMap<String, Arc<Application>>>,
}

impl ApplicationRegistry {
    pub fn new(
        enclave: Arc<Enclave>,
        datastore: Arc<Datastore>,
        remote_parties: Arc<RemoteParties>,
    ) -> Self {
        Self {
            enclave,
            datastore,
            remote_parties,
            applications: Mutex::new(HashMap::new()),
        }
    }

    pub fn create(&self, app_name: String, defs: AppDefinition) {
        let app = Application::new(
            app_name.clone(),
            self.enclave.clone(),
            self.datastore.clone(),
            self.remote_parties.clone(),
            defs,
        );
        let mut apps = self.applications.lock().unwrap();
        let res = apps.insert(app_name.clone(), Arc::new(app));

        if res.is_some() {
            panic!("App '{}' already existed!", app_name);
        }
    }

    pub fn get_app_definition(&self, app_name: &str) -> Arc<AppDefinition> {
        let app = self.get_application(app_name);
        app.get_definition()
    }

    pub fn get_global_values(&self, app_name: &str) -> Arc<Mutex<GlobalValuesMap>> {
        let app = self.get_application(app_name);
        app.get_global_values()
    }

    #[inline]
    fn get_application(&self, app_name: &str) -> Arc<Application> {
        let apps = self.applications.lock().unwrap();

        match apps.get(app_name) {
            Some(a) => a.clone(),
            None => {
                panic!(
                    "No such application '{app_name}'! Options are {:?}.",
                    apps.keys()
                );
            }
        }
    }

    pub async fn call(
        &self,
        op_id: OpId,
        source_account: AccountId,
        app_name: &str,
        fn_name: String,
        blockchain: Arc<BlockchainConnection>,
        args: Vec<Value>,
    ) {
        let app = self.get_application(app_name);
        let result = app.call(source_account, fn_name, blockchain, args).await;

        let msg = Message::CallResponse { op_id, result };

        match self.remote_parties.get_by_location(source_account) {
            Some(rp) => {
                rp.send(msg);
            }
            None => {
                log::trace!(
                    "Transaction done but calling remote party #{} disappeared.",
                    source_account
                );
            }
        }
    }
}
