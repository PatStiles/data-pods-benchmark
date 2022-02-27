use cowlang::Value;

use cowlang::TypeDefinition;
use data_pods_store::protocol::{CallResult, TransactionError};
use data_pods_store::{AppDefinition, Datastore, MetaFnDefinition, MetaOperation};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use std::time::Duration;

use crate::blockchain::BlockchainConnection;
use crate::enclave::Enclave;
use crate::remote_parties::RemoteParties;

use blockchain_simulator::AccountId;

pub type GlobalValuesMap = HashMap<String, Value>;

pub struct Application {
    name: String,
    enclave: Arc<Enclave>,
    datastore: Arc<Datastore>,
    remote_parties: Arc<RemoteParties>,
    definition: Arc<AppDefinition>,
    global_values: Arc<Mutex<GlobalValuesMap>>,
}

impl Application {
    pub fn new(
        name: String,
        enclave: Arc<Enclave>,
        datastore: Arc<Datastore>,
        remote_parties: Arc<RemoteParties>,
        defs: AppDefinition,
    ) -> Self {
        let mut global_values = GlobalValuesMap::new();

        for (name, type_def) in defs.globals.iter() {
            match type_def {
                TypeDefinition::Map { .. } => {
                    global_values.insert(name.clone(), Value::make_map());
                }
                _ => {
                    panic!("Got unsupported type: {:?}", type_def);
                }
            }
        }

        Self {
            datastore,
            remote_parties,
            name,
            enclave,
            definition: Arc::new(defs),
            global_values: Arc::new(Mutex::new(global_values)),
        }
    }

    pub fn get_global_values(&self) -> Arc<Mutex<GlobalValuesMap>> {
        self.global_values.clone()
    }

    pub fn get_definition(&self) -> Arc<AppDefinition> {
        self.definition.clone()
    }

    pub async fn call(
        &self,
        source_account: AccountId,
        fn_name: String,
        blockchain: Arc<BlockchainConnection>,
        args: Vec<Value>,
    ) -> CallResult {
        let meta_function = if let Some(fn_def) = self.definition.functions.get(&fn_name) {
            let (arg_defs, _, public) = fn_def;
            let mut arg_names = Vec::new();

            if !public {
                panic!("Function '{}' is not public.", fn_name);
            }

            for (name, _) in arg_defs.iter() {
                arg_names.push(name.clone());
            }

            // Wrap as meta function
            let commit_ops = vec![MetaOperation::Call {
                fn_name: fn_name.clone(),
                args: arg_names,
                alias: Some("result".to_string()),
            }];

            Arc::new(MetaFnDefinition::new(arg_defs.to_vec(), vec![commit_ops]))
        } else if let Some(meta_fn_def) = self.definition.meta_fns.get(&fn_name) {
            meta_fn_def.clone()
        } else {
            panic!("No such function (or meta function) '{}'", fn_name);
        };

        if args.len() != meta_function.args.len() {
            panic!(
                "Invalid number of arguments for {}::{}. Expected {}, got {}.",
                self.name,
                fn_name,
                meta_function.args.len(),
                args.len()
            );
        }

        loop {
            let transaction = self
                .enclave
                .create_transaction(
                    source_account,
                    self.name.clone(),
                    fn_name.clone(),
                    self.definition.clone(),
                    self.datastore.clone(),
                    blockchain.clone(),
                    self.enclave.clone(),
                    self.remote_parties.clone(),
                    meta_function.clone(),
                )
                .await;

            transaction.init_arguments(args.clone()).await;

            match transaction.execute().await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    if let TransactionError::AccessControlFailure { .. } = err {
                        return Err(err);
                    } else if let TransactionError::LockContention { .. } = err {
                        log::warn!("Lock contention! Will retry...");
                        // Avoid data races
                        let offset = Duration::from_millis(5);
                        tokio::time::sleep(offset).await;
                    } else if let TransactionError::NoResponse { .. } = err {
                        log::warn!("Transaction failed due to unresponsive remote peer");
                        return Err(err);
                    } else {
                        log::error!("Transaction failed due to: {:?}", err);
                        return Err(err);
                    }

                    // Retry
                    continue;
                }
            }
        }
    }
}
