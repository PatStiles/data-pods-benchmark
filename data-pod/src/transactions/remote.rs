use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use data_pods_store::values::Value;
use data_pods_store::{AppDefinition, Datastore, ReservationSet, TransactionInfo};

use crate::applications::{GlobalReservationSet, GlobalValuesMap};

use blockchain_simulator::protocol::EpochId;

use super::{FederatedTransaction, TransactionMode, TransactionState};

pub struct RemoteTransaction {
    #[allow(dead_code)]
    info: TransactionInfo,
    #[allow(dead_code)]
    state: TransactionState,

    // StdMutex used here because of cowlang
    global_ops: Arc<StdMutex<GlobalReservationSet>>,
    reservations: Arc<StdMutex<ReservationSet>>,
}

impl RemoteTransaction {
    pub fn new(info: TransactionInfo) -> Self {
        let state = TransactionState {
            mode: TransactionMode::Execute,
            current_stage: 0,
            skipped_stages: 0,
        };

        let reservations = Arc::new(StdMutex::new(ReservationSet::default()));

        let global_ops = Arc::new(StdMutex::new(GlobalReservationSet::default()));

        Self {
            info,
            reservations,
            global_ops,
            state,
        }
    }

    #[allow(dead_code)]
    pub async fn notify_new_epoch_started(&self, _id: EpochId) {
        //TODO
    }

    pub fn get_reservations(&self) -> Arc<StdMutex<ReservationSet>> {
        self.reservations.clone()
    }

    pub fn commit(&self, datastore: &Datastore) -> bool {
        let mut reservations = self.reservations.lock().unwrap();
        reservations.commit(datastore, self.info.identifier);
        drop(reservations);

        let global_ops = self.global_ops.lock().unwrap();
        global_ops.is_empty()
    }

    pub fn abort(&self, datastore: &Datastore) -> bool {
        let mut reservations = self.reservations.lock().unwrap();
        reservations.abort(datastore, self.info.identifier);
        drop(reservations);

        let global_ops = self.global_ops.lock().unwrap();
        global_ops.is_empty()
    }

    pub async fn execute_global_chunk(
        &self,
        global_values: Arc<StdMutex<GlobalValuesMap>>,
        args: Vec<String>,
        fn_name: String,
        meta_vars: HashMap<String, Value>,
        application: Arc<AppDefinition>,
    ) {
        let (fn_args, func, _) = match application.global_fns.get(&fn_name) {
            Some(f) => f,
            None => {
                panic!("No such global function '{}'", fn_name);
            }
        };

        let mut arg_map = HashMap::new();

        for (pos, arg_name) in args.iter().enumerate() {
            if let Some(val) = meta_vars.get(arg_name) {
                // Change the name to that in function signature
                arg_map.insert(fn_args[pos].0.clone(), val.clone());
            } else {
                panic!("No such meta variable '{}'", arg_name);
            }
        }

        let result = FederatedTransaction::call_global_chunk(
            func.clone(),
            arg_map,
            &self.global_ops,
            global_values,
        );

        match result {
            Ok(_val) => {
                // Do nothing?
            }
            Err(e) => {
                log::warn!("Global op failed: {:?}", e);
            }
        }
    }

    pub fn finalize_global_ops(&self, abort: bool, global_vals: &mut GlobalValuesMap) -> bool {
        let mut global_ops = self.global_ops.lock().unwrap();

        if abort {
            global_ops.abort(global_vals);
        } else {
            global_ops.commit(global_vals);
        }

        drop(global_ops);

        let reservations = self.reservations.lock().unwrap();
        reservations.is_empty()
    }
}
