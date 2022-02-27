use cowlang::ast::Program;
use cowlang::Interpreter;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryInto;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use tokio::sync::{oneshot, Mutex, MutexGuard};

use data_pods_store::{
    AppDefinition, Datastore, DigestAppendResult, EpochHashes, FunctionDefinition,
    MetaFnDefinition, MetaOperation, ReservationSet, TransactionInfo,
};

use data_pods_store::merkle::MerkleProof;
use data_pods_store::objects::ObjectId;
use data_pods_store::protocol::{LedgerMessage, TransactionError, TransactionFlag, TxWitness};
use data_pods_store::values::Value;

use data_pods_utils::VecSet;

use crate::applications::bindings::{
    AssertBindings, DatastoreBindings, GlobalBindings, RandomBindings,
};
use crate::applications::{GlobalReservationSet, GlobalValuesMap};
use crate::blockchain::BlockchainConnection;
use crate::enclave::Enclave;
use crate::remote_parties::RemoteParties;

use blockchain_simulator::protocol::EpochId;
use blockchain_simulator::AccountId;

use super::{PendingRequests, TransactionChunk, TransactionMode, TransactionState, APPLY_LEN};

/// This coordinates a transactions executing across multiple data pods
pub struct FederatedTransaction {
    /// Identifier and other transaction info
    info: TransactionInfo,
    location: AccountId,
    state: Mutex<TransactionState>,
    pending_requests: Mutex<PendingRequests>,
    application: Arc<AppDefinition>,
    /// The meta-function this transaction is execution
    meta_fn: Arc<MetaFnDefinition>,
    datastore: Arc<Datastore>,
    enclave: Arc<Enclave>,
    remote_pods: Mutex<VecSet<AccountId>>,
    blockchain: Arc<BlockchainConnection>,
    remote_parties: Arc<RemoteParties>,
    meta_vars: Mutex<HashMap<String, Value>>,
    done_sender: Mutex<Option<oneshot::Sender<()>>>,
    result: Mutex<Option<Result<Option<MerkleProof>, TransactionError>>>,
    /// Operations executing at the global ledger (not supported right now)
    global_ops: Arc<StdMutex<GlobalReservationSet>>,
    /// Reservations (locks) being held by this transaction
    reservations: Arc<StdMutex<ReservationSet>>,
}

impl FederatedTransaction {
    //FIXME
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        info: TransactionInfo,
        application: Arc<AppDefinition>,
        datastore: Arc<Datastore>,
        blockchain: Arc<BlockchainConnection>,
        enclave: Arc<Enclave>,
        remote_parties: Arc<RemoteParties>,
        meta_fn: Arc<MetaFnDefinition>,
    ) -> Self {
        let location = blockchain.get_location();
        let result = Mutex::new(None);

        let state = TransactionState {
            mode: TransactionMode::Execute,
            current_stage: 0,
            skipped_stages: 0,
        };

        log::debug!(
            "Created a new federated transaction with id={}",
            info.identifier
        );

        Self {
            reservations: Arc::new(Default::default()),
            global_ops: Arc::new(Default::default()),
            blockchain,
            remote_parties,
            datastore,
            result,
            application,
            meta_fn,
            location,
            enclave,
            remote_pods: Mutex::new(VecSet::default()),
            state: Mutex::new(state),
            meta_vars: Mutex::new(HashMap::new()),
            info,
            pending_requests: Mutex::new(PendingRequests::default()),
            done_sender: Mutex::new(None),
        }
    }

    async fn get_result(&self) -> Result<TxWitness, TransactionError> {
        let res = self.result.lock().await.take();
        let res = if let Some(value) = res {
            value
        } else {
            // Can happen during shutdown
            log::error!("Result not ready yet. Are we shutting down?");
            return Err(TransactionError::Unexpected);
        };

        let proof = match res {
            Ok(p) => p,
            Err(e) => {
                return Err(e);
            }
        };

        let meta_vars = self.meta_vars.lock().await;

        let result = if let Some(val) = meta_vars.get("result") {
            val.clone()
        } else {
            Value::None
        };

        Ok(TxWitness { result, proof })
    }

    pub async fn init_arguments(&self, mut args: Vec<Value>) {
        let mut meta_vars = self.meta_vars.lock().await;

        if args.len() != self.meta_fn.args.len() {
            panic!("Argument mismatch!");
        }

        for (pos, val) in args.drain(..).enumerate() {
            let (name, meta_val) = &self.meta_fn.args[pos];

            if !Value::type_check(meta_val, &val) {
                panic!("Typecheck failed!: name {name}");
            }

            meta_vars.insert(name.clone(), val.clone());
        }
    }

    async fn issue_next_stages(&self, mut state: MutexGuard<'_, TransactionState>, now: EpochId) {
        loop {
            if state.current_stage < self.meta_fn.num_stages() {
                state.current_stage += 1;
                let stage_end = state.execute_end(self.info.start);

                if stage_end <= now {
                    panic!("Transaction execute stage out of sync (start:{} stage:{} skipped:{} now:{})", self.info.start, state.current_stage, state.skipped_stages, now);
                }

                let skipped = match self.issue_stage(state.current_stage - 1).await {
                    Ok(skipped) => skipped,
                    Err(e) => {
                        // Stage didn't actually execute
                        // but aborted
                        state.current_stage -= 1;
                        self.issue_abort(state, e).await;
                        return;
                    }
                };

                if skipped {
                    state.skipped_stages += 1;
                } else {
                    return;
                }
            } else {
                let stage_end = state.finalize_end(self.info.start);

                if stage_end <= now {
                    panic!(
                        "Transaction commit out of sync (start:{} stage:{} skipped:{} now:{})",
                        self.info.start, state.current_stage, state.skipped_stages, now
                    );
                }

                self.issue_commit(state).await;
                return;
            }
        }
    }

    async fn process_fn_call(
        &self,
        fn_def: &FunctionDefinition,
        func_id: usize,
        args: &[String],
        alias: &Option<String>,
        is_local: &mut bool,
        remote_calls: &mut HashMap<AccountId, Vec<usize>>,
    ) -> Result<(), TransactionError> {
        let (fn_args, func, _) = fn_def;
        let mut location = None;
        let mut arg_map = HashMap::new();

        {
            let meta_vars = self.meta_vars.lock().await;

            for (pos, arg_name) in args.iter().enumerate() {
                if let Some(val) = meta_vars.get(arg_name) {
                    if location.is_none() {
                        let oid_result: Result<ObjectId, ()> = val.clone().try_into();

                        if let Ok(oid) = oid_result {
                            location = Some(oid.get_location());
                        }
                    }

                    // Change the name to that in function signature
                    arg_map.insert(fn_args[pos].0.clone(), val.clone());
                } else {
                    panic!("No such meta variable '{arg_name}' in {meta_vars:?}");
                }
            }
        }

        let location = if let Some(l) = location {
            l
        } else {
            //Execute locally by default
            self.location
        };

        if location == self.location {
            *is_local = true;

            let tx_chunk = Arc::new(TransactionChunk::new(
                self.info.identifier,
                self.info.source_account,
                self.location,
                self.datastore.clone(),
                self.reservations.clone(),
            ));

            let result = FederatedTransaction::call_sub_transaction(
                self.info.clone(),
                self.application.clone(),
                func.clone(),
                arg_map,
                self.datastore.clone(),
                tx_chunk,
            );

            match result {
                Ok(rval) => {
                    if let Some(var_name) = alias {
                        let mut meta_vars = self.meta_vars.lock().await;
                        meta_vars.insert(var_name.clone(), rval);
                    }

                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            let e = remote_calls.entry(location).or_insert_with(Vec::new);
            e.push(func_id);

            Ok(())
        }
    }

    async fn process_global_fn_call(
        &self,
        _fn_def: &FunctionDefinition,
        stage_id: u32,
        func_id: usize,
        _args: &[String],
    ) {
        let msg = LedgerMessage::ExecuteTxChunk {
            info: self.info.clone(),
            stage: stage_id,
            func_id,
            //FIXME don't send all meta vars
            meta_vars: self.meta_vars.lock().await.clone(),
        };

        self.blockchain.send_transaction(msg);
    }

    async fn issue_stage(&self, stage_id: u32) -> Result<bool, TransactionError> {
        let mut is_local = false;
        let stage = self.meta_fn.get_stage(stage_id);

        let mut has_global_call = false;
        let mut remote_calls = HashMap::new();

        log::debug!(
            "Issuing stage {} of {} for transaction #{}",
            stage_id + 1,
            self.meta_fn.num_stages(),
            self.info.identifier
        );

        for (func_id, op) in stage.iter().enumerate() {
            match op {
                MetaOperation::Call {
                    fn_name,
                    args,
                    alias,
                } => {
                    if let Some(fn_def) = self.application.functions.get(fn_name) {
                        if let Err(e) = self
                            .process_fn_call(
                                fn_def,
                                func_id,
                                args,
                                alias,
                                &mut is_local,
                                &mut remote_calls,
                            )
                            .await
                        {
                            return Err(e);
                        }
                    } else if let Some(fn_def) = self.application.global_fns.get(fn_name) {
                        if has_global_call {
                            panic!("Can't have more than one global call per stage");
                        }

                        has_global_call = true;
                        self.process_global_fn_call(fn_def, stage_id, func_id, args)
                            .await;
                    } else {
                        panic!("No such functions '{}'", fn_name);
                    }
                }
                MetaOperation::AssertEquals { lhs, rhs } => {
                    if lhs != rhs {
                        return Err(TransactionError::InvalidArguments);
                    }
                }
                MetaOperation::AssertNotEquals { lhs, rhs } => {
                    if lhs == rhs {
                        return Err(TransactionError::InvalidArguments);
                    }
                }
            }
        }

        if remote_calls.is_empty() && !has_global_call {
            Ok(true)
        } else {
            if is_local {
                loop {
                    match self.datastore.record_transaction(
                        self.info.source_location,
                        self.info.identifier,
                        TransactionFlag::Reserve,
                    ) {
                        DigestAppendResult::Ok { over_threshold } => {
                            if over_threshold {
                                self.blockchain.sync();
                            }
                            break;
                        }
                        DigestAppendResult::ReachedMax => {
                            self.blockchain.wait_sync().await;
                        }
                    }
                }
            }

            let mut pending_requests = self.pending_requests.lock().await;
            pending_requests.has_global_op = has_global_call;

            for (loc, calls) in remote_calls.drain() {
                let rp = match self.remote_parties.get_by_location(loc) {
                    Some(rp) => rp,
                    None => {
                        panic!("Cannot perform remote call: No such party #{loc}");
                    }
                };

                //FIXME don't send all meta variables
                let meta_vars = {
                    let lock = self.meta_vars.lock().await;
                    lock.clone()
                };

                let hdl = rp
                    .lock_transaction(self.info.clone(), stage_id, calls, meta_vars)
                    .await;

                pending_requests.peer_requests.push(hdl);

                let mut remote_pods = self.remote_pods.lock().await;
                remote_pods.insert(loc);
            }

            Ok(false)
        }
    }

    pub async fn execute_global_chunk(
        &self,
        stage: u32,
        func_id: usize,
        global_values: Arc<StdMutex<GlobalValuesMap>>,
    ) {
        let meta_op = &self.meta_fn.get_stage(stage)[func_id];

        let (args, fn_args, func, alias) = if let MetaOperation::Call {
            fn_name,
            args,
            alias,
        } = meta_op
        {
            let (fn_args, func, _) = match self.application.global_fns.get(fn_name) {
                Some(f) => f,
                None => {
                    panic!("No such global function \"{fn_name}\"");
                }
            };

            (args, fn_args, func, alias)
        } else {
            panic!("Invalid global call");
        };

        let mut arg_map = HashMap::new();

        for (pos, arg_name) in args.iter().enumerate() {
            let mvars = self.meta_vars.lock().await;

            if let Some(val) = mvars.get(arg_name) {
                // Change the name to that in function signature
                arg_map.insert(fn_args[pos].0.clone(), val.clone());
            } else {
                panic!("No such meta variable \"{arg_name}\"");
            }
        }

        let result = FederatedTransaction::call_global_chunk(
            func.clone(),
            arg_map,
            &self.global_ops,
            global_values,
        );

        match result {
            Ok(val) => {
                let mut preqs = self.pending_requests.lock().await;
                preqs.has_global_op = false;

                if let Some(name) = alias {
                    let mut mvars = self.meta_vars.lock().await;
                    mvars.insert(name.clone(), val);
                }
            }
            Err(err) => {
                log::warn!("Global op failed: {err:?}");
            }
        }
    }

    pub fn call_global_chunk(
        func: Arc<Program>,
        mut arg_map: HashMap<String, Value>,
        global_ops: &Arc<StdMutex<GlobalReservationSet>>,
        global_values: Arc<StdMutex<GlobalValuesMap>>,
    ) -> Result<Value, TransactionError> {
        let mut interpreter = Interpreter::default();

        let random_bindings = Rc::new(RandomBindings::new());
        interpreter.register_module(String::from("random"), random_bindings);

        for (name, _) in global_values.lock().unwrap().iter() {
            let global_bindings = Rc::new(GlobalBindings::new(
                name.clone(),
                global_ops.clone(),
                global_values.clone(),
            ));
            interpreter.register_module(String::from(name), global_bindings);
        }

        for (name, val) in arg_map.drain() {
            interpreter.set_value(name, val);
        }

        let result = interpreter.run(&*func);

        //TODO handle failure
        Ok(result)
    }

    pub fn call_sub_transaction(
        info: TransactionInfo,
        application: Arc<AppDefinition>,
        func: Arc<Program>,
        mut arg_map: HashMap<String, Value>,
        datastore: Arc<Datastore>,
        tx_chunk: Arc<TransactionChunk>,
    ) -> Result<Value, TransactionError> {
        let mut interpreter = Interpreter::default();

        let random_bindings = Rc::new(RandomBindings::new());
        interpreter.register_module(String::from("random"), random_bindings);

        let assert_bindings = Rc::new(AssertBindings::new(info.source_account, datastore));
        interpreter.register_module(String::from("assert"), assert_bindings.clone());

        let db_bindings = Rc::new(DatastoreBindings::new(
            info.application,
            application,
            tx_chunk.clone(),
        ));
        interpreter.register_module(String::from("db"), db_bindings);

        for (name, val) in arg_map.drain() {
            interpreter.set_value(name, val);
        }

        let res = interpreter.run(&*func);

        //FIXME allow returning multiple errors?
        if let Some((oid, path)) = tx_chunk.has_error() {
            Err(TransactionError::LockContention { oid, path })
        } else if let Some(err) = assert_bindings.has_error() {
            Err(err)
        } else {
            Ok(res)
        }
    }

    pub async fn notify_new_epoch_started(&self, epoch: EpochId) {
        let mut state = self.state.lock().await;

        match state.mode {
            TransactionMode::Execute => {
                if (epoch > self.info.start) && (epoch - self.info.start) % APPLY_LEN == 0 {
                    self.check_stage_end(state, epoch).await;
                }
            }
            TransactionMode::Commit | TransactionMode::Abort => {
                let end = state.finalize_end(self.info.start);

                match end.cmp(&epoch) {
                    Ordering::Equal => {
                        state.mode = TransactionMode::Done;
                        drop(state);

                        self.enclave.remove_transaction(self.info.identifier).await;

                        let sender = self.done_sender.lock().await.take().unwrap();
                        sender.send(()).expect("Failed to send result");
                    }
                    Ordering::Less => {
                        panic!("Invalid state: did not terminate transaction (skipped={}, start={}, epoch={epoch})", state.skipped_stages, self.info.start);
                    }
                    _ => {}
                }
            }
            TransactionMode::Done => {}
        }
    }

    async fn check_stage_end(&self, state: MutexGuard<'_, TransactionState>, now: EpochId) {
        let result = {
            let mut pending_requests = self.pending_requests.lock().await;
            let mut tx_result = Ok(());

            for hdl in pending_requests.peer_requests.drain(..) {
                match hdl.try_get_result() {
                    Some(val) => {
                        match val {
                            Ok((results, _proof)) => {
                                //FIXME verify proof here

                                for (name, value) in results {
                                    let mut mvars = self.meta_vars.lock().await;
                                    mvars.insert(name, value);
                                }
                            }
                            Err(err) => {
                                tx_result = Err(err);
                            }
                        }
                    }
                    None => {
                        log::trace!("Aborting transaction due to no response from remote data pod (start:{} stage:{} skipped:{} now:{})", self.info.start, state.current_stage, state.skipped_stages, now);

                        tx_result = Err(TransactionError::NoResponse);
                    }
                }
            }

            if pending_requests.has_global_op {
                log::warn!("Did not get global operation response");
                Err(TransactionError::NoResponse)
            } else {
                tx_result
            }
        };

        match result {
            Ok(()) => {
                self.issue_next_stages(state, now).await;
            }
            Err(err) => {
                let stage_end = state.finalize_end(self.info.start);

                if stage_end <= now {
                    panic!(
                        "Transaction abort out of sync (start:{} stage:{} skipped:{} now:{})",
                        self.info.start, state.current_stage, state.skipped_stages, now
                    );
                }

                self.issue_abort(state, err).await;
            }
        }
    }

    pub async fn flush(&self, pos: usize, hashes: &EpochHashes, set_size: usize) {
        let proof = hashes.transaction_tree.generate_proof(pos, 2, set_size);
        *self.result.lock().await = Some(Ok(Some(proof)));
    }

    #[inline]
    async fn issue_commit(&self, mut state: MutexGuard<'_, TransactionState>) {
        let has_local_writes = {
            let mut res = self.reservations.lock().unwrap();

            // commit will drain all ops so we have to check this before
            let has_writes = res.has_writes();

            if !res.is_empty() {
                res.commit(&*self.datastore, self.info.identifier);
            }

            has_writes
        };

        // One-shot read-only
        if !has_local_writes && self.remote_pods.lock().await.is_empty() {
            state.mode = TransactionMode::Done;
            *self.result.lock().await = Some(Ok(None));

            let sender = self.done_sender.lock().await.take().unwrap();
            sender.send(()).expect("Failed to send result");
            return;
        }

        state.mode = TransactionMode::Commit;
        drop(state);

        loop {
            match self.datastore.record_transaction(
                self.info.source_location,
                self.info.identifier,
                TransactionFlag::Commit,
            ) {
                DigestAppendResult::Ok { over_threshold } => {
                    if over_threshold {
                        self.blockchain.sync();
                    }
                    break;
                }
                DigestAppendResult::ReachedMax => {
                    self.blockchain.wait_sync().await;
                }
            }
        }

        self.enclave.notify_commit(self.info.identifier).await;

        {
            let global_ops = self.global_ops.lock().unwrap();

            if !global_ops.is_empty() {
                let msg = LedgerMessage::FinalizeTransaction {
                    info: self.info.clone(),
                    abort: false,
                };

                self.blockchain.send_transaction(msg);
            }
        }

        let remote_pods = self.remote_pods.lock().await;

        for loc in remote_pods.iter() {
            match self.remote_parties.get_by_location(*loc) {
                Some(rp) => {
                    rp.finalize_transaction(self.info.identifier, false).await;
                }
                None => {
                    // Can happen during shutdown currently
                    log::error!("Transaction cannot perform remote call. No such party #{loc}");
                }
            }
        }
    }

    async fn issue_abort(
        &self,
        mut state: MutexGuard<'_, TransactionState>,
        err: TransactionError,
    ) {
        let result = Err(err);

        state.mode = TransactionMode::Abort;
        *self.result.lock().await = Some(result.clone());

        drop(state);

        {
            let mut res = self.reservations.lock().unwrap();
            res.abort(&*self.datastore, self.info.identifier);
        }

        {
            let global_ops = self.global_ops.lock().unwrap();

            if !global_ops.is_empty() {
                let msg = LedgerMessage::FinalizeTransaction {
                    info: self.info.clone(),
                    abort: true,
                };

                self.blockchain.send_transaction(msg);
            }
        }

        let remote_pods = self.remote_pods.lock().await;

        for loc in remote_pods.iter() {
            match self.remote_parties.get_by_location(*loc) {
                Some(rp) => {
                    rp.finalize_transaction(self.info.identifier, true).await;
                }
                None => {
                    log::error!("Transaction cannot perform remote call. No such party #{loc}");
                }
            }
        }

        drop(remote_pods);

        loop {
            match self.datastore.record_transaction(
                self.info.source_location,
                self.info.identifier,
                TransactionFlag::Abort,
            ) {
                DigestAppendResult::Ok { over_threshold } => {
                    if over_threshold {
                        self.blockchain.sync();
                    }
                    break;
                }
                DigestAppendResult::ReachedMax => {
                    self.blockchain.wait_sync().await;
                }
            }
        }
    }

    pub fn finalize_global_ops(&self, abort: bool, global_vals: &mut GlobalValuesMap) {
        let mut global_ops = self.global_ops.lock().unwrap();

        if abort {
            global_ops.abort(global_vals);
        } else {
            global_ops.commit(global_vals);
        }
    }

    pub async fn execute(&self) -> Result<TxWitness, TransactionError> {
        let (sender, receiver) = oneshot::channel();
        let now = self.info.start;

        // Make sure to drop this lock before you wait
        // on the receiver
        {
            let mut lock = self.done_sender.lock().await;

            if lock.is_some() {
                panic!("Invalid state");
            } else {
                *lock = Some(sender);
            }
        }

        let state = self.state.lock().await;
        self.issue_next_stages(state, now).await;

        if receiver.await.is_err() {
            log::error!("Failed to receive commit result. Are we shutting down?");
            return Err(TransactionError::Unexpected);
        }

        self.get_result().await
    }
}
