use serde::{Deserialize, Serialize};

use crate::merkle::{MerkleHash, MerkleProof};
use crate::objects::{ObjectId, ObjectTypeId};
use crate::transactions::TransactionInfo;
use crate::values::Value;
use crate::{AppDefinition, DatastoreError, DigestBatch};

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

use blockchain_simulator::AccountId;

pub type OpId = u32;
pub type TxId = u64;

pub type GetResult = Result<Value, DatastoreError>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxWitness {
    pub result: Value,

    // Tx inclusion proof does not exist for read-only transactions
    pub proof: Option<MerkleProof>, //TODO add client request

                                    //TODO add data pod signature
}

pub type CallResult = Result<TxWitness, TransactionError>;

pub type TxResultValues = Vec<(String, Value)>;
pub type TxResult = Result<(TxResultValues, MerkleProof), TransactionError>;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
pub enum TransactionFlag {
    Reserve,
    Commit,
    Abort,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TransactionError {
    StaleRead,
    AccessControlFailure { object_id: ObjectId },
    LockContention { oid: ObjectId, path: Vec<String> },
    InvalidArguments,
    NoSuchTx,
    NoResponse,
    Unexpected,
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self {
            TransactionError::StaleRead => {
                write!(f, "Stale read")
            }
            TransactionError::InvalidArguments => {
                write!(f, "Invalid arguments")
            }
            TransactionError::AccessControlFailure { object_id } => {
                write!(f, "Access control on object {:?} failed", object_id)
            }
            TransactionError::LockContention { oid, path } => {
                if path.is_empty() {
                    write!(f, "Lock contention at object {:?}", oid)
                } else {
                    write!(f, "Lock contention at field {:?}:{:?}", oid, path)
                }
            }
            TransactionError::NoSuchTx => {
                write!(f, "No such transaction")
            }
            TransactionError::NoResponse => {
                write!(f, "Got no response")
            }
            TransactionError::Unexpected => {
                write!(f, "Unexpected error")
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Read {
        path: Vec<String>,
    },
    Update {
        arg: Value,
    },
    Create {
        application: String,
        owner: AccountId,
        typeid: ObjectTypeId,
        fields: HashMap<String, Value>,
    },
    ListAppend {
        path: Vec<String>,
        arg: Value,
    },
    MapInsert {
        path: Vec<String>,
        arg: Value,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    GetRequest {
        op_id: OpId,
        obj_id: ObjectId,
    },
    CallRequest {
        op_id: OpId,
        application: String,
        key: String,
        args: Vec<Value>,
    },
    CallResponse {
        op_id: OpId,
        result: CallResult,
    },
    GetResponse {
        op_id: OpId,
        result: GetResult,
    },
    TxLockRequest {
        op_id: OpId,
        tx_info: TransactionInfo,
        stage: u32,
        calls: Vec<usize>,
        meta_vars: HashMap<String, Value>,
    },
    TxPrepareCommit {
        op_id: OpId,
        tx_info: TransactionInfo,
        calls: Vec<usize>,
        global_vars: HashMap<String, Value>,
    },
    TxFinalizeRequest {
        op_id: OpId,
        tx_id: TxId,
        abort: bool,
    },
    TxResponse {
        op_id: OpId,
        tx_id: TxId,
        result: TxResult,
    },
    SendLocation {
        location: AccountId,
    },
    ConnectionEstablished {},
    PeerConnect {
        address: String,
    },
    SendDigestBatch {
        position: usize,
        batch: DigestBatch,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LedgerMessage {
    // Op Batching
    SyncState {
        position: usize,
        state_hash: MerkleHash,
        transaction_hash: MerkleHash,
        reservation_hash: MerkleHash,
    },

    // Application management
    CreateApplication {
        name: String,
        definition: AppDefinition,
    },

    // Federated Transactions
    ExecuteTxChunk {
        info: TransactionInfo,
        stage: u32,
        func_id: usize,
        meta_vars: HashMap<String, Value>,
    },

    FinalizeTransaction {
        abort: bool,
        info: TransactionInfo,
    },
}
