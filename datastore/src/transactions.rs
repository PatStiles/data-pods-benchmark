use crate::protocol::TxId;

use blockchain_simulator::protocol::EpochId;
use blockchain_simulator::AccountId;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransactionInfo {
    pub source_account: AccountId,
    pub source_location: AccountId,
    pub identifier: TxId,
    pub application: String,
    pub fn_name: String,
    pub start: EpochId,
}

impl TransactionInfo {
    pub fn new(
        source_account: AccountId,
        source_location: AccountId,
        identifier: TxId,
        application: String,
        fn_name: String,
        start: EpochId,
    ) -> Self {
        Self {
            source_account,
            source_location,
            identifier,
            application,
            fn_name,
            start,
        }
    }
}
