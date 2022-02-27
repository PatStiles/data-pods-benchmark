use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

#[derive(Serialize, Deserialize, Debug)]
pub enum ProxyMessage {
    BlockchainConnect {
        name: String,
        address: String,
    },
    /// Tell the data pod to create a new digest
    SyncRequest,
    /// Tell the data pod that we are connected to a new remote party
    NotifyConnect {
        /// The remote parties unique id
        identifier: u32,
        /// Are we initiating this connection?
        initiating: bool,
    },
    NotifyDisconnect {
        identifier: u32,
    },
    /// Forward data from a remote party
    /// This contains the raw bytes
    ForwardMessage {
        identifier: u32,
        payload: ByteBuf,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum EnclaveMessage {
    ForwardResponse { rp_id: u32, payload: ByteBuf },
    EnclaveReady,
    RequestConnect { address: String },
}
