mod chunks;
pub use chunks::TransactionChunk;

mod federated;
pub use federated::FederatedTransaction;

mod remote;
pub use remote::RemoteTransaction;

use crate::requests::TxRequestHandle;

use blockchain_simulator::protocol::EpochId;

pub type EpochLen = u32;

// Assuming message propagation takes at most one minute.
// Message needs to propagate to remote party
// + another round trip time for a potential availability wager
pub const APPLY_LEN: EpochLen = 3;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TransactionMode {
    Execute,
    Commit,
    Abort,
    Done,
}

struct TransactionState {
    current_stage: u32,
    /// # of local stages that do not need an execution window
    skipped_stages: u32,
    mode: TransactionMode,
}

impl TransactionState {
    #[inline]
    pub fn num_stages(&self) -> u32 {
        assert!(self.current_stage >= self.skipped_stages);
        (self.current_stage - self.skipped_stages) as u32
    }

    #[inline]
    pub fn execute_end(&self, start: EpochId) -> EpochId {
        start + self.num_stages() * APPLY_LEN
    }

    #[inline]
    pub fn finalize_end(&self, start: EpochId) -> EpochId {
        self.execute_end(start) + APPLY_LEN
    }
}

#[derive(Default)]
struct PendingRequests {
    has_global_op: bool,
    peer_requests: Vec<TxRequestHandle>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalize_end() {
        let state = TransactionState {
            current_stage: 5,
            skipped_stages: 4,
            mode: TransactionMode::Commit,
        };

        assert_eq!(10, state.execute_end(7));
        assert_eq!(13, state.finalize_end(7));
    }
}
