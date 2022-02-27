use tokio::sync::oneshot;

use data_pods_store::protocol::{CallResult, TxResult};

/// There's a lot of duplicated code here because PyO3 does not seem to work well with Generics
/// TODO clean up this mess

pub struct RequestResult<ResultType: Clone> {
    cond_receiver: oneshot::Receiver<ResultType>,
}

pub type CallRequestResult = RequestResult<CallResult>;
pub type TxRequestResult = RequestResult<TxResult>;

pub struct CallRequestHandle {
    result: CallRequestResult,
}

pub struct TxRequestHandle {
    result: TxRequestResult,
}

impl<T: Clone> RequestResult<T> {
    pub fn new() -> (Self, oneshot::Sender<T>) {
        let (cond_sender, cond_receiver) = oneshot::channel();
        (Self { cond_receiver }, cond_sender)
    }
}

impl TxRequestHandle {
    pub fn new() -> (Self, oneshot::Sender<TxResult>) {
        let (result, sender) = TxRequestResult::new();
        (Self { result }, sender)
    }
}

#[allow(dead_code)]
impl CallRequestHandle {
    pub fn new() -> (Self, oneshot::Sender<CallResult>) {
        let (result, sender) = CallRequestResult::new();
        (Self { result }, sender)
    }
}

impl CallRequestHandle {
    #[allow(dead_code)]
    pub async fn get_result(self) -> CallResult {
        self.result.cond_receiver.await.unwrap()
    }
}

impl TxRequestHandle {
    pub fn try_get_result(mut self) -> Option<TxResult> {
        self.result.cond_receiver.try_recv().ok()
    }
}
