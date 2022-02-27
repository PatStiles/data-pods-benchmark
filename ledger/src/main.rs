use log::info;

use std::sync::Arc;

use blockchain_simulator::server::main_thread as blockchain_main_thread;
use blockchain_simulator::server::Callback as BlockchainServerCallback;

use data_pods_store::protocol::LedgerMessage;

pub type BlockchainTransaction = blockchain_simulator::Transaction<LedgerMessage>;

struct LedgerCallback {}

impl BlockchainServerCallback<LedgerMessage> for LedgerCallback {
    fn validate_transaction(&self, _: &BlockchainTransaction) -> bool {
        //FIXME actually validate transactions here
        true
    }

    fn notify_new_transaction(&self, _: &BlockchainTransaction) {}
}

fn main() {
    pretty_env_logger::init();

    let num_threads = 2 * num_cpus::get();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads)
        .enable_io()
        .enable_time()
        .build()
        .expect("Failed to start worker threads");

    info!("Started {num_threads} worker threads");

    let callback = Arc::new(LedgerCallback {});

    rt.block_on(async move {
        blockchain_main_thread::<LedgerMessage>(callback).await;
    });
}
