use pyo3::prelude::*;
use pyo3::{wrap_pyfunction, PyResult};

use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpStream as StdTcpStream;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tokio::runtime::Builder as TokioRuntimeBuilder;
use tokio::runtime::Runtime as TokioRuntime;

use blockchain_simulator::protocol::EpochId;
use blockchain_simulator::{AccountId, DEFAULT_BLOCKCHAIN_PORT};

use data_pods_store::protocol::{LedgerMessage, Message};
use data_pods_store::values::Value;
use data_pods_store::{compile_app_definition, DEFAULT_PROXY_PORT};

pub use data_pods_store::objects::{ObjectId, ObjectUid};

use data_pods_utils::parse_address;

mod requests;
use requests::{CallRequestHandle, GetRequestHandle};

mod blockchain;
mod data_pod;

use blockchain::BlockchainConnection;
use data_pod::DataPodConnection;

#[pyclass]
pub struct BatchStatistics {
    pub identifier: usize,
    pub aborts: usize,
    pub commits: usize,
    pub reservations: usize,
}

#[pyclass]
pub struct Statistics {
    elapsed: u32,
    committed_txs: usize,
    aborted_txs: usize,
    reservations: usize,
    total_ops: usize,

    batches: Vec<BatchStatistics>,
}

#[pymethods]
impl Statistics {
    pub fn elapsed_time(&self) -> u32 {
        self.elapsed
    }

    pub fn num_committed_txs(&self) -> usize {
        self.committed_txs
    }

    pub fn num_aborted_txs(&self) -> usize {
        self.aborted_txs
    }

    pub fn num_reservations(&self) -> usize {
        self.reservations
    }

    pub fn total_operations(&self) -> usize {
        self.total_ops
    }

    pub fn has_more_batches(&self) -> bool {
        !self.batches.is_empty()
    }

    pub fn get_next_batch(&mut self) -> BatchStatistics {
        self.batches.pop().unwrap()
    }
}

#[pyclass]
pub struct Client {
    // We need to hold this reference so the runtime does not terminate
    tokio_rt: TokioRuntime,

    blockchain: Arc<BlockchainConnection>,
    data_pod: Arc<DataPodConnection>,
    next_op_id: AtomicU32,
}

#[pymethods]
impl Client {
    pub fn call(&mut self, application: String, key: String, args: Vec<Value>) -> PyResult<Value> {
        let hdl = self.call_async(application, key, args);
        hdl.wait();
        hdl.get_result()
    }

    pub fn call_async(
        &mut self,
        application: String,
        key: String,
        args: Vec<Value>,
    ) -> CallRequestHandle {
        let op_id = self.next_op_id.fetch_add(1, Ordering::SeqCst);

        let request = Message::CallRequest {
            op_id,
            application,
            key,
            args,
        };
        let handle = CallRequestHandle::new();

        self.data_pod
            .insert_call_request(op_id, handle.clone_result());

        self.tokio_rt.block_on(async {
            self.data_pod.send(request).await;
        });

        handle
    }

    // FIXME add object-bindings for python
    pub fn get(&mut self, obj_id: Value) -> PyResult<Value> {
        let hdl = self.get_async(obj_id);
        hdl.wait();

        hdl.get_result()
    }

    pub fn get_async(&mut self, obj_id: Value) -> GetRequestHandle {
        let op_id = self.next_op_id.fetch_add(1, Ordering::SeqCst);
        let oid: ObjectId = obj_id.try_into().unwrap();

        let request = Message::GetRequest { op_id, obj_id: oid };
        let handle = GetRequestHandle::new();

        self.data_pod
            .insert_get_request(op_id, handle.clone_result());

        self.tokio_rt.block_on(async {
            self.data_pod.send(request).await;
        });

        handle
    }

    //TODO wait for response
    pub fn peer_connect(&mut self, address: String) {
        let request = Message::PeerConnect { address };

        self.tokio_rt.block_on(async {
            self.data_pod.send(request).await;
        });
    }

    pub fn create_application(&mut self, name: String, filename: &str) {
        let file = match File::open(filename) {
            Ok(f) => f,
            Err(err) => panic!("Failed to open file {filename}: {err:?}"),
        };

        let mut buf_reader = BufReader::new(file);
        let mut code = String::new();
        buf_reader.read_to_string(&mut code).unwrap();

        let definition = compile_app_definition(&code);

        log::trace!("Sending request to create application \"{name}\"");

        self.tokio_rt.block_on(async {
            let request = LedgerMessage::CreateApplication { name, definition };
            self.blockchain.send(request).await;
        });
    }

    pub fn get_remote_location(&mut self) -> AccountId {
        let dp = self.data_pod.clone();

        self.tokio_rt
            .block_on(async move { dp.get_location().await })
    }

    pub fn close(&mut self) {
        log::info!("Closing client connection");

        self.tokio_rt.block_on(async {
            self.blockchain.disconnect().await;
            self.data_pod.disconnect().await;
        });

        log::trace!("Client shutdown done");
    }

    pub fn get_current_blockchain_epoch(&self) -> EpochId {
        let ledger = self.blockchain.get_ledger();
        ledger.get_current_epoch()
    }

    pub fn get_statistics(&mut self, start: EpochId, end: EpochId) -> Statistics {
        let bc = self.blockchain.clone();
        let dp = self.data_pod.clone();

        self.tokio_rt.block_on(async move {
            let (dstart, dend) = bc
                .get_digest_start_end(dp.get_location().await, start, end)
                .await;
            let ledger = bc.get_ledger();

            let elapsed =
                (ledger.get_epoch_timestamp(end) - ledger.get_epoch_timestamp(start)) as u32;

            let mut stats = dp.get_statistics(dstart, dend).await;

            // Python does not like hash map
            let mut batches = Vec::new();
            for (identifier, val) in stats.batches.drain() {
                let batch = BatchStatistics {
                    identifier,
                    commits: val.commits,
                    aborts: val.aborts,
                    reservations: val.reservations,
                };

                batches.push(batch);
            }

            Statistics {
                elapsed,
                committed_txs: stats.committed_txs,
                aborted_txs: stats.aborted_txs,
                reservations: stats.reservations,
                total_ops: stats.total_ops,
                batches,
            }
        })
    }
}

impl Client {
    pub fn new(identifier: u32, server_address: &str, blockchain_address: &str) -> Self {
        if pretty_env_logger::try_init().is_err() {
            log::warn!("Failed to initialize logger; is one already running?");
        }

        let tokio_rt = TokioRuntimeBuilder::new_multi_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("Initialize tokio runtime");

        let (data_pod, blockchain) = tokio_rt.block_on(async move {
            let dp_addr = parse_address(server_address, DEFAULT_PROXY_PORT);
            let bc_addr = parse_address(blockchain_address, DEFAULT_BLOCKCHAIN_PORT);

            let dp_stream = match StdTcpStream::connect(&dp_addr) {
                Ok(s) => s,
                Err(err) => panic!("Failed to connect to data pod at {dp_addr}: {err}"),
            };

            let bc_stream = match StdTcpStream::connect(&bc_addr) {
                Ok(s) => s,
                Err(err) => panic!("Failed to connect to blockchain at {bc_addr}: {err}"),
            };

            dp_stream.set_nodelay(true).unwrap();
            bc_stream.set_nodelay(true).unwrap();

            let data_pod = DataPodConnection::new(dp_stream).await;
            data_pod.wait_location_ready().await;

            let blockchain = BlockchainConnection::new(identifier, bc_stream).await;

            // once we have the location we know the account id
            // associated with this data pod
            let loc = data_pod.get_location().await;
            blockchain.register_data_pod(loc, data_pod.clone()).await;

            // Wait until we have synchronized up to a recent block
            blockchain.wait_ready().await;
            log::debug!("Synchronized with blockchain");

            data_pod.send_location(blockchain.get_account_id()).await;

            // Make sure we have the corresponding digest for all batches recorded on the
            // blockchain
            data_pod.wait_ready().await;
            log::debug!("Synchronized with data pod");

            (data_pod, blockchain)
        });

        let next_op_id = AtomicU32::new(1);

        Self {
            data_pod,
            blockchain: Arc::new(blockchain),
            next_op_id,
            tokio_rt,
        }
    }
}

#[pyfunction]
pub fn create_client(identifier: u32, server_address: &str, blockchain_address: &str) -> Client {
    Client::new(identifier, server_address, blockchain_address)
}

#[pymodule]
fn data_pods(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_wrapped(wrap_pyfunction!(create_client))?;

    Ok(())
}
