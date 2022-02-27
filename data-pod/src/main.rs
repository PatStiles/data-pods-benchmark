mod applications;
mod blockchain;
mod enclave;
mod proxy;
mod remote_parties;
mod requests;
mod transactions;

#[cfg(feature = "enable-ctrlc")]
use ctrlc;

use std::env;
use std::net::{SocketAddr, TcpListener};
use std::process::exit;
use std::sync::Arc;
use std::thread;
use std::vec::Vec;

use blockchain_simulator::Ledger;

use data_pods_store::{Datastore, DEFAULT_ENCLAVE_PORT};

use crate::applications::ApplicationRegistry;
use crate::blockchain::BlockchainConnection;
use crate::enclave::Enclave;
use crate::proxy::ProxyConnection;
use crate::remote_parties::RemoteParties;

use tokio::runtime;

#[cfg(feature = "enable-profiling")]
use cpuprofiler::PROFILER;

fn main() {
    //TODO add a flag to disable backtraces
    std::env::set_var("RUST_BACKTRACE", "1");

    #[cfg(not(target_env = "sgx"))]
    pretty_env_logger::init();

    log::info!("Started data pod");

    #[cfg(not(feature = "use-tls"))]
    log::warn!("TLS is disabled. Only use this for testing!");

    let args: Vec<String> = env::args().collect();
    let addr_str = if args.len() < 2 {
        format!("127.0.0.1:{DEFAULT_ENCLAVE_PORT}")
    } else {
        format!("127.0.0.1:{}", &args[1])
    };

    let addr = addr_str.parse::<SocketAddr>().unwrap();

    let enclave = Arc::new(Enclave::new());

    let ledger = Arc::new(Ledger::default());

    let remote_parties = Arc::new(RemoteParties::new());

    let database = Arc::new(Datastore::default());

    let applications = Arc::new(ApplicationRegistry::new(
        enclave.clone(),
        database.clone(),
        remote_parties.clone(),
    ));

    #[cfg(not(target_env = "sgx"))]
    let num_cpu_cores = num_cpus::get();
    #[cfg(target_env = "sgx")]
    let num_cpu_cores = 12;

    // Make sure we have at least 4 threads
    // otherwise execution will be serial...
    let num_tokio_threads = num_cpu_cores.max(4);

    let tokio_runtime = Arc::new(
        runtime::Builder::new_multi_thread()
            .enable_time()
            .worker_threads(num_tokio_threads)
            .build()
            .expect("Failed to create tokio runtime"),
    );

    #[cfg(not(target_env = "sgx"))]
    log::info!(
        "Detected {num_cpu_cores} logical cpu cores and started {num_tokio_threads} tokio threads."
    );
    #[cfg(target_env = "sgx")]
    log::info!("Cannot detect number of logical cpu cores due to SGX. Started {num_tokio_threads} tokio threads.");

    let blockchain_conn = Arc::new(BlockchainConnection::new(
        enclave.clone(),
        ledger,
        database.clone(),
        applications.clone(),
        remote_parties.clone(),
        tokio_runtime.clone(),
    ));

    let mut threads = Vec::<thread::JoinHandle<()>>::new();

    let listener = match TcpListener::bind(&addr) {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to bind client socket: {}", e);
            exit(-1);
        }
    };

    // Start profiling after we generated the private key
    // (that can take long...)
    #[cfg(feature = "enable-profiling")]
    {
        let mut profiler = PROFILER.lock().unwrap();
        let fname = format!("data-pod_{}.profile", std::process::id());
        profiler
            .start(fname.clone())
            .expect("Failed to start profiler");

        log::info!("CPU profiler enabled. Writing output to '{}'", fname);
    }

    #[cfg(feature = "enable-ctrlc")]
    ctrlc::set_handler(move || {
        log::info!("Got Ctrl-C. Shutting down data pod...");

        #[cfg(feature = "enable-profiling")]
        {
            let mut profiler = PROFILER.lock().unwrap();
            profiler.stop().expect("Failed to stop profiler");
        }

        exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    log::info!("Listening for proxy connections at {addr_str}");

    let mut next_id = 0;

    while enclave.is_okay() {
        match listener.accept() {
            Ok((conn, peer_addr)) => {
                log::info!("Got proxy connection #{next_id} from {peer_addr}");

                let proxy_conn = Arc::new(ProxyConnection::new(
                    enclave.clone(),
                    applications.clone(),
                    database.clone(),
                    blockchain_conn.clone(),
                    remote_parties.clone(),
                    conn,
                    tokio_runtime.clone(),
                ));

                next_id += 1;

                enclave.register_proxy_connection(proxy_conn.clone());

                threads.push(thread::spawn(move || {
                    proxy_conn.run();
                }));
            }
            Err(e) => {
                log::error!("Failed to accept connection: {:?}", e);
            }
        }
    }

    for thread in threads {
        let res = thread.join();

        if let Err(e) = res {
            log::error!("Worker thread panicked: {:?}", e);
        }
    }

    #[cfg(feature = "enable-profiling")]
    {
        let mut profiler = PROFILER.lock().unwrap();
        profiler.stop().expect("Failed to stop profiler");
    }

    log::info!("Data pod terminated");
}
