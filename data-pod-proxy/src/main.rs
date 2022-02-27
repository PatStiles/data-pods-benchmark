use tokio::net::TcpListener;
use tokio::runtime;
use tokio::sync::Mutex;

use data_pods_utils::parse_address;

use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{command, Arg};

mod remote_parties;
use remote_parties::{RemoteParties, RemotePartyConnection};

mod datapod;
use datapod::{DatapodConnection, DatapodConnections, SyncConnection};

use data_pods_store::{DEFAULT_ENCLAVE_PORT, DEFAULT_PROXY_PORT};
use data_pods_utils::enclave_protocol::ProxyMessage;

#[cfg(feature = "enable-profiling")]
use cpuprofiler::PROFILER;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    let default_port_str = DEFAULT_ENCLAVE_PORT.to_string();

    let arg_matches = command!()
        .arg(
            Arg::new("listen_address")
                .takes_value(true)
                .long("listen")
                .short('l')
                .help("The address to listen for client connections")
                .default_value("localhost"),
        )
        .arg(
            Arg::new("blockchain_address")
                .takes_value(true)
                .long("blockchain_address")
                .short('b')
                .help("The address of the blockchain server to connect to")
                .default_value("localhost"),
        )
        .arg(
            Arg::new("server_name")
                .takes_value(true)
                .long("server_name")
                .required(true),
        )
        .arg(
            Arg::new("enclave_port")
                .takes_value(true)
                .long("enclave_port")
                .default_value(&default_port_str),
        )
        .get_matches();

    // Make sure we have at least 16 threads
    // otherwise execution will be serial...
    let num_threads = num_cpus::get().max(16);

    let rt = runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .worker_threads(num_threads)
        .build()?;

    #[cfg(feature = "enable-profiling")]
    PROFILER
        .lock()
        .unwrap()
        .start("./data-pod-proxy.profile")
        .unwrap();

    let num_workers = num_cpus::get().max(4);

    let blockchain_address = arg_matches
        .value_of("blockchain_address")
        .unwrap()
        .to_string();
    let listen_address = arg_matches.value_of("listen_address").unwrap().to_string();
    let enclave_port = arg_matches
        .value_of("enclave_port")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let server_name = arg_matches.value_of("server_name").unwrap().to_string();

    let remote_parties = Arc::new(RemoteParties::new());
    let db_conns = Arc::new(Mutex::new(DatapodConnections::new()));

    let rps = remote_parties.clone();
    let dbs = db_conns.clone();

    let sync_connection = rt.block_on(async move {
        // Retry until the data pod is ready
        let start = Instant::now();

        loop {
            match SyncConnection::new(
                enclave_port,
                blockchain_address.clone(),
                server_name.clone(),
                rps.clone(),
                dbs.clone(),
            )
            .await
            {
                Ok(conn) => {
                    return conn;
                }
                Err(()) => {
                    // Give up after two minutes
                    let elapsed = Instant::now() - start;
                    if elapsed > Duration::from_secs(120) {
                        panic!("Failed to connect to data pod");
                    }

                    let dur = Duration::from_millis(200);
                    tokio::time::sleep(dur).await;
                }
            }
        }
    });

    // Wait until the enclave is connected to the blockchain network and set up
    sync_connection.wait_enclave_ready();

    let rp_clone = remote_parties.clone();
    let db_clone = db_conns.clone();

    ctrlc::set_handler(move || {
        log::info!("Got Ctrl-C. Shutting down data pod proxy");

        #[cfg(feature = "enable-profiling")]
        PROFILER.lock().unwrap().stop().unwrap();

        exit(0);
    })
    .unwrap();

    rt.block_on(async move {
        // spawn multiple connection threads
        for _ in 0..num_workers {
            let conn =
                DatapodConnection::new(enclave_port, rp_clone.clone(), db_clone.clone()).await;
            db_clone.lock().await.push(Arc::new(conn));
        }

        log::info!("Started {num_workers} worker tasks");

        let addr = parse_address(&listen_address, DEFAULT_PROXY_PORT);
        log::info!("Listening for client connections on {addr}");

        let listener = TcpListener::bind(&addr)
            .await
            .expect("Failed to bind socket!");

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    let identifier = remote_parties.get_next_id();

                    log::info!("Got connection #{identifier} from {addr}");

                    let pos = (identifier as usize) % num_workers;
                    let db_conn = db_conns.lock().await[pos].clone();

                    let conn = RemotePartyConnection::new(
                        socket,
                        identifier,
                        db_conn.clone(),
                        remote_parties.clone(),
                    )
                    .await;

                    remote_parties.insert(identifier, conn.clone()).await;

                    let msg = ProxyMessage::NotifyConnect {
                        identifier,
                        initiating: false,
                    };
                    db_conn.forward_message(msg).await;
                }
                Err(err) => {
                    log::error!("Failed to accept new connection: {err}");
                }
            }
        }
    });

    #[cfg(feature = "enable-profiling")]
    PROFILER.lock().unwrap().stop().unwrap();

    Ok(())
}
