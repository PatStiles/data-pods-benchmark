use crate::applications::ApplicationRegistry;
use crate::blockchain::BlockchainConnection;
use crate::enclave::Enclave;
use crate::remote_parties::{RemoteParties, RemoteParty};

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::exit;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{Decoder, Encoder};

use bytes::BytesMut;

use data_pods_store::Datastore;
use data_pods_utils::enclave_protocol::{EnclaveMessage, ProxyMessage};

use tokio::runtime::Runtime as TokioRuntime;

pub struct ProxyOutput {
    msg_queue: Mutex<VecDeque<EnclaveMessage>>,
    msg_cond: Condvar,
}

pub struct ProxyConnection {
    enclave: Arc<Enclave>,
    applications: Arc<ApplicationRegistry>,
    datastore: Arc<Datastore>,
    blockchain: Arc<BlockchainConnection>,
    remote_parties: Arc<RemoteParties>,
    read_stream: Mutex<TcpStream>,
    output: Arc<ProxyOutput>,
    tokio_runtime: Arc<TokioRuntime>,
}

impl ProxyConnection {
    pub fn new(
        enclave: Arc<Enclave>,
        applications: Arc<ApplicationRegistry>,
        datastore: Arc<Datastore>,
        blockchain: Arc<BlockchainConnection>,
        remote_parties: Arc<RemoteParties>,
        socket: TcpStream,
        tokio_runtime: Arc<TokioRuntime>,
    ) -> Self {
        socket.set_nodelay(true).unwrap();

        let out_socket = socket.try_clone().unwrap();
        let read_stream = Mutex::new(socket);
        let output = Arc::new(ProxyOutput::new());

        let o2 = output.clone();
        thread::spawn(move || {
            o2.send_loop(out_socket);
        });

        Self {
            enclave,
            applications,
            datastore,
            blockchain,
            read_stream,
            output,
            remote_parties,
            tokio_runtime,
        }
    }

    fn handle_message(&self, msg: ProxyMessage) {
        match msg {
            ProxyMessage::BlockchainConnect { name, address } => {
                self.blockchain.connect(&address);

                log::trace!("Data pod got name set to \"{name}\"");

                // Start receive loop once we know our name
                let bcpy = self.blockchain.clone();
                bcpy.set_name(name);
                thread::spawn(move || {
                    bcpy.run();
                });

                let msg = EnclaveMessage::EnclaveReady {};
                self.send(msg);
            }
            ProxyMessage::SyncRequest {} => {
                self.blockchain.sync();
            }
            ProxyMessage::ForwardMessage {
                identifier,
                payload,
            } => {
                let rp = self.remote_parties.get_by_id(identifier);
                rp.handle_messages(&payload);
            }
            ProxyMessage::NotifyConnect {
                identifier,
                initiating: _,
            } => {
                let rp = Arc::new(RemoteParty::new(
                    identifier,
                    self.applications.clone(),
                    self.datastore.clone(),
                    self.blockchain.clone(),
                    self.remote_parties.clone(),
                    self.output.clone(),
                    self.enclave.clone(),
                    self.tokio_runtime.clone(),
                ));

                self.remote_parties.register(identifier, rp.clone());
                rp.initialize(self.blockchain.get_location());
            }
            ProxyMessage::NotifyDisconnect { identifier } => {
                self.remote_parties.unregister(identifier);
            }
        }
    }

    pub fn send(&self, msg: EnclaveMessage) {
        self.output.send(msg);
    }

    pub fn run(&self) {
        let mut buffer = BytesMut::new();
        let mut codec = LengthDelimitedCodec::new();

        while self.enclave.is_okay() {
            {
                let mut data = [0; 1024];

                let len = match self.read_stream.lock().unwrap().read(&mut data) {
                    Ok(l) => l,
                    Err(e) => {
                        log::error!("failed to read from socket: {}", e);
                        exit(0);
                    }
                };

                if len > 0 {
                    buffer.extend_from_slice(&data[0..len]);
                } else {
                    log::info!("Lost connection to proxy. Shutting down...");
                    exit(0);
                }
            }

            loop {
                match codec.decode(&mut buffer) {
                    Ok(Some(data)) => {
                        let msg = bincode::deserialize(&data).expect("Parse ProxyMessage");
                        self.handle_message(msg);
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        log::warn!("Failed to decode from socket; error = {:?}", e);
                    }
                }
            }
        }
    }
}

impl ProxyOutput {
    pub fn new() -> Self {
        Self {
            msg_queue: Mutex::new(VecDeque::new()),
            msg_cond: Condvar::new(),
        }
    }

    pub fn send(&self, msg: EnclaveMessage) {
        let mut msg_queue = self.msg_queue.lock().unwrap();
        msg_queue.push_back(msg);
        self.msg_cond.notify_one();
    }

    fn send_loop(&self, mut socket: TcpStream) {
        let mut codec = LengthDelimitedCodec::new();

        loop {
            let msg = {
                let mut msg_queue = self.msg_queue.lock().unwrap();

                while msg_queue.is_empty() {
                    msg_queue = self.msg_cond.wait(msg_queue).unwrap();
                }

                msg_queue.pop_front().unwrap()
            };

            let binmsg = bincode::serialize(&msg).expect("Serialize EnclaveMessage");

            let mut msg_data = BytesMut::new();
            codec
                .encode(binmsg.into(), &mut msg_data)
                .expect("Encode EnclaveMessage");

            match socket.write_all(&msg_data) {
                Ok(()) => {}
                Err(e) => {
                    log::error!("Failed to send message to proxy: {}", e);
                    return;
                }
            }
        }
    }
}
