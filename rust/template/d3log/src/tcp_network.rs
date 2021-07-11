// provide a tcp implementation of Transport
// depends on a relational table in d3.tl - TcpAddress(x: D3logLocationId, destination: string)
// to provide location id to tcp address mapping
//
// do not currently attempt to use a single duplex connection between two nodes, instead
// running one in each direction
//
// there is a likely race when a second send comes in before the connection attempt started
// by the first has completed, resulting in a dead connection
//
// put a queue on the TcpPeer to allow for misordering wrt TcpAddress and to cover setup

use tokio::{
    io::AsyncReadExt,
    io::AsyncWriteExt,
    net::TcpListener,
    net::TcpStream,
    sync::Mutex,
    //    task::JoinHandle,
};

use crate::{
    async_error, fact, json_framer::JsonFramer, Batch, DDValueBatch, Dispatch, Error, Evaluator,
    Forwarder, Node, Port, RecordBatch, Transport,
};

use differential_datalog::record::*;
use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;
//use tokio::task::JoinHandle;

struct AddressListener {
    eval: Evaluator,
    forwarder: Arc<Forwarder>,
    management: Port,
}

impl Transport for AddressListener {
    fn send(&self, b: Batch) {
        for (_r, v, _w) in &RecordBatch::from(self.eval.clone(), b) {
            if let Some(destination) = v.get_struct_field("destination") {
                if let Some(location) = v.get_struct_field("location") {
                    match destination {
                        // what are the unused fields?
                        Record::String(string) => {
                            let address = string.parse();
                            let loc: u128 =
                                async_error!(self.management, FromRecord::from_record(&location));
                            // we add an entry to forward this nid to this tcp address
                            self.forwarder.register(
                                loc,
                                Arc::new(TcpPeer {
                                    management: self.management.clone(),
                                    tcp_inner: Arc::new(Mutex::new(TcpPeerInternal {
                                        eval: self.eval.clone(),
                                        management: self.management.clone(),
                                        stream: None,
                                        address: address.unwrap(), //async error
                                                                   // sends: Vec::new(),
                                    })),
                                }),
                            );
                            return;
                        }
                        _ => async_error!(
                            self.management.clone(),
                            Err(Error::new(format!(
                                "bad tcp address {}",
                                destination.to_string()
                            )))
                        ),
                    }
                }
            }

            async_error!(
                self.management.clone(),
                Err(Error::new("ill formed process".to_string()))
            );
        }
    }
}

pub async fn tcp_bind(
    dispatch: Arc<Dispatch>,
    me: Node,
    forwarder: Arc<Forwarder>,
    data: Port,
    eval: Evaluator, // evaluator is a data port, or a management port?
    management: Port,
) -> Result<(), Error> {
    dispatch.register(
        "d3_application::TcpAddress",
        Arc::new(AddressListener {
            eval: eval.clone(),
            forwarder,
            management: management.clone(),
        }),
    )?;

    // xxx get external  ip address
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr().unwrap();

    management.send(fact!(
        d3_application::TcpAddress,
        location => me.into_record(),
        destination => addr.to_string().into_record()));

    let eclone = eval.clone();
    loop {
        // exchange ids
        let (socket, _a) = listener.accept().await?;

        management.clone().send(fact!(
            d3_application::ConnectionStatus,
            time => eclone.clone().now().into_record(),
            me => me.into_record(),
            them => me.into_record()));

        // well, this is a huge .. something. if i just use the socket
        // in the async move block, it actually gets dropped
        let sclone = Arc::new(Mutex::new(socket));
        let dclone = data.clone();
        let mclone = management.clone();
        let eclone = eval.clone();

        tokio::spawn(async move {
            let mut jf = JsonFramer::new();
            let mut buffer = [0; 64];
            loop {
                // xxx - remove socket from peer table on error and post notification
                match sclone.lock().await.read(&mut buffer).await {
                    Ok(bytes_input) => {
                        for i in jf
                            .append(&buffer[0..bytes_input])
                            .expect("json coding error")
                        {
                            dclone.send(Batch::Value(async_error!(
                                mclone.clone(),
                                eclone.clone().deserialize_batch(i)
                            )));
                        }
                    }
                    Err(x) => panic!("read error {}", x),
                }
            }
        });
    }
}

struct TcpPeerInternal {
    address: SocketAddr,
    stream: Option<Arc<Mutex<TcpStream>>>,
    // sends: Vec<JoinHandle<Result<(), std::io::Error>>>, // these need to be waited on for memory?
    management: Port,
    eval: Evaluator,
}

// do we have anonymous structs?
#[derive(Clone)]
struct TcpPeer {
    tcp_inner: Arc<Mutex<TcpPeerInternal>>,
    management: Port,
}

impl Transport for TcpPeer {
    fn send(&self, b: Batch) {
        // we should be saving this return value in a vector of completions so
        // we can swoop back later and collect errors
        let tcp_inner_clone = self.tcp_inner.clone();
        let mgmt_clone = self.management.clone();

        tokio::spawn(async move {
            let mut tcp_peer = tcp_inner_clone.lock().await;

            if tcp_peer.stream.is_none() {
                tcp_peer.stream = match TcpStream::connect(tcp_peer.address).await {
                    Ok(x) => Some(Arc::new(Mutex::new(x))),
                    // xxx async error channel - self.management
                    Err(_x) => panic!("connection failure {}", tcp_peer.address),
                };
            };

            let eval = tcp_peer.eval.clone();
            let ddval_batch = async_error!(mgmt_clone.clone(), DDValueBatch::from(&(*eval), b));
            let bytes = async_error!(
                mgmt_clone.clone(),
                eval.clone().serialize_batch(ddval_batch)
            );
            // async error this, right?
            async_error!(
                tcp_peer.management.clone(),
                tcp_peer
                    .stream
                    .as_ref()
                    .unwrap()
                    .lock()
                    .await
                    .write_all(&bytes)
                    .await
            );
        });
    }
}
