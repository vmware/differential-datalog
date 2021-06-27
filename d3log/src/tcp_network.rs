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
    async_error, fact, Batch, DDValueBatch, Dispatch, Error, Evaluator, Forwarder, JsonFramer,
    Node, Port, RecordBatch, Transport,
};

use differential_datalog::record::*;
use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;
//use tokio::task::JoinHandle;

struct AddressListener {
    e: Evaluator,
    f: Arc<Forwarder>,
    management: Port,
}

impl Transport for AddressListener {
    fn send(self: &Self, b: Batch) {
        for (_r, v, _w) in &RecordBatch::from(self.e.clone(), b) {
            // macro deconstructor - error
            if let Some(destination) = v.get_struct_field("destination") {
                if let Some(location) = v.get_struct_field("location") {
                    match destination {
                        // what are the unused fields?
                        Record::String(s) => {
                            let address = s.parse() ;
                            let loc: u128 = async_error!(self.management, FromRecord::from_record(&location));
                            // we add an entry to forward this nid to this tcp address
                            self.f.register(
                                loc,
                                Arc::new(TcpPeer {
                                    management: self.management.clone(),
                                    t: Arc::new(Mutex::new(TcpPeerInternal {
                                        e:self.e.clone(),
                                        management: self.management.clone(),
                                        s: None,
                                        address:address.unwrap(),//async error 
                                        // sends: Vec::new(),
                                    })),
                                }),
                            );
                            return;
                        }
                        _ => self.management.send(fact!(
                            Error,
                            text => Record::String(format!("bad tcp address {}", destination.to_string()))
                        )),
                    }
                }
            }

            // make a general error-fact wrapper
            self.management.send(fact!(
                Error,
                text => Record::String(format!("ill formed process"))
            ));
        }
    }
}

pub async fn tcp_bind(
    d: Dispatch,
    me: Node,
    f: Forwarder,
    data: Port,
    eval: Evaluator, // evaluator is a data port, or a management port?
    management: Port,
) -> Result<(), Error> {
    d.register(
        "d3_application::TcpAddress",
        Arc::new(AddressListener {
            e: eval.clone(),
            f: Arc::new(f),
            management: management.clone(),
        }),
    )?;

    // xxx get external  ip address
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let a = listener.local_addr().unwrap();

    management.send(fact!(
        d3_application::TcpAddress,
        location => me.into_record(),
        destination => a.to_string().into_record()));

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
                            dclone.send(Batch::DDValue(async_error!(
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
    s: Option<Arc<Mutex<TcpStream>>>,
    // sends: Vec<JoinHandle<Result<(), std::io::Error>>>, // these need to be waited on for memory?
    management: Port,
    e: Evaluator,
}

// do we have anonymous structs?
#[derive(Clone)]
struct TcpPeer {
    t: Arc<Mutex<TcpPeerInternal>>,
    management: Port,
}

impl Transport for TcpPeer {
    fn send(self: &Self, b: Batch) {
        // we should be saving this return value in a vector of completions so
        // we can swoop back later and collect errors
        let t = self.t.clone();
        let m = self.management.clone();

        tokio::spawn(async move {
            let mut sc = t.lock().await;

            if sc.s.is_none() {
                sc.s = match TcpStream::connect(sc.address).await {
                    Ok(x) => Some(Arc::new(Mutex::new(x))),
                    // xxx async error channel - self.management
                    Err(_x) => panic!("connection failure {}", sc.address),
                };
            };

            let e = sc.e.clone();
            let d = async_error!(m.clone(), DDValueBatch::from(e.clone(), b));
            let bytes = async_error!(m.clone(), e.clone().serialize_batch(d));
            // async error this, right?
            async_error!(
                sc.management.clone(),
                sc.s.as_ref().unwrap().lock().await.write_all(&bytes).await
            );
        });
    }
}
