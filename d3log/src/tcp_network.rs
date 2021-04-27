// provide a tcp implementation of Transport
// depends on a relational table in d3.tl - TcpAddress(x: D3logLocationId, destination: string)
// to provide location id to tcp address mapping
//
// do not currently attempt to use a single duplex connection between two nodes, instead
// running one in each direction
//
// there is a likely race when a second send comes in before the connection attempt started
// by the first has completed, resulting in a dead connection

use tokio::{
    io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener, net::TcpStream, sync::Mutex,
    task::JoinHandle,
};

use mm_ddlog::typedefs::d3::{Connection, Workers};

use crate::batch::singleton;
use crate::child::output_json;
use crate::{json_framer::JsonFramer, transact::ArcTransactionManager, Batch, Node, Transport};

use differential_datalog::ddval::DDValConvert;
use std::collections::HashMap; //, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex as SyncMutex;


// to be destroyed
pub fn address_to_nid(peer: SocketAddr) -> Node {
    let i = match peer.ip() {
        IpAddr::V4(x) => x.octets(),
        IpAddr::V6(_) => return 0,
    };
    let p = peer.port().to_be_bytes();

    // prefer big endian for generic reasons, but we are travelling through
    // a constricted u64 pipe
    u128::from_le_bytes([
        i[0], i[1], i[2], i[3], p[0], p[1], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ])
}

pub fn nid_to_socketaddr(n: Node) -> SocketAddr {
    let b = n.to_le_bytes();
    SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(b[0], b[1], b[2], b[3])),
        u16::from_be_bytes([b[4], b[5]]),
    )
}

#[derive(Clone)]
pub struct TcpNetwork {
    peers: Arc<Mutex<HashMap<Node, Arc<Mutex<TcpStream>>>>>,
    sends: Arc<SyncMutex<Vec<JoinHandle<Result<(), std::io::Error>>>>>, // ;JoinHandle<()>>>>,
}

// we wanted to make a network class, that would have a send function, and
// that would allow us to encapsulate quite a bit about our environment.
//
// there is an async_trait macro that promises to do this, but unsurprisingly
// MutexGuard does not implement Send. So its unclear how to let such
// a trait function mutate. punting.
impl TcpNetwork {
    pub fn new() -> TcpNetwork {
        TcpNetwork {
            peers: Arc::new(Mutex::new(HashMap::new())),
            sends: Arc::new(SyncMutex::new(Vec::new())),
        }
    }
}

#[derive(Clone)]
pub struct ArcTcpNetwork {
    n: Arc<SyncMutex<TcpNetwork>>,
}

impl ArcTcpNetwork {
    pub fn new() -> ArcTcpNetwork {
        ArcTcpNetwork {
            n: Arc::new(SyncMutex::new(TcpNetwork::new())),
        }
    }

    // xxx - caller should get the address and send the address fact, not us
    pub async fn bind(&self, _t: ArcTransactionManager) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;

        let a = listener.local_addr().unwrap();
        let nid = address_to_nid(a);
        let w = Workers { x: nid }.into_ddvalue();
        output_json(&(singleton("d3::Workers", &w)?)).await?;
        loop {
            let (socket, a) = listener.accept().await?;

            let c = Connection {
                x: address_to_nid(a),
            }
            .into_ddvalue();
            output_json(&(singleton("d3::Connection", &c)?)).await?;

            // well, this is a huge .. something. if i just use the socket
            // in the async move block, it actually gets dropped. what
            // happened to ultimate safety?
            let sclone = Arc::new(Mutex::new(socket));
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
                                let v: Batch = match serde_json::from_str(&i) {
                                    Ok(x) => x,
                                    Err(x) => panic!(x),
                                };
                                println!("{}", v);
                            }
                        }
                        Err(x) => panic!("read error {}", x),
                    }
                }
            });
        }
    }
}

// should this be one instance per transport type, or one per peer - seems
// like the latter really
impl Transport for ArcTcpNetwork {
    fn send(&self, nid: Node, b: Batch) -> Result<(), std::io::Error> {
        let p = {
            let x = &mut (*self.n.lock().expect("lock")).peers;
            x.clone()
        };
        let completion = tokio::spawn(async move {
            // sync lock in async context? - https://tokio.rs/tokio/tutorial/shared-state
            // says its ok. otherwise this gets pretty hard. it does steal a tokio thread
            // for the duration of the wait
            let encoded = serde_json::to_string(&b).expect("tcp network send json encoding error");
            println!("send {} {} {}", nid, b, encoded.chars().count());

            // this is racy because we keep having to drop this lock across
            // await. if we lose, there will be a once used but after idle
            // connection
            match p
                .lock()
                .await
                .entry(nid)
                .or_insert(match TcpStream::connect(nid_to_socketaddr(nid)).await {
                    Ok(x) => {
                        Arc::new(Mutex::new(x))
                    }
                    Err(_x) => panic!("connection failure {}", nid_to_socketaddr(nid)),
                })
                .lock()
                .await
                .write_all(&encoded.as_bytes())
                .await
            {
                Ok(_) => Ok(()),
                // these need to get directed to a retry machine and an async reporting relation
                Err(x) => panic!("send error {}", x),
            }
        });
        (*self.n.lock().expect("lock").sends.lock().expect("lock")).push(completion);
        Ok(())
    }
}
