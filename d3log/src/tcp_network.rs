use tokio::{
    io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener, net::TcpStream, sync::Mutex,
    task::JoinHandle,
};

use crate::batch::singleton;
use crate::child::output_json;
use crate::{json_framer::JsonFramer, transact::ArcTransactionManager, Batch, Node};

use differential_datalog::ddval::DDValConvert;
use std::collections::HashMap; //, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex as SyncMutex;

async fn process_socket(_n: &TcpNetwork, mut s: TcpStream) -> Result<(), std::io::Error> {
    let mut jf = JsonFramer::new();
    let mut buffer = [0; 64];
    print!("reading");
    let k = s.read(&mut buffer).await?;
    print!("read {}", k);

    for i in jf.append(&buffer[0..k])? {
        println!("{}", i);
    }
    Ok(())
}

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

//    Pin<Box<dyn Future<Output = Result<Result<(), std::io::Error>, JoinError>> + Send + Sync + 'a>>;

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

    // nee  Result<Pin<Box<dyn Future<Output=Result<(),std::io::Error>> + Send + Sync + '_>>,

    pub fn send(self, nid: Node, b: Batch) -> Result<(), std::io::Error> {
        println!("send {}", nid);
        let p = {
            let x = &mut (*self.n.lock().expect("lock")).peers;
            x.clone()
        };
        let completion = tokio::spawn(async move {
            // sync lock in async context? - https://tokio.rs/tokio/tutorial/shared-state
            // says its ok. otherwise this gets pretty hard. it does steal a tokio thread
            // for the duration of the wait
            let encoded = serde_json::to_string(&b).expect("tcp network send json encoding error");

            // this is racy because we keep having to drop this lock across
            // await. if we lose, there will be a once used but after idle
            // connection
            match p
                .lock()
                .await
                .entry(nid)
                .or_insert(match TcpStream::connect(nid_to_socketaddr(nid)).await {
                    Ok(x) => Arc::new(Mutex::new(x)),
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

use mm_ddlog::typedefs::d3::{Connection, Workers};

// xxx - caller should get the address and send the address fact, not us
pub async fn bind(n: TcpNetwork, _t: ArcTransactionManager) -> Result<(), std::io::Error> {
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
        // xxx - socket errors shouldn't stop the listener
        process_socket(&n, socket).await?;
    }
}
