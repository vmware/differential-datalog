use tokio::{net::TcpListener,
            net::TcpStream,
            io::AsyncReadExt,
            io::AsyncWriteExt};

use core::future::Future;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use crate::{json_framer::JsonFramer, Node, Batch, transact::TransactionManager};
use differential_datalog::ddval::DDValConvert;
use std::collections::HashMap;
use std::sync::Arc;
use std::pin::Pin;
use async_std::sync::Mutex;
use crate::child::output_json;
use crate::batch::singleton;


async fn process_socket(_n:&TcpNetwork, mut s: TcpStream) -> Result<(), std::io::Error> {
    let mut jf = JsonFramer::new();
    let mut buffer = [0; 64];
    let k = s.read(&mut buffer).await?;
    
    for i in jf.append(&buffer[0..k])? {
        println!("{}", i);
    }
    Ok(())
}


pub fn address_to_nid(peer:SocketAddr) -> Node{
    let i = match peer.ip(){
        IpAddr::V4(x) => x.octets(),
        IpAddr::V6(_) => return 0
    };
    let p = peer.port().to_be_bytes();    

    // prefer big endian for generic reasons, but we are travelling through
    // a constricted u64 pipe
    u128::from_le_bytes([i[0], i[1], i[2], i[3], p[0], p[1], 0, 0, 0, 0, 0,
                         0,0,0,0,0])
}

pub fn nid_to_socketaddr(n:Node) -> SocketAddr {
    let b = n.to_be_bytes();
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(b[0], b[1], b[2], b[3])), u16::from_be_bytes([b[4], b[5]]))
}

pub struct TcpNetwork {
    peers : Arc::<Mutex::<HashMap<Node, Arc::<Mutex::<TcpStream>>>>>
}


// we wanted to make a network class, that would have a send function, and
// that would allow us to encapsulate quite a bit about our environment.
//
// there is an async_trait macro that promises to do this, but unsurprisingly
// MutexGuard does not implement Send. So its unclear how to let such
// a trait function mutate. punting.

impl TcpNetwork {
    pub async fn soft_intern(&self, nid:Node) -> Result<Arc::<Mutex::<TcpStream>>, std::io::Error> {
        let p = &mut (*self.peers).lock().await;
        
        match p.get(&nid) {
            Some(x) => Ok((*x).clone()), 
            None => {
                let c = Arc::new(Mutex::new(TcpStream::connect(nid_to_socketaddr(nid)).await?));
                p.insert(nid, c.clone());
                Ok(c)
            }
        }
    }
        
    pub fn new () -> TcpNetwork {
        TcpNetwork{peers:Arc::new(Mutex::new(HashMap::new()))}
    }

    pub fn send(&self, nid:Node, _b:Arc::<Mutex::<Batch>>) ->
        Result<Pin<Box<dyn Future<Output=Result<(),std::io::Error>> + Send + Sync + '_>>,
               std::io::Error> {
            println!("send {}", nid);
            Ok(Box::pin(async move {
                let p = &mut (*self.peers).lock().await;
                let encoded = Vec::<u8>::new();
                // actually this creates a connection, so...kinda yeah
                let c = self.soft_intern(nid).await.expect("cant fail");
                let mut cl = c.lock().await;
                cl.write_all(&encoded).await
            }))
        }
}

use mm_ddlog::typedefs::d3::Workers;

pub async fn bind(n:&TcpNetwork, _t:Arc<Mutex<TransactionManager>>) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    
    // there is likely a more direct translation not through strings - reevalute nid
    let a = listener.local_addr().unwrap();
    let nid = address_to_nid(a);
    // should be a better contract about the name(s) of the membership relations, or more broadly
    // system defined relations

    let w = Workers{x:nid}.into_ddvalue();
    output_json(&(singleton("d3::Workers", &w)?)).await?;
    loop {
        let (socket, a) = listener.accept().await?;
        output_json(&(singleton("Connection", &(address_to_nid(a) as u64).ddvalue())?)).await?;
        // xxx - socket errors shouldn't stop the listener
        process_socket(n, socket).await?;
    }    
}


