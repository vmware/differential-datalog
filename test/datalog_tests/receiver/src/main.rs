extern crate serde_json;

use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use differential_datalog::program::{RelId, Update, Response};

use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;
use ddd_ddlog::channel::{Observable, Observer, Subscription};
use ddd_ddlog::server::{UpdatesSubscription};
use ddd_ddlog::*;

use tokio::net::{TcpStream, TcpListener};
use tokio::net::tcp::ConnectFuture;
use tokio::prelude::*;
use tokio::io;

use std::sync::{Arc, Mutex};
use std::net::{SocketAddr, Shutdown};
use std::iter;
use std::io::BufReader;

pub struct TcpReceiver{
    addr: SocketAddr,
    server: Option<Box<dyn Future<Item = (), Error = ()> + Send>>,
    listener: Option<TcpListener>,
    observer: Option<Arc<Mutex<dyn Observer<Update<Value>, String>>>>,
}

impl TcpReceiver{
    pub fn new(socket: SocketAddr) -> Self {
        TcpReceiver{
            addr: socket,
            server: None,
            listener: None,
            observer: None,
        }
    }
}

impl Observable<Update<Value>, String> for TcpReceiver {
    fn subscribe<'a>(&'a mut self, observer: Arc<Mutex<dyn Observer<Update<Value>, String>>>) -> Box<dyn Subscription + 'a> {
        self.observer = Some(observer);
        Box::new(UpdatesSubscription::new(&mut self.observer))
    }
}

fn handle_connection(observer: Arc<Mutex<dyn Observer<Update<Value>, String>>>, stream: TcpStream) -> impl Future<Item = (), Error = ()> {
    let mut upds = Vec::new();
    let stream = std::io::BufReader::new(stream);
    tokio::io::lines(stream).for_each(move |line| {
        // This closure is called for each line we receive,
        // and returns a Future that represents the work we
        // want to do before accepting the next line.
        // In this case, we just wanted to print, so we
        // don't need to do anything more.
        let (v, relid, pol): (RelId, Value, bool) = serde_json::from_str(&line).unwrap();
        println!("{:?}", (v, pol));
        upds.push((v, relid, pol));
        println!("{:?}", upds);
        Ok(())
    }).map_err(|err| {
        println!("accept error = {:?}", err);
    })
}

impl TcpReceiver {
    pub fn listen(&mut self) {
        let listener = TcpListener::bind(&self.addr).unwrap();
        if let Some(observer) = self.observer.take() {
            let server = listener.incoming().for_each(move |_socket| {
                let mut obs = observer.lock().unwrap();
                obs.on_start();
                obs.on_commit();
                // obs.lock().unwrap().on_start();
                // tokio::spawn(handle_connection(observer.clone(), socket));
                Ok(())
            }).map_err(|err| {
                println!("accept error = {:?}", err);
            });

            tokio::run(server);
        }
    }
}

struct TestObserver{}

impl Observer<Update<Value>, String> for TestObserver {
    fn on_start(&mut self) -> Response<()> {
        println!("startin!");
        Ok(())
    }
    fn on_commit(&mut self) -> Response<()> {
        println!("commiting!");
        Ok(())
    }
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<Value>> + 'a>) -> Response<()> {
        Ok(())
    }
    fn on_completed(self) -> Response<()> {
        println!("completing!");
        Ok(())
    }
}

fn main() {
    let addr_s = "127.0.0.1:8000";
    let addr = addr_s.parse::<SocketAddr>().unwrap();

    let mut receiver = TcpReceiver::new(addr);

    let obs = Arc::new(Mutex::new(TestObserver{}));

    receiver.subscribe(obs.clone());

    receiver.listen();
}
