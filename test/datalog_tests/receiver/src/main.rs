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
use futures::sync::oneshot;

use std::sync::{Arc, Mutex};
use std::net::{SocketAddr, Shutdown};
use std::iter;
use std::io::BufReader;

use std::boxed::*;

pub struct TcpReceiver{
    addr: SocketAddr,
    server: Option<Box<dyn Future<Item = (), Error = ()> + Send + Sync>>,
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

struct TcpSubscription {
    shutdown: oneshot::Sender<()>,
    server: Option<Box<dyn Future<Item = (), Error = ()> + Send + Sync>>,
}

impl TcpSubscription {
    fn listen(&mut self) {
        if let Some(server) = self.server.take(){
            tokio::run(server);
        }
    }
}

impl Subscription for TcpSubscription {
    fn unsubscribe(self: Box<Self>) {
        self.shutdown.send(());
    }
}

impl Observable<Update<Value>, String> for TcpReceiver {
    fn subscribe<'a>(&'a mut self, observer: Arc<Mutex<dyn Observer<Update<Value>, String>>>) -> Box<dyn Subscription + 'a> {
        let listener = TcpListener::bind(&self.addr).unwrap();
        let server = listener.incoming().for_each(move |socket| {

            let observer = observer.clone();

            let stream = std::io::BufReader::new(socket);

            let work = tokio::io::lines(stream).map(move |line| {
                let (relid, v, pol): (RelId, Value, bool) = serde_json::from_str(&line).unwrap();
                if pol {
                    Update::Insert{relid: relid, v: v}
                } else {
                    Update::DeleteValue{relid: relid, v: v}
                }
            }).collect().and_then(move |upds| {
                let mut obs = observer.lock().unwrap();
                obs.on_start();
                obs.on_updates(Box::new(upds.into_iter()));
                Ok(())
            }).map_err(|err| {
                print!("error {:?}", err)
            });

            tokio::spawn(work);
            Ok(())
        }).map_err(|err| {
            oneshot::Canceled
        });

        let (shutdown_sender, shutdown_receiver) = oneshot::channel();

        let f = server.select(shutdown_receiver)
            .and_then(|_| Ok(()))
            .map_err(|_err| {
            println!("err");
        });

        // self.server = Some(Box::new(f));

        Box::new(TcpSubscription{
            shutdown: shutdown_sender,
            server: Some(Box::new(f))
        })
    }
}

impl TcpReceiver {
    pub fn listen(&self) {}
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
        let upds:Vec<_> = updates.collect();
        println!("{:?}", upds.len());
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

    let sub = receiver.subscribe(obs.clone());
    sub.unsubscribe();

    //if let Some(server) = receiver.server.take() {
    //    tokio::run(server);
    //}
    //receiver.listen();
    //sub.unsubscribe();
    //receiver.listen();
}
