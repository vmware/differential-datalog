extern crate serde_json;

use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use differential_datalog::program::{RelId, Update, Response};

use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;
use ddd_ddlog::channel::{Observable, Observer, Channel, Subscription};
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

pub struct TcpSender{
    addr: SocketAddr,
    client: Option<Box<dyn Future<Item = (), Error = ()> + Send>>,
    stream: Option<ConnectFuture>,
}

impl TcpSender{
    pub fn new(socket: SocketAddr) -> Self {
        TcpSender{
            addr: socket,
            client: None,
            stream: None,
        }
    }
}

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

impl Observer<Update<Value>, String> for TcpSender {
    fn on_start(&mut self) -> Response<()> {
        self.stream = Some(TcpStream::connect(&self.addr));
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<Value>> + 'a>) -> Response<()> {

        let upds: Vec<_> = updates.map(|upd| {
            match upd {
                Update::Insert{relid, v} =>
                    serde_json::to_string(&(relid, v, true)).unwrap() + "\n", // TODO what's the right way to do this?
                Update::DeleteValue{relid, v} =>
                    serde_json::to_string(&(relid, v, false)).unwrap() + "\n", // TODO what's the right way to do this?
                _ => panic!("Committed update is neither insert or delete")
            }
        }).collect();

        if let Some(stream) = self.stream.take() {
            // TODO FIX: if we allow multiple on_update calls between on_commit,
            // this will only execute the last call
            self.client = Some(Box::new(stream.and_then(move |stream| {
                // TODO check buffering
                stream::iter_ok(upds).fold(stream, |writer, buf| {
                    tokio::io::write_all(writer, buf)
                        .map(|(writer, _buf)| writer)
                }).then(|_: Result<_, std::io::Error>| Ok(()))
            }).map_err(|err| {
                println!("connection error = {:?}", err);
            })));
        };

        Ok(())
    }

    fn on_commit(&mut self) -> Response<()> {
        if let Some(client) = self.client.take() {
            tokio::run(client);
        }
        Ok(())
    }

    fn on_completed(self) -> Response<()> {
        // TODO Properly shutdown client and server. This might
        // involve calling shutdown on client and dropping the server.
        Ok(())
    }
}

impl Observable<Update<Value>, String> for TcpReceiver {
    fn subscribe<'a>(&'a mut self, observer: Arc<Mutex<dyn Observer<Update<Value>, String>>>) -> Box<dyn Subscription + 'a> {
        self.observer = Some(observer);
        Box::new(UpdatesSubscription::new(&mut self.observer))
    }
}


fn handle_connection(stream: TcpStream) -> impl Future<Item = (), Error = ()> {
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
    pub fn listen(&self) {
        let listener = TcpListener::bind(&self.addr).unwrap();
        let server = listener.incoming().for_each(move |socket| {
            println!("yo");
            tokio::spawn(handle_connection(socket));
            Ok(())
        }).map_err(|err| {
            println!("accept error = {:?}", err);
        });

        tokio::run(server);
    }
}

fn main() -> Response<()> {
    let addr_s = "127.0.0.1:8000";
    let addr = addr_s.parse::<SocketAddr>().unwrap();

    let receiver = TcpReceiver::new(addr);
    let mut sender = TcpSender::new(addr);

    receiver.listen();

    let updates = vec![
        Update::Insert{relid: 3, v: Value::bool(false)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
        Update::DeleteValue{relid: 2, v: Value::bool(true)},
    ];
    let upds: Box<dyn Iterator<Item = Update<Value>>> = Box::new(updates.into_iter());

    sender.on_start()?;
    sender.on_updates(upds)?;
    sender.on_commit()
}
