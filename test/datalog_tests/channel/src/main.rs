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
use std::net::SocketAddr;

pub struct TCPChannel{
    stream: Option<ConnectFuture>,
    listener: Option<TcpListener>,

    client: Option<Box<dyn Future<Item = (), Error = ()> + Send>>,
    server: Option<Box<dyn Future<Item = (), Error = ()> + Send>>,

    addr: SocketAddr,
    output: Option<Arc<Mutex<dyn Observer<Update<Value>, String>>>>,
}

fn handle_connection(stream: TcpStream) -> impl Future<Item = (), Error = ()> {
    let buf = Vec::new();
    let res = io::read_to_end(stream, buf);

    let work = res.then(move |result| {
        match result {
            Ok((_socket, buf)) => {
                let s = String::from_utf8(buf).unwrap();
                let (rec, _table): (Record, usize) = serde_json::from_str(&s).unwrap();

                let updates = &[UpdCmd::Insert(
                    RelIdentifier::RelId(lr_right_Middle as usize),
                    rec)];
                println!("{:?}", updates);
                // TODO pass on the updates to the observer
            }
            Err(e) => println!("{:?}", e),
        }

        Ok(())
    });

    work
}

impl Observer<Update<Value>, String> for TCPChannel {
    fn on_start(&mut self) -> Response<()> {
        self.stream = Some(TcpStream::connect(&self.addr));
        self.listener = Some(TcpListener::bind(&self.addr).unwrap());
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<Value>> + 'a>) -> Response<()> {
        if let Some(stream) = self.stream.take() {
            self.client = Some(Box::new(stream.and_then(move |stream| {
                // TODO transmit the updates through the channel
                io::write_all(stream, "TODO").then(|_| Ok(()))
            }).map_err(|err| {
                println!("connection error = {:?}", err);
            })));
        };

        if let Some(listener) = self.listener.take() {
            self.server = Some(Box::new(listener.incoming().for_each(move |socket| {
                tokio::spawn(handle_connection(socket));
                Ok(())
            }).map_err(|err| {
                println!("accept error = {:?}", err);
            })))
        }

        Ok(())
    }

    fn on_commit(&mut self) -> Response<()> {
        if let Some(client) = self.client.take() {
            tokio::run(client);
        }
        Ok(())
    }

    fn on_completed(self) -> Response<()> {
        // TODO Disconnect channel
        Ok(())
    }
}

impl Observable<Update<Value>, String> for TCPChannel {
    fn subscribe<'a>(&'a mut self, observer: Arc<Mutex<dyn Observer<Update<Value>, String>>>) -> Box<dyn Subscription + 'a> {
        self.output = Some(observer);
        Box::new(UpdatesSubscription::new(&mut self.output))
    }
}

impl Channel<Update<Value>, String> for TCPChannel {}

fn main() {
    println!("Hello, world!");
}
