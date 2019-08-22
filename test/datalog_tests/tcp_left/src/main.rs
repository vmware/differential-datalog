use differential_datalog::program::Update;
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use observe::{Observer, Observable, Subscription};
use tcp_channel::TcpSender;

use ddd_ddlog::*;
use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;

use std::net::SocketAddr;
use std::collections::{HashSet, HashMap};

fn main() -> Result<(), String> {

    // Construct left server with no redirect
    let prog1 = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect1 = HashMap::new();
    redirect1.insert(lr_left_Left as usize, lr_left_Left as usize);
    let mut s1 = server::DDlogServer::new(prog1, redirect1);

    // First TCP channel
    let addr_s = "127.0.0.1:8001";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut chan1 = TcpSender::new(addr);
    chan1.connect();
    let adapter1 = Adapter{observer: Box::new(chan1)};

    // Second TCP channel
    let addr_s = "127.0.0.1:8002";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut chan2 = TcpSender::new(addr);
    chan2.connect();
    let adapter2 = Adapter{observer: Box::new(chan2)};

    // Stream Up table from left server
    let mut tup = HashSet::new();
    tup.insert(lr_left_Up as usize);
    let mut outlet_up = s1.add_stream(tup);

    // First TcpSender subscribes to the stream
    let _sub1 = {
        outlet_up.subscribe(Box::new(adapter1))
    };

    // Stream Down table from left server
    let mut tdown = HashSet::new();
    tdown.insert(lr_left_Down as usize);
    let mut outlet_down = s1.add_stream(tdown);

    // Second TcpSender subscribes to the stream
    let _sub2 = {
        outlet_down.subscribe(Box::new(adapter2))
    };

    // Insert `true` to Left in left server
    let rec = Record::Bool(false);
    let table_id = RelIdentifier::RelId(lr_left_Left as usize);
    let updates = &[UpdCmd::Insert(table_id, rec)];

    // Execute and transmit the update
    s1.on_start()?;
    s1.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())))?;
    s1.on_commit()?;
    s1.on_completed()?;

    s1.shutdown()
}

// We need the following Adapters to transform observables until
// `Observable::map` is implemented. `map` is challenging because
// it is a generic method, but we want to have trait objects of
// `Observable`. Generics are not object safe. We can take notes
// of the Rust Iterator implementation to address the dilemma.

struct Adapter {
    observer: Box<dyn Observer<(usize, Value, bool), String>>
}

struct AdapterSub;

impl Subscription for AdapterSub {
    fn unsubscribe(self: Box<Self>) {
    }
}

impl Observable<(usize, Value, bool), String> for Adapter {
    fn subscribe(&mut self,
                 observer: Box<dyn Observer<(usize, Value, bool), String>+ Send>) -> Box<dyn Subscription>{
        self.observer = observer;
        Box::new(AdapterSub)
    }
}

impl Observer<Update<Value>, String> for Adapter {
    fn on_start(&mut self) -> Result<(), String> {
        self.observer.on_start()
    }
    fn on_commit(&mut self) -> Result<(), String> {
        self.observer.on_commit()
    }
    fn on_next(&mut self, item: Update<Value>) -> Result<(), String> {
        self.observer.on_next(match item {
            Update::Insert{relid, v} => (relid, v, true),
            Update::DeleteValue{relid, v} => (relid, v, false),
            _ => panic!("Only insert and deletes are permitted")
        })
    }
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<Value>> + 'a>) -> Result<(), String> {
        self.observer.on_updates(Box::new( updates.map(|upd| match upd {
            Update::Insert{relid, v} => (relid, v, true),
            Update::DeleteValue{relid, v} => (relid, v, false),
            _ => panic!("Only insert and deletes are permitted")
        })))
    }
    fn on_completed(&mut self) -> Result<(), String> {
        self.observer.on_completed()
    }
    fn on_error(&self, error: String) {
        self.observer.on_error(error)
    }
}
