use differential_datalog::program::{RelId, Update, Response};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use observe::{Observer, Observable, Subscription};
use tcp_channel::{TcpReceiver, TcpSender};

use ddd_ddlog::*;
use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;

use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

fn main() -> Result<(), String> {
    // Read from this port
    let addr_s = "127.0.0.1:8001";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut receiver = TcpReceiver::new(addr);
    receiver.connect().join();

    // Write to this port
    let addr_s = "127.0.0.1:8010";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut sender = TcpSender::new(addr);
    sender.connect();

    // Construct up server, redirect input table
    let prog = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect = HashMap::new();
    redirect.insert(lr_left_Up as usize, lr_up_Left as usize);
    let mut s = server::DDlogServer::new(prog, redirect);

    // Stream right table from up server
    let mut table = HashSet::new();
    table.insert(lr_up_Right as usize);
    let mut outlet = s.add_stream(table);

    // Server subscribes to the upstream TCP channel
    let s = Arc::new(Mutex::new(s));
    let _sub1 = {
        let s_a = server::ADDlogServer(s.clone());
        let adapter = AdapterR{observer: Box::new(s_a)};
        receiver.subscribe(Box::new(adapter))
    };

    // Downstream TCP channel subscribes to the server
    let _sub2 = {
        let adapter = AdapterL{observer: Box::new(sender)};
        outlet.subscribe(Box::new(adapter))
    };

    // Listen for updates on the upstream channel
    let handle = receiver.listen();
    handle.join();

    // Shutdown server
    s.lock().unwrap().shutdown()?;
    Ok(())
}

// We need the following Adapters to transform observables until
// `Observable::map` is implemented. `map` is challenging because
// it is a generic method, but we want to have trait objects of
// `Observable`. Generics are not object safe. We can take notes
// of the Rust Iterator implementation to address the dilemma.

struct AdapterR {
    observer: Box<dyn Observer<Update<Value>, String> + Send>
}

struct AdapterSub;

impl Subscription for AdapterSub {
    fn unsubscribe(self: Box<Self>) {
    }
}

impl Observable<Update<Value>, String>  for AdapterR {
    fn subscribe(&mut self,
                 observer: Box<dyn Observer<Update<Value>, String> + Send>) -> Box<dyn Subscription>{
        self.observer = observer;
        Box::new(AdapterSub)
    }
}

impl Observer<(usize, Value, bool), String> for AdapterR {
    fn on_start(&mut self) -> Result<(), String> {
        self.observer.on_start()
    }
    fn on_commit(&mut self) -> Result<(), String> {
        self.observer.on_commit()
    }
    fn on_next(&mut self, item: (usize, Value, bool)) -> Result<(), String> {
        let (relid, v, b) = item;
        let item = if b {
            Update::Insert{relid, v}
        } else {
            Update::DeleteValue{relid, v}
        };
        self.observer.on_next(item)
    }
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = (usize, Value, bool)> + 'a>) -> Result<(), String> {
        self.observer.on_updates(Box::new(updates.map(|(relid, v, b)| {
            if b {
                Update::Insert{relid, v}
            } else {
                Update::DeleteValue{relid, v}
            }
        })))
    }
    fn on_completed(&mut self) -> Result<(), String> {
        self.observer.on_completed()
    }
    fn on_error(&self, error: String) {
        self.observer.on_error(error)
    }
}

struct AdapterL {
    observer: Box<dyn Observer<(usize, Value, bool), String>>
}

impl Observable<(usize, Value, bool), String> for AdapterL {
    fn subscribe(&mut self,
                 observer: Box<dyn Observer<(usize, Value, bool), String>+ Send>) -> Box<dyn Subscription>{
        self.observer = observer;
        Box::new(AdapterSub)
    }
}

impl Observer<Update<Value>, String> for AdapterL {
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
