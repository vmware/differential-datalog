use differential_datalog::program::Update;
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use observe::{Observer, Observable, Subscription};
use tcp_channel::{TcpSender, TcpReceiver};

use roundtrip_ddlog::*;
use roundtrip_ddlog::api::*;
use roundtrip_ddlog::Relations::*;

use std::net::SocketAddr;
use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};

fn main() -> Result<(), String> {

    // Construct server a, redirect Out
    let prog = HDDlog::run(1, false, |_,_:&Record, _| {});
    let mut redirect = HashMap::new();
    redirect.insert(rt_b_Out as usize, rt_a_FromB as usize);
    let mut s = server::DDlogServer::new(prog, redirect);

    // Sending TCP channel
    let addr_s = "127.0.0.1:8001";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let sender = TcpSender::new(addr);

    // Receiving TCP channel
    let addr_s = "127.0.0.1:8002";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let mut receiver = TcpReceiver::new(addr);

    // Stream table from left server
    let mut tup = HashSet::new();
    tup.insert(rt_a_ToB as usize);
    let outlet = s.add_stream(tup);

    // Server subscribes to the upstream TCP channel
    let s = Arc::new(Mutex::new(s));
    let _sub1 = {
        let s_a = server::ADDlogServer(s.clone());
        let adapter = AdapterR{observer: Box::new(s_a)};
        receiver.subscribe(Box::new(adapter))
    };

    // Downstream TCP channel subscribes to the stream
    let _sub2 = {
        let stream = outlet.clone();
        let mut stream = stream.lock().unwrap();
        let adapter = AdapterL{observer: Box::new(sender)};
        stream.subscribe(Box::new(adapter))
    };

    // Insert `true` to Left in left server
    let rec = Record::Bool(true);
    let table_id = RelIdentifier::RelId(rt_a_In as usize);
    let updates = &[UpdCmd::Insert(table_id, rec)];

    // Execute and transmit the update
    {
        let s = s.clone();
        let mut s = s.lock().unwrap();

        s.on_start()?;
        s.on_updates(Box::new(updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap())))?;
        s.on_commit()?;
        s.on_completed()?;
    }

    let handle = receiver.listen();

    handle.join();

    let mut s = s.lock().unwrap();
    s.shutdown()
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
        println!("Rstart");
        self.observer.on_start()?;
        println!("Rstart done");
        Ok(())
    }
    fn on_commit(&mut self) -> Result<(), String> {
        println!("Rcommit");
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
        println!("coming through!");
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
        println!("coming through!");
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
