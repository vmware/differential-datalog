// fowarder takes a batch and uses the hddlog interface to extract those facts with
// locality annotations, groups them by destination, and calls the registered send
// method for that destination

use crate::{
    async_error, function, send_error, Batch, DDValueBatch, Dispatch, Evaluator, Node, Port,
    RecordBatch, Transport,
};
use differential_datalog::record::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

struct ForwardingEntryHandler {
    eval: Evaluator,
    forwarder: Arc<Forwarder>,
}

impl Transport for ForwardingEntryHandler {
    fn send(&self, b: Batch) {
        // reconcile
        for (_r, f, _w) in &RecordBatch::from(self.eval.clone(), b) {
            let target = async_error!(
                self.eval,
                u128::from_record(f.get_struct_field("target").expect("target"))
            );
            let z = f.get_struct_field("intermediate").expect("intermediate");
            let intermediate = async_error!(self.eval, u128::from_record(z));
            let e = self.forwarder.lookup(intermediate);
            let mut e2 = { e.lock().expect("lock") };
            match &e2.port {
                Some(p) => self.forwarder.register(target, p.clone()),
                None => {
                    e2.registrations.push_back(target);
                }
            }
        }
    }
}

#[derive(Clone)]
struct Entry {
    port: Option<Port>,
    batches: VecDeque<Batch>,
    registrations: VecDeque<Node>,
}

pub struct Forwarder {
    eval: Evaluator,
    fib: Arc<Mutex<HashMap<Node, Arc<Mutex<Entry>>>>>,
}

impl Forwarder {
    pub fn new(eval: Evaluator, dispatch: Arc<Dispatch>, _management: Port) -> Arc<Forwarder> {
        let f = Arc::new(Forwarder {
            eval: eval.clone(),
            fib: Arc::new(Mutex::new(HashMap::new())),
        });
        dispatch
            .clone()
            .register(
                "d3_application::Forward",
                Arc::new(ForwardingEntryHandler {
                    eval: eval.clone(),
                    forwarder: f.clone(),
                }),
            )
            .expect("register");
        f
    }

    fn lookup(&self, n: Node) -> Arc<Mutex<Entry>> {
        self.fib
            .lock()
            .expect("lock")
            .entry(n)
            .or_insert_with(|| {
                Arc::new(Mutex::new(Entry {
                    port: None,
                    batches: VecDeque::new(),
                    registrations: VecDeque::new(),
                }))
            })
            .clone()
    }

    pub fn register(&self, n: Node, p: Port) {
        // overwrite warning?
        let entry = self.lookup(n);
        {
            entry.lock().expect("lock").port = Some(p.clone());
        }

        while let Some(b) = { entry.lock().expect("lock").batches.pop_front() } {
            p.clone().send(b);
        }
        while let Some(r) = { entry.lock().expect("lock").registrations.pop_front() } {
            self.register(r, p.clone());
        }
    }
}

use std::ops::DerefMut;

impl Transport for Forwarder {
    fn send(&self, b: Batch) {
        let mut output = HashMap::<Node, Box<DDValueBatch>>::new();
        for (rel, v, weight) in &DDValueBatch::from(&(*self.eval), b).expect("iterator") {
            if let Some((loc_id, in_rel, inner_val)) = self.eval.localize(rel, v.clone()) {
                output
                    .entry(loc_id)
                    .or_insert_with(|| Box::new(DDValueBatch::new()))
                    .deref_mut()
                    .insert(in_rel, inner_val, weight);
            }
        }

        for (nid, b) in output.drain() {
            let p = {
                match self.lookup(nid).lock() {
                    Ok(mut x) => match &x.port {
                        Some(x) => x.clone(),
                        None => {
                            x.batches.push_front(Batch::Value(*b));
                            break;
                        }
                    },
                    Err(_) => panic!("lock"),
                }
            };
            p.send(Batch::Value(b.deref().clone()))
        }
    }
}
