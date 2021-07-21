// fowarder takes a batch and uses the hddlog interface to extract those facts with
// locality annotations, groups them by destination, and calls the registered send
// method for that destination

use crate::{
    async_error, function, send_error, Batch, DDValueBatch, Dispatch, Error, Evaluator, Node, Port,
    RecordBatch, Transport,
};
use differential_datalog::record::*;
use std::collections::HashMap;
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
            // what about ordering wrt the assertion of the intermediate and transitive forwarding
            // what about the source port
            let x = if let Some(p) = self.forwarder.fib.lock().expect("lock").get(&intermediate) {
                p.clone()
            } else {
                panic!("nested forwarder error should be asynch");
            };
            self.forwarder.register(target, x.clone());
        }
    }
}

#[derive(Clone)]
pub struct Forwarder {
    eval: Evaluator,
    management: Port,
    fib: Arc<Mutex<HashMap<Node, Port>>>,
}

impl Forwarder {
    pub fn new(eval: Evaluator, dispatch: Arc<Dispatch>, management: Port) -> Arc<Forwarder> {
        let f = Arc::new(Forwarder {
            eval: eval.clone(),
            management: management.clone(),
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

    pub fn register(&self, n: Node, p: Port) {
        self.fib.lock().expect("lock").insert(n, p);
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
            let x = {
                if let Some(x) = self.fib.lock().expect("lock").get(&nid) {
                    x.clone()
                } else {
                    send_error!(self.eval.clone(), format!("missing nid {}", nid));
                    return;
                }
            };
            x.send(Batch::Value(b.deref().clone()));
        }
    }
}
