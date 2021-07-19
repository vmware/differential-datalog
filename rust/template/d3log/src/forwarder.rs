// fowarder takes a batch and uses the hddlog interface to extract those facts with
// locality annotations, groups them by destination, and calls the registered send
// method for that destination

use crate::{async_error, function, Batch, DDValueBatch, Error, Evaluator, Node, Port, Transport};
use differential_datalog::record::*;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Forwarder {
    eval: Evaluator,
    management: Port,
    fib: Arc<Mutex<HashMap<Node, Port>>>,
}

impl Forwarder {
    pub fn new(eval: Evaluator, management: Port) -> Forwarder {
        Forwarder {
            eval,
            management,
            fib: Arc::new(Mutex::new(HashMap::new())),
        }
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
            match self.fib.lock().expect("lock").get(&nid) {
                Some(x) => x.send(Batch::Value(b.deref().clone())),
                None => async_error!(
                    self.eval.clone(),
                    Err(Error::new(format!("missing nid {}", nid)))
                ),
            }
        }
    }
}
