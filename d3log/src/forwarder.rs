use crate::{Batch, DDValueBatch, Evaluator, Node, Port, Transport};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Forwarder {
    eval: Evaluator,
    fib: Arc<Mutex<HashMap<Node, Port>>>,
}

impl Forwarder {
    pub fn new(e: Evaluator) -> Forwarder {
        // ok - we dont really want to start another hddlog here, but it helps
        // quite a bit in reducing the amount of sharing going on through TM.
        // ideally we could ask this question without access to the whole machine?
        // or share better with the other guy

        Forwarder {
            eval: e,
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

        for (rel, v, weight) in &DDValueBatch::from(self.eval.clone(), b).expect("iterator") {
            // xxx - through an api
            match self.eval.localize(rel, v.clone()) {
                // not sure I agree with inner_val .. guess so?
                Some((loc_id, in_rel, inner_val)) => output
                    .entry(loc_id)
                    .or_insert_with(|| Box::new(DDValueBatch::new()))
                    .deref_mut()
                    .insert(in_rel, inner_val, weight),
                None => (),
            }
        }

        for (nid, b) in output.drain() {
            // there is a short version of this like expect?
            match self.fib.lock().expect("lock").get(&nid) {
                Some(x) => x.send(Batch::DDValue(b.deref().clone())),
                None => {
                    panic!("missing nid");
                }
            }
        }
    }
}
