// a temporary shim to direct updates to functions as a placeholder for sinks. route
// sub-batches to ports. ideally this would be some kind of general query which includes
// the relation id and the envelope. This would replace Transact.forward, which seems
// correct

use crate::{Batch, Error, Evaluator, Port, RecordBatch, Transport};

//use differential_datalog::program::RelId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Dispatch {
    eval: Evaluator, // to be removed
    count: Arc<AtomicUsize>,
    handlers: Arc<Mutex<HashMap<String, Vec<(u64, Port)>>>>,
}

impl Transport for Dispatch {
    // having send() take a nid makes this a bit strange for internal plumbing. if we close
    // the nid under Port - then need to expose it in the evelope. we're going to assign
    // a number to each otuput.. why does that seem wrong

    fn send(&self, b: Batch) {
        let mut output = HashMap::<u64, (Port, RecordBatch)>::new();

        for (rel, v, weight) in &RecordBatch::from(self.eval.clone(), b) {
            if let Some(ports) = self.handlers.lock().expect("lock").get(&rel) {
                for (i, p) in ports {
                    // we can probably do this databatch to recordbatch translation elsewhere and
                    // not pull in evaluator. oh, we also have translation port!

                    output
                        .entry(*i)
                        .or_insert_with(|| (p.clone(), RecordBatch::new()))
                        .1
                        .insert(rel.clone(), v.clone(), weight);
                }
            }
        }
        for (_, (p, b)) in output {
            p.send(Batch::Rec(b));
        }
    }
}

impl Dispatch {
    pub fn new(eval: Evaluator) -> Dispatch {
        Dispatch {
            eval,
            handlers: Arc::new(Mutex::new(HashMap::new())),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    // deregstration? return a handle?
    pub fn register(self, relation_name: &str, p: Port) -> Result<(), Error> {
        let id = self.count.fetch_add(1, Ordering::SeqCst);

        self.handlers
            .lock()
            .expect("lock")
            .entry(relation_name.to_string())
            .or_insert_with(|| Vec::new())
            .push((id as u64, p));
        Ok(())
    }
}
