// dispatch is an external placeholder for a timely sink. It allows ports to be registered against
// relations, and like forwarder, groups up sub-batches based on relation id and routes them
// out the correct port. Used to hang management relation update ports off the broadcast tree

use crate::{Batch, Error, Evaluator, Port, RecordBatch, Transport};

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

type DispatchMap = HashMap<String, Vec<(u64, Port)>>;

#[derive(Clone)]
pub struct Dispatch {
    eval: Evaluator, // to be removed
    count: Arc<AtomicUsize>,
    handlers: Arc<Mutex<DispatchMap>>,
}

impl Transport for Dispatch {
    fn send(&self, b: Batch) {
        let mut output = HashMap::<u64, (Port, RecordBatch)>::new();

        println!(
            "dispatch: {}",
            RecordBatch::from(self.eval.clone(), b.clone())
        );

        for (rel, v, weight) in &RecordBatch::from(self.eval.clone(), b.clone()) {
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
    pub fn register(&self, relation_name: &str, p: Port) -> Result<(), Error> {
        let id = self.count.fetch_add(1, Ordering::SeqCst);

        self.handlers
            .lock()
            .expect("lock")
            .entry(relation_name.to_string())
            .or_insert_with(Vec::new)
            .push((id as u64, p));
        Ok(())
    }
}
