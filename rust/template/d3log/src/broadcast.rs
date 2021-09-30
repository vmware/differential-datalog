// a replication component to support broadcast metadata facts. this includes 'split horizon' as a temporary
// fix for simple pairwise loops. This will need an additional distributed coordination mechanism in
// order to maintain a consistent spanning tree (and a strategy for avoiding storms for temporariliy
// inconsistent topologies

use crate::{Batch, BatchBody, Error, Evaluator, Node, Port, Transport, ValueSet};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct Broadcast {
    // the first arc/rwlock to allow Some() to be written in register_eval, and the
    // next is over the batch itself
    uuid: Node,
    accumulator: Arc<RwLock<Option<Arc<RwLock<ValueSet>>>>>,
    count: Arc<AtomicUsize>,
    pub ports: Arc<RwLock<Vec<(Port, usize)>>>,
}

struct AccumulatePort {
    eval: Evaluator,
    b: Arc<RwLock<ValueSet>>,
}

impl Transport for AccumulatePort {
    fn send(&self, b: Batch) {
        for (r, f, w) in &ValueSet::from(self.eval.clone(), b).expect("iterator") {
            self.b.write().expect("lock").insert(r, f, w);
        }
    }
}

impl Broadcast {
    pub fn new(uuid: Node) -> Arc<Broadcast> {
        Arc::new(Broadcast {
            uuid,
            accumulator: Arc::new(RwLock::new(None)),
            count: Arc::new(AtomicUsize::new(0)),
            ports: Arc::new(RwLock::new(Vec::new())),
        })
    }

    fn push(&self, p: Port, index: usize) {
        self.ports.write().expect("lock").push((p, index));
    }

    // not pretty - this is an issue with initialization order
    pub fn register_eval(a: Arc<Broadcast>, eval: Evaluator) {
        let a2 = a.clone();
        let a4 = a.clone();
        let acc = Arc::new(RwLock::new(ValueSet::new(eval.clone())));
        *(a.accumulator.write().unwrap()) = Some(acc.clone());
        a2.clone().push(
            Arc::new(AccumulatePort {
                eval: eval,
                b: acc.clone(),
            }),
            a4.clone().count.fetch_add(1, Ordering::Acquire),
        );
    }

    // couple is a specialized version of subscribe to connect two broadcast instances together
    pub fn couple(a: Arc<Broadcast>, b: Arc<Broadcast>) -> Result<(), Error> {
        let index = a.count.fetch_add(1, Ordering::Acquire);
        let pba = Arc::new(Ingress {
            broadcast: a.clone(),
            index,
        });
        let pab = b.clone().subscribe(pba.clone(), b.clone().uuid);
        a.push(pab.clone(), index);

        let batch = (*a.accumulator.clone().read().unwrap())
            .clone()
            .unwrap()
            .read()
            .unwrap()
            .clone();

        pab.send(Batch {
            // may as well express the source here..and i suppose all the contributions?
            metadata: HashMap::new(),
            // we're assuming that there is no way to get a batch before the register_eval()
            // call - it needs to preceed any subscriptions or sends.
            body: BatchBody::Value(batch),
        });
        Ok(())
    }
}

pub trait PubSub {
    fn subscribe(self, p: Port, uuid: Node) -> Port;
}

impl PubSub for Arc<Broadcast> {
    fn subscribe(self, p: Port, _uuid: Node) -> Port {
        let index = self.clone().count.fetch_add(1, Ordering::Acquire);
        self.clone().push(p.clone(), index);

        // are we racing against future insertions into the accumulator?
        // or is this a proper copy?

        let batch = (*self.accumulator.clone().read().unwrap())
            .clone()
            .unwrap()
            .read()
            .unwrap()
            .clone();

        p.clone().send(Batch {
            metadata: HashMap::new(),
            body: BatchBody::Value(batch),
        });

        Arc::new(Ingress {
            broadcast: self.clone(),
            index,
        })
    }
}

impl Transport for Broadcast {
    fn send(&self, b: Batch) {
        // We clone this map to have a read-only copy, else, we'd open up the possiblity of a
        // deadlock, if this `send` forms a cycle.
        let ports = { &*self.ports.read().expect("lock").clone() };
        for (port, _) in ports {
            port.send(b.clone())
        }
    }
}

// an Ingress port couples an output with an input, to avoid redistributing
// facts back to the source. proper cycle detection will require spanning tree
pub struct Ingress {
    index: usize,
    broadcast: Arc<Broadcast>,
}

impl Ingress {
    pub fn remove(&mut self, _p: Port) {}
}

impl Transport for Ingress {
    fn send(&self, b: Batch) {
        let ports = &*self.broadcast.ports.read().expect("lock").clone();
        for (port, index) in ports {
            if *index != self.index {
                port.send(b.clone())
            }
        }
    }
}
