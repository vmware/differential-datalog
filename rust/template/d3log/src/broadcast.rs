// a replication component to support broadcast metadata facts. this includes 'split horizon' as a temporary
// fix for simple pairwise loops. This will need an additional distributed coordination mechanism in
// order to maintain a consistent spanning tree (and a strategy for avoiding storms for temporariliy
// inconsistent topologies)

use crate::{Batch, BatchBody, Error, Evaluator, Node, Port, Properties, Transport, ValueSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

type PortId = usize;
#[derive(Clone, Default)]
pub struct Broadcast {
    // uuid is only used for debugging
    uuid: Node,
    // accumulator is copy of the broadcast log for use in catching up new subscribers
    // the first arc/rwlock to allow Some() to be written in register_eval, and the next is over the batch itself
    accumulator: Arc<RwLock<Option<Arc<RwLock<ValueSet>>>>>,
    // count is effectively an allocator we use to label ports (rather than use the address of the underlying object)
    // so we can perform trivial forwarding cycle checks
    count: Arc<AtomicUsize>,
    // the set of ports to distribute facts to
    pub ports: Arc<RwLock<Vec<(Port, PortId)>>>,
}

pub struct Ingress {
    id: PortId,
    broadcast: Arc<Broadcast>,
}

struct AccumulatePort {
    // eval is only used for ValueSet::from - we could define a narrower trait
    eval: Evaluator,
    accumulator: Arc<RwLock<ValueSet>>,
}

impl Transport for AccumulatePort {
    fn send(&self, b: Batch) {
        for (r, f, w) in &ValueSet::from(self.eval.clone(), b).expect("iterator") {
            self.accumulator.write().expect("lock").insert(r, f, w);
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

    fn push(&self, p: Port, id: PortId) {
        self.ports.write().expect("lock").push((p, id));
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
                accumulator: acc.clone(),
            }),
            a4.clone().count.fetch_add(1, Ordering::Acquire),
        );
    }

    // couple is a specialized version of subscribe to connect two in-process broadcast instances together
    pub fn couple(a: Arc<Broadcast>, b: Arc<Broadcast>) -> Result<(), Error> {
        let id = a.count.fetch_add(1, Ordering::Acquire);
        // pba is the port that forwards from b to a
        let pba = Arc::new(Ingress {
            broadcast: a.clone(),
            id,
        });

        // pab is the port that forwards from a to b
        let pab = b.clone().subscribe(pba.clone(), b.clone().uuid);
        a.push(pab.clone(), id);

        let batch = (*a.accumulator.clone().read().unwrap())
            .clone()
            .unwrap()
            .read()
            .unwrap()
            .clone();

        pab.send(Batch {
            // may as well express the source here..and i suppose all the contributions?
            metadata: Properties::new(),
            // we're assuming that there is no way a batch may be received before the register_eval()
            // call - it needs to precede any subscriptions or sends.
            body: BatchBody::Value(batch),
        });
        Ok(())
    }
}

pub trait PubSub {
    // xxx - should be able to remove a subscription

    // receive is the port which broadcast will send to, and
    // the returned port one the caller should use to inject
    // batches (which cant be named in rust?)
    fn subscribe(self, receive: Port, uuid: Node) -> Port;
}

impl PubSub for Arc<Broadcast> {
    fn subscribe(self, receive: Port, _uuid: Node) -> Port {
        let id = self.clone().count.fetch_add(1, Ordering::Acquire);
        self.clone().push(receive.clone(), id);

        // are we racing against future insertions into the accumulator?
        // or is this a proper copy?
        let batch = (*self.accumulator.clone().read().unwrap())
            .clone()
            .unwrap()
            .read()
            .unwrap()
            .clone();

        receive.clone().send(Batch {
            metadata: Properties::new(),
            body: BatchBody::Value(batch),
        });

        Arc::new(Ingress {
            broadcast: self.clone(),
            id,
        })
    }
}

impl Transport for Broadcast {
    fn send(&self, b: Batch) {
        // We clone this map to have a read-only copy, else, we'd open up the possiblity of a
        // deadlock, if this `send` forms a cycle. concerned that there is a race as above.
        let ports = { &*self.ports.read().expect("lock").clone() };
        for (port, _) in ports {
            port.send(b.clone())
        }
    }
}

// an Ingress port couples an output with an input, to avoid redistributing
// facts back to the source. proper cycle detection will require spanning tree
impl Transport for Ingress {
    fn send(&self, b: Batch) {
        let ports = &*self.broadcast.ports.read().expect("lock").clone();
        for (port, id) in ports {
            if *id != self.id {
                port.send(b.clone())
            }
        }
    }
}
