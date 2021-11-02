//! This component allows multiple D3log instances to communicate with
//! each other.  It essentially buffers the entire history of messages
//! exchanged between instances and replays it every time a new instance
//! joins the distributed system.  This code runs in all instances.

// this includes 'split horizon' as a temporary
// fix for simple pairwise loops. This will need an additional distributed coordination mechanism in
// order to maintain a consistent spanning tree (and a strategy for avoiding storms for temporariliy
// inconsistent topologies)

use crate::{
    async_error, function, send_error, Batch, BatchBody, Error, ErrorSend, Evaluator, Port,
    Properties, Transport, ValueSet,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

/// A local connector for a broadcast bus.
type PortId = usize;
#[derive(Clone, Default)]
pub struct Broadcast {
    // accumulator is a log of all sent messages.
    // Its contents is replayed to new subscribers.
    // the first arc/rwlock to allow Some() to be written in register_eval,
    // and the next is over the batch itself.
    // Collection names used in the ValueSet are global collection names
    // (not the localized names).
    accumulator: Arc<RwLock<Option<Arc<RwLock<ValueSet>>>>>,
    // count is effectively an allocator use allocate unique ids to ports
    // (rather than use the address of the underlying object)
    // so we can perform trivial forwarding cycle checks
    count: Arc<AtomicUsize>,
    // the set of ports and their ids to distribute facts to
    pub ports: Arc<RwLock<Vec<(Port, PortId)>>>,
}

// Models a port used to send broadcast to other ports, but which filters
// out the sent messages so that they are not received again.
pub struct Ingress {
    id: PortId,
    broadcast: Arc<Broadcast>,
}

struct AccumulatePort {
    // eval is only used for ValueSet::from - we could define a narrower trait
    eval: Evaluator,
    error: Arc<ErrorSend>,
    accumulator: Arc<RwLock<ValueSet>>,
}

impl Transport for AccumulatePort {
    // Place the batch data on the broadcast bus.  The batch is
    // received by everyone who has subscribed to the broadcast bus.
    fn send(&self, batch: Batch) {
        let vbatch = async_error!(self.error.clone(), ValueSet::from(self.eval.clone(), batch));
        for (r, f, w) in vbatch.into_iter() {
            self.accumulator.write().expect("lock").insert(r, f, w);
        }
    }
}

impl Broadcast {
    pub fn new() -> Arc<Broadcast> {
        Arc::new(Broadcast {
            accumulator: Arc::new(RwLock::new(None)),
            count: Arc::new(AtomicUsize::new(0)),
            ports: Arc::new(RwLock::new(Vec::new())),
        })
    }

    // Register a new port to receive broadcast messages.
    // TODO: There is no 'pop' to remove a port.
    fn push(&self, p: Port, id: PortId) {
        self.ports.write().expect("lock").push((p, id));
    }

    // used to register a new DDlog instance (eval) when it starts up.
    pub fn register_eval(a: Arc<Broadcast>, eval: Evaluator, error: Arc<ErrorSend>) {
        let a2 = a.clone();
        let a4 = a.clone();
        let acc = Arc::new(RwLock::new(ValueSet::new(eval.clone())));
        *(a.accumulator.write().unwrap()) = Some(acc.clone());
        a2.clone().push(
            Arc::new(AccumulatePort {
                error: error,
                eval: eval,
                accumulator: acc.clone(),
            }),
            a4.clone().count.fetch_add(1, Ordering::Acquire),
        );
    }

    // 'couple' here stands for a verb: it performs the union of
    // the state in two distinct broadcast groups, coupling them.
    // couple is a specialized version of subscribe to connect two in-process broadcast instances together
    pub fn couple(a: Arc<Broadcast>, b: Arc<Broadcast>) -> Result<(), Error> {
        let id = a.count.fetch_add(1, Ordering::Acquire);
        // pba is the port that forwards from b to a
        let pba = Arc::new(Ingress {
            broadcast: a.clone(),
            id,
        });

        // pab is the port that forwards from a to b
        let pab = b.clone().subscribe(pba.clone());
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
    fn subscribe(self, receive: Port) -> Port;
}

impl PubSub for Arc<Broadcast> {
    fn subscribe(self, receive: Port) -> Port {
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
