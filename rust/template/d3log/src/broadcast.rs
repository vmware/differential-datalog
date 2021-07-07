// the initial implementation of a distribution tree for replicated
// metadata. at minimum this will likely need to implement 'split horizon',
// where updates aren't sent back along the same branch to prevent loops.
// later, its likely that some form of spanning tree protocol be implemented

use crate::{Batch, Port, Transport};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct Ingress {
    index: usize,
    broadcast: Arc<Broadcast>,
}

#[derive(Clone, Default)]
pub struct Broadcast {
    count: Arc<AtomicUsize>,
    ports: Arc<Mutex<Vec<(Port, usize)>>>,
}

impl Broadcast {
    pub fn new() -> Arc<Broadcast> {
        Arc::new(Broadcast {
            count: Arc::new(AtomicUsize::new(0)),
            ports: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

// this is kind of ridiculous. i have to define this trait because it _needs_ to take
// an Arc(alpha), but we .. cant extend arc, except we can add a trait.

pub trait Adder {
    fn add(self, p: Port) -> Port;
}

impl Adder for Arc<Broadcast> {
    fn add(self, p: Port) -> Port {
        let index = self.count.fetch_add(1, Ordering::Acquire);
        let mut ports = self.ports.lock().expect("lock ports");
        ports.push((p, index));
        Arc::new(Ingress {
            broadcast: self.clone(),
            index,
        })
    }
}

impl Ingress {
    pub fn remove(&mut self, _p: Port) {}
}

impl Transport for Ingress {
    fn send(&self, b: Batch) {
        for i in &*self.broadcast.ports.lock().expect("lock") {
            if i.1 != self.index {
                i.0.send(b.clone())
            }
        }
    }
}

impl Transport for Broadcast {
    fn send(&self, b: Batch) {
        for i in &*self.ports.lock().expect("lock") {
            i.0.send(b.clone())
        }
    }
}
