// the initial implementation of a distribution tree for replicated
// metadata. at minimum this will likely need to implement 'split horizon',
// where updates aren't sent back along the same branch to prevent loops.
// later, its likely that some form of spanning tree protocol be implemented

use crate::{Batch, Port, Transport};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct Ingress {
    index: usize,
    broadcast: Arc<Broadcast>,
}

#[derive(Clone)]
pub struct Broadcast {
    count: Arc<AtomicUsize>,
    ports: Vec<(Port, usize)>,
}

impl Broadcast {
    pub fn new() -> Broadcast {
        Broadcast {
            count: Arc::new(AtomicUsize::new(0)),
            ports: Vec::new(),
        }
    }
}

// this is kind of ridiculous. i have to define this trait because it _needs_ to take
// an Arc(alpha), but we .. cant extend arc, except we can add a trait.

pub trait Adder {
    fn add(self, p: Port) -> Port;
}

impl Adder for Arc<Broadcast> {
    fn add(mut self, p: Port) -> Port {
        let index = self.count.fetch_add(1, Ordering::Acquire);
        let b = Arc::get_mut(&mut self).unwrap();
        b.ports.push((p, index));
        Arc::new(Ingress {
            broadcast: self,
            index,
        })
    }
}

impl Ingress {
    pub fn remove(&mut self, _p: Port) {}
}

impl Transport for Ingress {
    fn send(&self, b: Batch) {
        for i in &self.broadcast.ports {
            if i.1 != self.index {
                i.0.send(b.clone())
            }
        }
    }
}

impl Transport for Broadcast {
    fn send(&self, b: Batch) {
        for i in &self.ports {
            i.0.send(b.clone())
        }
    }
}
