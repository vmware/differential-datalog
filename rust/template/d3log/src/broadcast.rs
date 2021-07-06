// the initial implementation of a distribution tree for replicated
// metadata. at minimum this will likely need to implement 'split horizon',
// where updates aren't sent back along the same branch to prevent loops.
// later, its likely that some form of spanning tree protocol be implemented

use crate::{Batch, Port, Transport};

#[derive(Default)]
pub struct Broadcast {
    ports: Vec<Port>,
}

impl Broadcast {
    pub fn new() -> Broadcast {
        Broadcast { ports: Vec::new() }
    }

    // yields correlator
    //    pub fn add(&mut self, p: Port) {
    //        self.ports.push(p);
    //    }

    //    pub fn remove(&mut self, _p: Port) {}
}

impl Transport for Broadcast {
    fn send(&self, b: Batch) {
        for i in &self.ports {
            i.send(b.clone());
        }
    }
}
