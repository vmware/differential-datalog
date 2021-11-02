//! A ThreadInstance is a D3log instance that executes within a separate thread

use crate::{
    async_error, batch, fact, function, send_error, Batch, BatchBody, Broadcast, Error,
    EvalFactory, Instance, RecordSet, Transport,
};

use differential_datalog::record::FromRecord;
use differential_datalog::record::{IntoRecord, Record};
use std::sync::{Arc, Mutex};

pub struct ThreadInstance {
    /// Parent instance that created this thread instance.
    parent: Arc<Instance>,
    new_evaluator: EvalFactory,
    /// All instances here have the same parent.
    instances: Mutex<Vec<Arc<Instance>>>, // to allow us to wire them up to each other
}

/// This Trait is invoked when a DDlog program inserts a row in the ThreadInstance
/// system relation, in effect performing a fork.
impl Transport for ThreadInstance {
    fn send(&self, b: Batch) {
        // There is currently no way to kill the created thread instances.
        // xxx handle deletes - this turned out to be more difficult than one
        //     might hope - no memory or scheduling isolation so we'd need to
        //     account for all the instance resources explcitly.

        for (_, p, weight) in &RecordSet::from(b).expect("batch") {
            let uuid_record = p.get_struct_field("id").unwrap();
            let uuid = async_error!(self.parent.error.clone(), u128::from_record(uuid_record));

            if weight != 1 {
                // Ideally negative weights should lead to a deletion, but that does not work yet.
                // Weight greater than 1 do not really make sense.
                async_error!(
                    self.parent.error.clone(),
                    Err("thread instance retraction not supported".to_string())
                );
            }

            let instance = async_error!(
                self.parent.error.clone(),
                Instance::new(self.parent.rt.clone(), self.new_evaluator.clone(), uuid)
            );

            // Each record inserted in the ThreadInstance relation represents a new
            // instance; we register here a forwarder for that instance.
            self.parent
                .forwarder
                .register(uuid, instance.eval_port.clone());

            let threads: u64 = 1;
            let bytes: u64 = 1;

            {
                // Register forwarders of this instance and of siblings to know about each other.
                let mut inst = self.instances.lock().expect("lock");
                for sibling in &(*inst) {
                    instance
                        .forwarder
                        .register(sibling.uuid, sibling.eval_port.clone());
                    sibling.forwarder.register(uuid, instance.eval_port.clone());
                }
                inst.push(instance.clone());
            }

            // Connect the broadcast bus of the parent with the broadcast bus of the new instance.
            async_error!(
                self.parent.error.clone(),
                Broadcast::couple(self.parent.broadcast.clone(), instance.broadcast.clone())
            );

            // Insert a row in the InstanceStatus relation.
            self.parent
                .broadcast
                .send(fact!(d3_application::InstanceStatus,
                            time => self.parent.now().into_record(),
                            id => uuid.into_record(),
                            memory_bytes => bytes.into_record(),
                            threads => threads.into_record()));
        }
    }
}

impl ThreadInstance {
    /// Creates the system ThreadInstance relation and
    /// registers a dispatcher listening for updates to this relation.
    pub fn new(instance: Arc<Instance>, new_evaluator: EvalFactory) -> Result<(), Error> {
        instance.clone().dispatch.clone().register(
            "d3_application::ThreadInstance",
            Arc::new(ThreadInstance {
                new_evaluator: new_evaluator.clone(),
                parent: instance.clone(),
                instances: Mutex::new(Vec::new()),
            }),
        )?;
        Ok(())
    }
}
