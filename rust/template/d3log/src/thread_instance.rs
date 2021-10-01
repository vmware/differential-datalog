use crate::{
    async_error, batch, fact, function, send_error, Batch, BatchBody, Broadcast, Error,
    EvalFactory, Instance, RecordSet, Transport,
};

use differential_datalog::record::FromRecord;
use differential_datalog::record::{IntoRecord, Record};
use std::sync::{Arc, Mutex};

pub struct ThreadInstance {
    parent: Arc<Instance>,
    new_evaluator: EvalFactory,
    instances: Mutex<Vec<Arc<Instance>>>, // to allow us to wire them up to each other
}

// xxx handle deletes - this turned out to be more difficult than one might hope - no memory or scheduling isolation
//     so we'd need to account for all the instance resources explcitly. have to do something
impl Transport for ThreadInstance {
    fn send(&self, b: Batch) {
        for (_, p, _weight) in &RecordSet::from(b).expect("batch") {
            let uuid_record = p.get_struct_field("id").unwrap();
            let uuid = async_error!(self.parent.eval.clone(), u128::from_record(uuid_record));

            let instance = async_error!(
                self.parent.eval,
                Instance::new(self.parent.rt.clone(), self.new_evaluator.clone(), uuid)
            );

            self.parent
                .forwarder
                .register(uuid, instance.eval_port.clone());

            let threads: u64 = 1;
            let bytes: u64 = 1;

            {
                let mut inst = self.instances.lock().expect("lock");
                for sibling in &(*inst) {
                    instance
                        .forwarder
                        .register(sibling.uuid, sibling.eval_port.clone());
                    sibling.forwarder.register(uuid, instance.eval_port.clone());
                }
                inst.push(instance.clone());
            }

            async_error!(
                self.parent.eval.clone(),
                Broadcast::couple(self.parent.broadcast.clone(), instance.broadcast.clone())
            );

            self.parent
                .broadcast
                .send(fact!(d3_application::InstanceStatus,
                            time => self.parent.eval.clone().now().into_record(),
                            id => uuid.into_record(),
                            memory_bytes => bytes.into_record(),
                            threads => threads.into_record()));
        }
    }
}

impl ThreadInstance {
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
