pub mod batch;
pub mod broadcast;
mod dispatch;
pub mod error;
mod forwarder;
pub mod json_framer;
pub mod record_set;
pub mod thread_instance;
pub mod value_set;

use differential_datalog::{
    ddval::{AnyDeserializeSeed, DDValue},
    program::RelId,
    record::*,
    D3logLocationId,
};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::{
    batch::{Batch, BatchBody, Properties},
    broadcast::{Broadcast, PubSub},
    dispatch::Dispatch,
    error::Error,
    forwarder::Forwarder,
    record_set::RecordSet,
    thread_instance::ThreadInstance,
    value_set::ValueSet,
};

pub type Node = D3logLocationId;

type EvalFactory = Arc<dyn Fn(Node, Port) -> Result<(Evaluator, Batch), Error> + Send + Sync>;

pub trait EvaluatorTrait {
    fn ddvalue_from_record(&self, id: String, r: Record) -> Result<DDValue, Error>;
    fn eval(&self, input: Batch) -> Result<Batch, Error>;
    fn id_from_relation_name(&self, s: String) -> Result<RelId, Error>;
    fn localize(&self, rel: RelId, v: DDValue) -> Option<(Node, RelId, DDValue)>;
    fn record_from_ddvalue(&self, d: DDValue) -> Result<Record, Error>;
    fn relation_name_from_id(&self, id: RelId) -> Result<String, Error>;
    fn relation_deserializer(&self, id: RelId) -> Result<AnyDeserializeSeed, Error>;

    // these methods probably belong in instance?
    fn now(&self) -> u64;
    fn myself(&self) -> Node;
    fn error(&self, text: Record, line: Record, filename: Record, functionname: Record);
}

pub type Evaluator = Arc<(dyn EvaluatorTrait + Send + Sync)>;

#[derive(Clone)]
pub struct Instance {
    pub uuid: Node,
    pub broadcast: Arc<Broadcast>,
    pub init_batch: Batch,
    pub eval_port: Port,
    pub eval: Evaluator,
    pub dispatch: Arc<Dispatch>,
    pub forwarder: Arc<Forwarder>,
    pub rt: Arc<tokio::runtime::Runtime>,
}

impl Instance {
    pub fn new(
        rt: Arc<Runtime>,
        new_evaluator: EvalFactory,
        uuid: u128,
    ) -> Result<Arc<Instance>, Error> {
        let broadcast = Broadcast::new(uuid);
        let (eval, init_batch) = new_evaluator(uuid, broadcast.clone())?;
        Broadcast::register_eval(broadcast.clone(), eval.clone());
        let dispatch = Arc::new(Dispatch::new(eval.clone()));
        let forwarder = Forwarder::new(eval.clone());

        let (esend, erecv) = channel(20);
        let eval_port = Arc::new(EvalPort {
            rt: rt.clone(),
            eval: eval.clone(),
            dispatch: dispatch.clone(),
            s: esend,
        });

        let instance = Arc::new(Instance {
            init_batch,
            uuid,
            rt,
            eval_port: eval_port.clone(),
            broadcast: broadcast.clone(),
            forwarder: forwarder.clone(),
            dispatch: dispatch.clone(),
            eval: eval.clone(),
        });

        Instance::eval_loop(instance.clone(), erecv);
        broadcast.clone().subscribe(eval_port.clone(), u128::MAX);
        ThreadInstance::new(instance.clone(), new_evaluator)?;
        eval_port.send(fact!(d3_application::Myself, me => uuid.into_record()));
        Ok(instance)
    }

    // doesn't seem work defining a trait just so we can get 'self : Arc<Instance>'
    fn eval_loop(instance: Arc<Instance>, mut erecv: Receiver<Batch>) {
        let i2 = instance.clone();
        i2.clone().rt.spawn(async move {
            loop {
                if let Some(b) = erecv.recv().await {
                    let e = i2.eval.clone();
                    i2.dispatch.send(b.clone());
                    let out = async_error!(e.clone(), e.eval(b.clone()));
                    i2.dispatch.send(out.clone());
                    i2.forwarder.send(out.clone());
                } else {
                    panic!("eval shut down");
                }
            }
        });
    }
}

pub trait Transport {
    // since most of these errors are async, we're adopting a general
    // policy for the moment of making all errors async and reported out
    // of band.

    // should really be type parametric shouldn't it?
    fn send(&self, b: Batch);
}

pub type Port = Arc<(dyn Transport + Send + Sync)>;

struct EvalPort {
    rt: Arc<tokio::runtime::Runtime>,
    eval: Evaluator,
    dispatch: Port,
    s: Sender<Batch>,
}

impl Transport for EvalPort {
    fn send(&self, b: Batch) {
        self.dispatch.send(b.clone());
        let eclone = self.eval.clone();
        let sclone = self.s.clone();
        self.rt.spawn(async move {
            async_error!(eclone, sclone.send(b.clone()).await);
        });
    }
}
