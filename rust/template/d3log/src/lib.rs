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
use std::time::{SystemTime, UNIX_EPOCH};

pub type Node = D3logLocationId;

type EvalFactory = Arc<dyn Fn() -> Result<(Evaluator, Batch), Error> + Send + Sync>;

pub trait EvaluatorTrait {
    fn ddvalue_from_record(&self, id: String, r: Record) -> Result<DDValue, Error>;
    fn eval(&self, input: Batch) -> Result<Batch, Error>;
    fn id_from_relation_name(&self, s: String) -> Result<RelId, Error>;
    fn localize(&self, rel: RelId, v: DDValue) -> Option<(Node, RelId, DDValue)>;
    fn record_from_ddvalue(&self, d: DDValue) -> Result<Record, Error>;
    fn relation_name_from_id(&self, id: RelId) -> Result<String, Error>;
    fn relation_deserializer(&self, id: RelId) -> Result<AnyDeserializeSeed, Error>;
}

pub type Evaluator = Arc<(dyn EvaluatorTrait + Send + Sync)>;

#[derive(Clone)]
pub struct Instance {
    pub uuid: Node,
    pub broadcast: Arc<Broadcast>,
    pub init_batch: Batch,
    pub eval_port: Port,
    pub eval: Evaluator,
    pub error: Arc<ErrorSend>,
    pub dispatch: Arc<Dispatch>,
    pub forwarder: Arc<Forwarder>,
    pub rt: Arc<tokio::runtime::Runtime>,
}

// idk what we're going to do should we ever need to keep state here
fn now_base() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

impl Instance {
    pub fn new(
        rt: Arc<Runtime>,
        new_evaluator: EvalFactory,
        uuid: u128,
    ) -> Result<Arc<Instance>, Error> {
        let broadcast = Broadcast::new();
        let (eval, init_batch) = new_evaluator()?;
        let error = Arc::new(ErrorSend {
            uuid,
            port: broadcast.clone(),
            now: now_base,
        });

        Broadcast::register_eval(broadcast.clone(), eval.clone(), error.clone());
        let dispatch = Arc::new(Dispatch::new());
        let forwarder = Forwarder::new(eval.clone());

        let (esend, erecv) = channel(20);
        let eval_port = Arc::new(EvalPort {
            rt: rt.clone(),
            error: error.clone(),
            dispatch: dispatch.clone(),
            send: esend,
        });

        let instance = Arc::new(Instance {
            init_batch,
            uuid,
            rt,
            error: error.clone(),
            broadcast: broadcast.clone(),
            eval: eval.clone(),
            eval_port: eval_port.clone(),
            forwarder: forwarder.clone(),
            dispatch: dispatch.clone(),
        });

        Instance::eval_loop(instance.clone(), erecv);
        broadcast.clone().subscribe(eval_port.clone());
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
                    i2.dispatch.send(b.clone());
                    let out = async_error!(i2.error.clone(), i2.eval.eval(b.clone()));
                    i2.dispatch.send(out.clone());
                    i2.forwarder.send(out.clone());
                } else {
                    panic!("eval shut down");
                }
            }
        });
    }

    // doesn't belong here. but we'd like a monotonic wallclock
    // to sequence system events. Also - it would be nice if ddlog
    // had some basic time functions (format)
    fn now(&self) -> u64 {
        1
    }
}

pub trait Transport {
    // since most of these errors are async, we're adopting a general
    // policy for the moment of making all errors async and reported out
    // of band.

    // should really be type parametric shouldn't it?
    fn send(&self, batch: Batch);
}

pub type Port = Arc<(dyn Transport + Send + Sync)>;

#[derive(Clone)]
struct EvalPort {
    rt: Arc<tokio::runtime::Runtime>,
    error: Arc<ErrorSend>,
    dispatch: Port,
    send: Sender<Batch>,
}

impl Transport for EvalPort {
    fn send(&self, batch: Batch) {
        let cl = self.clone();
        self.rt.spawn(async move {
            async_error!(cl.error, cl.send.send(batch.clone()).await);
        });
    }
}

#[derive(Clone)]
pub struct ErrorSend {
    now: fn() -> u64,
    uuid: Node,
    port: Port,
}

impl ErrorSend {
    fn error(&self, text: String, line: u32, filename: String, functionname: String) {
        let f = fact!(d3_application::Error,
                      now => (self.now)().into_record(),
                      uuid => self.uuid.into_record(),
                      text => text.into_record(),
                      line => line.into_record(),
                      instance => self.uuid.clone().into_record(),
                      filename => Record::String(filename),
                      functionname => Record::String(functionname));
        self.port.clone().send(f);
    }
}
