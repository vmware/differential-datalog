pub mod broadcast;
pub mod ddvalue_batch;
mod dispatch;
pub mod error;
mod forwarder;
mod json_framer;
pub mod record_batch;
mod tcp_network;

use core::fmt;
use differential_datalog::{ddval::DDValue, record::*, D3logLocationId};
use std::borrow::Cow;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

use crate::{
    broadcast::{Broadcast, PubSub},
    ddvalue_batch::DDValueBatch,
    dispatch::Dispatch,
    error::Error,
    forwarder::Forwarder,
    record_batch::RecordBatch,
    tcp_network::tcp_bind,
};

pub type Node = D3logLocationId;

pub trait EvaluatorTrait {
    fn ddvalue_from_record(&self, id: String, r: Record) -> Result<DDValue, Error>;
    fn eval(&self, input: Batch) -> Result<Batch, Error>;
    fn id_from_relation_name(&self, s: String) -> Result<usize, Error>;
    fn localize(&self, rel: usize, v: DDValue) -> Option<(Node, usize, DDValue)>;
    fn now(&self) -> u64;
    fn myself(&self) -> Node;
    fn error(&self, text: String, line: Record, filename: Record, functionname: Record);
    fn record_from_ddvalue(&self, d: DDValue) -> Result<Record, Error>;
    fn relation_name_from_id(&self, id: usize) -> Result<String, Error>;

    // these is ddvalue/relationid specific
    fn serialize_batch(&self, b: DDValueBatch) -> Result<Vec<u8>, Error>;
    fn deserialize_batch(&self, s: Vec<u8>) -> Result<DDValueBatch, Error>;
}

pub type Evaluator = Arc<(dyn EvaluatorTrait + Send + Sync)>;

#[derive(Clone)]
pub enum Batch {
    Value(DDValueBatch),
    Rec(RecordBatch),
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Batch [").unwrap();
        match self {
            Batch::Value(b) => b.fmt(f),
            Batch::Rec(b) => b.fmt(f),
        }
        .unwrap();
        writeln!(f, "\n]\n")
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

struct AccumulatePort {
    eval: Evaluator,
    b: Arc<Mutex<DDValueBatch>>,
}

impl Transport for AccumulatePort {
    fn send(&self, b: Batch) {
        println!("{}", b);
        for (r, f, w) in &DDValueBatch::from(&(*self.eval), b).expect("iterator") {
            self.b.lock().expect("lock").insert(r, f, w);
        }
    }
}

use std::collections::VecDeque;

struct EvalPort {
    eval: Evaluator,
    forwarder: Port,
    dispatch: Port,
    queue: Arc<Mutex<VecDeque<Batch>>>,
}

impl Transport for EvalPort {
    fn send(&self, b: Batch) {
        for (r, f, w) in &RecordBatch::from(self.eval.clone(), b.clone()) {
            println!("in: {} {} {}", r, f, w);
        }
        self.queue.lock().expect("lock").push_back(b);
        while let Some(b) = self.queue.lock().expect("lock").pop_front() {
            let out = async_error!(self.eval.clone(), self.eval.eval(b));
            println!("out {}", out);
            self.dispatch.send(out.clone());
            self.forwarder.send(out.clone());
        }
    }
}

struct ThreadInstance {
    rt: Arc<tokio::runtime::Runtime>,
    eval: Evaluator,
    new_evaluator: Arc<dyn Fn() -> Result<(Evaluator, Batch), Error> + Send + Sync>,
    forwarder: Arc<Forwarder>,
    broadcast: Arc<Broadcast>,
    // process needs this too
    accumulator: Arc<Mutex<DDValueBatch>>,
}

// we're just throwing this into the same runtime - do we want/need scheduling isolation?
// xxx handle deletes
impl Transport for ThreadInstance {
    fn send(&self, b: Batch) {
        for (_, p, _weight) in &RecordBatch::from(self.eval.clone(), b) {
            // async_error variant for Some
            let uuid_record = p.get_struct_field("id").unwrap();
            let uuid = async_error!(self.eval.clone(), u128::from_record(uuid_record));

            let (_p, _init_batch, ep, _jh) = async_error!(
                self.eval,
                start_instance(self.rt.clone(), self.new_evaluator.clone(), uuid)
            );

            ep.send(Batch::Value(self.accumulator.lock().expect("lock").clone()));

            // deadlock
            self.forwarder.register(uuid, ep);
            let threads: u64 = 1;
            let bytes: u64 = 1;

            self.broadcast.send(fact!(d3_application::InstanceStatus,
                                      time => self.eval.clone().now().into_record(),
                                      id => uuid.into_record(),
                                      memory_bytes => bytes.into_record(),
                                      threads => threads.into_record()));
            // encoding none ? we should have a better termination report.
        }
    }
}

struct DebugPort {
    eval: Evaluator,
}

impl Transport for DebugPort {
    fn send(&self, b: Batch) {
        for (r, f, w) in &RecordBatch::from(self.eval.clone(), b) {
            println!("{} {} {}", r, f, w);
        }
    }
}

pub fn start_instance(
    rt: Arc<Runtime>,
    new_evaluator: Arc<dyn Fn() -> Result<(Evaluator, Batch), Error> + Send + Sync>,
    uuid: u128,
) -> Result<(Port, Batch, Port, tokio::task::JoinHandle<()>), Error> {
    let (eval, init_batch) = new_evaluator()?;
    let dispatch = Arc::new(Dispatch::new(eval.clone()));
    let broadcast = Broadcast::new();

    broadcast.clone().subscribe(dispatch.clone());
    broadcast
        .clone()
        .subscribe(Arc::new(DebugPort { eval: eval.clone() }));
    let forwarder = Arc::new(Forwarder::new(eval.clone(), broadcast.clone()));
    let accu_batch = Arc::new(Mutex::new(DDValueBatch::new()));

    dispatch.clone().register(
        "d3_application::ThreadInstance",
        Arc::new(ThreadInstance {
            rt: rt.clone(),
            accumulator: accu_batch.clone(),
            eval: eval.clone(),
            forwarder: forwarder.clone(),
            new_evaluator: new_evaluator.clone(),
            broadcast: broadcast.clone(),
        }),
    )?;

    // shouldn't evaluator just implement Transport?
    let eval_port = Arc::new(EvalPort {
        forwarder: forwarder.clone(),
        dispatch: dispatch.clone(),
        eval: eval.clone(),
        queue: Arc::new(Mutex::new(VecDeque::new())),
    });
    broadcast.clone().subscribe(eval_port.clone());
    broadcast.clone().subscribe(Arc::new(AccumulatePort {
        eval: eval.clone(),
        b: accu_batch.clone(),
    }));
    eval_port.send(fact!(d3_application::Myself, me => uuid.into_record()));

    let management_clone = broadcast.clone();
    let forwarder_clone = forwarder.clone();
    let eval_clone = eval.clone();
    let dispatch_clone = dispatch.clone();

    let handle = rt.spawn(async move {
        async_error!(
            eval.clone(),
            tcp_bind(
                dispatch_clone,
                uuid,
                forwarder_clone,
                management_clone.clone(),
                eval_clone.clone(),
                management_clone.clone(),
            )
            .await
        );
    });
    Ok((broadcast, init_batch, eval_port, handle))
}
