pub mod broadcast;
mod datfile;
pub mod ddvalue_batch;
mod dispatch;
pub mod error;
mod forwarder;
mod json_framer;
pub mod process;
pub mod record_batch;
mod tcp_network;

use core::fmt;
use differential_datalog::{ddval::DDValue, record::*, D3logLocationId};
use std::borrow::Cow;
use std::fmt::Display;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::{
    broadcast::Broadcast, ddvalue_batch::DDValueBatch, dispatch::Dispatch, error::Error,
    forwarder::Forwarder, process::ProcessManager, record_batch::RecordBatch,
    tcp_network::tcp_bind,
};

pub type Node = D3logLocationId;

pub trait EvaluatorTrait {
    fn ddvalue_from_record(&self, id: usize, r: Record) -> Result<DDValue, Error>;
    fn eval(&self, input: Batch) -> Result<Batch, Error>;
    fn id_from_relation_name(&self, s: String) -> Result<usize, Error>;
    fn localize(&self, rel: usize, v: DDValue) -> Option<(Node, usize, DDValue)>;
    fn now(&self) -> u64;
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

pub fn start_instance(
    rt: Arc<Runtime>,
    eval: Evaluator,
    uuid: u128,
    b: Arc<Broadcast>,
) -> Result<(Port, tokio::task::JoinHandle<()>), Error> {
    let dispatch = Dispatch::new(eval.clone());
    //race between registration and new data.

    // TODO: Create an Instance manager and register with the dispatcher.
    // Instance manager's send will create new instances
    let forwarder = Forwarder::new(eval.clone());

    // pass d to process manager for it to register itself
    dispatch
        .clone()
        .register(
            "d3_application::Process",
            Arc::new(ProcessManager::new(eval.clone(), rt.clone(), b.clone())),
        )
        .expect("registration failed");

    let management_clone = b.clone();
    let forwarder_clone = forwarder.clone();

    let eval_clone = eval.clone();
    let dispatch_clone = dispatch.clone();

    // TODO: rt.block_on never returns. The idea is to spawn a thread and return the thread handle
    // for that to the main thread. The main thread just waits on this thread join handle as long
    // as the thread is running.
    let handle = rt.spawn(async move {
        async_error!(
            management_clone.clone(),
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
    Ok((Arc::new(dispatch), handle))
}
