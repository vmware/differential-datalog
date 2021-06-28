pub mod broadcast;
mod datfile;
pub mod ddvalue_batch;
mod dispatch;
mod display;
pub mod error;
mod forwarder;
mod json_framer;
pub mod process;
mod record_batch;
mod tcp_network;

use core::fmt;
use core::fmt::Display as CoreDisplay;
use std::sync::Arc;
use tokio::runtime::Runtime;

use differential_datalog::{ddval::DDValue, record::Record, D3logLocationId};

use crate::{
    ddvalue_batch::DDValueBatch, dispatch::Dispatch, display::Display, error::Error,
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
    DDValue(DDValueBatch),
    Record(RecordBatch),
}

pub trait Transport {
    // since most of these errors are async, we're adopting a general
    // policy for the moment of making all errors async and reported out
    // of band.

    // should really be type parametric shouldn't it?
    fn send(&self, b: Batch);
}

pub type Port = Arc<(dyn Transport + Send + Sync)>;

impl CoreDisplay for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Batch::Record(r) => r.fmt(f),
            Batch::DDValue(d) => d.fmt(f),
        }
    }
}

// input optional string is temporary demo plumbing
pub fn start_instance(e: Evaluator, uuid: u128, management: Port) -> Result<(), Error> {
    let rt = Runtime::new().unwrap();

    // pass rt?
    let _eg = rt.enter();

    let d = Dispatch::new(e.clone());
    let f = Forwarder::new(e.clone());

    // pass d to process manager
    d.clone().register(
        "d3_application::Process",
        Arc::new(ProcessManager::new(e.clone(), management.clone())),
    )?;

    // conditionalize - fact configuration
    let f2 = f.clone();
    let e2 = e.clone();
    let d2 = d.clone();
    let m2 = management.clone();

    tokio::spawn(async move {
        Display::new(8080, e2.clone(), m2.clone(), f2.clone(), m2.clone()).await;
    });

    // I _think_ that dropping rt is going to hold this thread until
    // the scheduler is empty, based on documentation. apparently not.
    let m2 = management.clone();
    let f2 = f.clone();
    rt.block_on(async move {
        tcp_bind(d2, uuid, f2, m2.clone(), e.clone(), m2.clone())
            .await
            .expect("bind")
    });
    Ok(())
}
