//! This code provides a bridge between a DDlog application and the D3Runtime that
//! runs it.  To start a D3log application, you start a DDlog program with the -d3log
//! command-line argument.  Doing this will start the first instance of a process
//! in the D3Runtime; additional instances are created from the first one by 'forking'
//! new processes (or threads).

use crate::{relval_from_record, run_with_config, Relations};

use d3log::{
    batch::{Batch, BatchBody, Properties},
    broadcast::PubSub,
    error::Error,
    json_framer::JsonFramer,
    value_set::ValueSet,
    Evaluator, EvaluatorTrait, Instance, Node, Transport,
};
use differential_datalog::{
    api::*,
    ddval::AnyDeserializeSeed,
    ddval::DDValue,
    program::{config::Config, RelId, Update},
    record::IntoRecord,
    record::Record,
    record::RelIdentifier,
    D3log, DDlog, DDlogDynamic,
};

use rand::Rng;
use std::convert::TryFrom;
use std::fs;
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;

// The equivalent of the Unix 'init' process: the first DDlog instance created.
pub struct D3 {
    hddlog: HDDlog,
    eval: Arc<Mutex<Option<Evaluator>>>, // a reference to myself to use with other apis
}

/// Read the initial facts (facts that are inserted in relations prior to the execution
/// of the program) from a Json file.
pub fn read_json_file(
    eval: Evaluator,
    /// File to read.
    filename: String,
    /// Callback to invoke for every batch read from the file.
    cb: &mut dyn FnMut(Batch),
) -> Result<(), Error> {
    let body = fs::read_to_string(filename)?;
    let mut jf = JsonFramer::new();
    for i in jf.append(body.as_bytes())?.into_iter() {
        let rs = Batch::deserialize(i, eval.clone())?;
        cb(rs);
    }
    Ok(())
}

impl D3 {
    /// Start the execution of an new D3 distributed runtime.
    /// Returns an evaluator and the batch of outputs produced by it's execution.
    pub fn new() -> Result<(Evaluator, Batch), Error> {
        // xxx - looks like the 'run' variant is no longer really accessible because
        // of namespace collisions?
        let config = Config::new().with_timely_workers(1);
        let (hddlog, init_output) = run_with_config(config, false)?;
        let ad = Arc::new(D3 {
            hddlog,
            eval: Arc::new(Mutex::new(None)),
        });
        *ad.eval.lock().expect("lock") = Some(ad.clone());
        Ok((
            ad.clone(),
            ValueSet::from_delta_map(ad.clone(), init_output),
        ))
    }
}

// This is the only implementation of this trait.
impl EvaluatorTrait for D3 {
    // FromRecord for DDValue not implemented. is there another official path here?

    // Convert a Record to a DDValue
    fn ddvalue_from_record(&self, name: String, r: Record) -> Result<DDValue, Error> {
        let id = self.id_from_relation_name(name.clone())?;
        let t: RelIdentifier = RelIdentifier::RelId(id);
        // xxx global reference Relations
        let rel = Relations::try_from(&t).expect("huh");
        relval_from_record(rel, &r)
            .map_err(|x| Error::new("bad record conversion: ".to_string() + &x.to_string()))
    }

    /// Given a batch of data, feed it to the as an input transaction.
    /// Returns the outputs produced by this transaction.
    fn eval(&self, input: Batch) -> Result<Batch, Error> {
        let eval = (*self.eval.lock().expect("lock")).clone().unwrap().clone();
        let vb = ValueSet::from(eval.clone(), input)?;
        let mut upd = Vec::new();

        for (relid, v, w) in &vb {
            match w {
                1 => upd.push(Update::Insert { relid, v }),
                -1 => upd.push(Update::DeleteValue { relid, v }),
                _ => panic!("non-unit weights not supported"),
            }
        }

        self.hddlog.transaction_start()?;
        self.hddlog.apply_updates(&mut upd.clone().drain(..))?;
        Ok(ValueSet::from_delta_map(
            eval.clone(),
            self.hddlog.transaction_commit_dump_changes()?,
        ))
    }

    /// Get the relation id from a string
    fn id_from_relation_name(&self, s: String) -> Result<RelId, Error> {
        // Relations is a global - ideally we should be going through hddlog, not clear how?
        let s: &str = &s;
        match Relations::try_from(s) {
            Ok(r) => Ok(r as RelId),
            Err(_) => Err(Error::new(format!("bad relation {}", s))),
        }
    }

    /// Convert a global DDValue into a DDValue for the local corresponding relation name.
    fn localize(&self, rel: usize, v: DDValue) -> Option<(Node, RelId, DDValue)> {
        match self.hddlog.d3log_localize_val(rel, v.clone()) {
            Ok((Some(n), r, v)) => Some((n, r, v)),
            Ok((None, _, _)) => None,
            Err(_) => None,
        }
    }

    /// Convert a DDvalue to a record.
    fn record_from_ddvalue(&self, d: DDValue) -> Result<Record, Error> {
        Ok(d.into_record())
    }

    /// Get a relation name from a relation id.
    fn relation_name_from_id(&self, id: RelId) -> Result<String, Error> {
        Ok(self.hddlog.inventory.get_table_name(id)?.to_string())
    }

    fn relation_deserializer(&self, id: RelId) -> Result<AnyDeserializeSeed, Error> {
        if let Some(x) = self
            .hddlog
            .any_deserialize
            .get_deserialize(id)
            .map(AnyDeserializeSeed::new)
        {
            Ok(x)
        } else {
            Err(Error::new("bad deserializing relation".to_string()))
        }
    }
}

/// Used for debugging.  Batches sent to this port are printed on the screen as JSON.
pub struct JsonDebugPort {
    eval: Evaluator,
}

impl Transport for JsonDebugPort {
    fn send(&self, b: Batch) {
        let vb = Batch {
            metadata: Properties::new(),
            body: BatchBody::Value(
                ValueSet::from(self.eval.clone(), b.clone()).expect("value batch"),
            ),
        };
        println!(
            "{}",
            std::str::from_utf8(&vb.clone().serialize().expect("ser")).expect("utf")
        );
    }
}

/// Start the d3log distributed runtime.  If the environment variable uuid is set,
/// use it as an id for the first instance, else generate a random id.
/// `inputfile`: if supplied, contains the initial facts in JSON
pub fn start_d3log(debug_broadcast: bool, inputfile: Option<String>) -> Result<(), Error> {
    let uuid = if let Some(uuid) = std::env::var_os("uuid") {
        if let Some(uuid) = uuid.to_str() {
            uuid.parse::<u128>().unwrap()
        } else {
            panic!("bad uuid");
        }
    } else {
        // use uuid crate
        u128::from_be_bytes(rand::thread_rng().gen::<[u8; 16]>())
    };

    let d = move || -> Result<(Evaluator, Batch), Error> { D3::new() };

    let rt = Arc::new(Runtime::new()?);
    let instance = Instance::new(rt.clone(), Arc::new(d), uuid)?;

    if debug_broadcast {
        instance
            .broadcast
            .clone()
            .subscribe(Arc::new(JsonDebugPort {
                eval: instance.eval.clone(),
            }));
    }

    if let Some(f) = inputfile {
        if let Err(x) = read_json_file(instance.eval.clone(), f, &mut |b: Batch| {
            instance.eval_port.send(b.clone());
        }) {
            println!("json err {}", x);
            panic!("json");
        }
    }

    // this function doesn't return, because the way its called in src/main.rs it would re-run the
    // instance outside d3 and exit. we can change the plumbing there. in any case, we dont actually
    // want to exit since we're running as a service.
    loop {
        std::thread::park();
    }
}
