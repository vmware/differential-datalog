use crate::{relval_from_record, run_with_config, Relations};

use d3log::{
    batch::Batch, batch::BatchBody, broadcast::PubSub, error::Error, fact, json_framer::JsonFramer,
    record_set::RecordSet, value_set::ValueSet, Evaluator, EvaluatorTrait, Instance, Node, Port,
    Transport,
};
use differential_datalog::{
    api::*, ddval::AnyDeserializeSeed, ddval::DDValue, program::config::Config, program::Update,
    record::IntoRecord, record::Record, record::RelIdentifier, D3log, DDlog, DDlogDynamic,
};

use rand::Rng;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::runtime::Runtime;

pub struct D3 {
    uuid: u128,
    error: Port,
    h: HDDlog,
    e: Arc<Mutex<Option<Evaluator>>>, // a reference to myself to use with other apis
}

pub fn read_json_file(
    e: Evaluator,
    filename: String,
    cb: &mut dyn FnMut(Batch),
) -> Result<(), Error> {
    let body = fs::read_to_string(filename)?;
    let mut jf = JsonFramer::new();
    for i in jf.append(body.as_bytes())?.into_iter() {
        let rs = Batch::deserialize(i, e.clone())?;
        cb(rs);
    }
    Ok(())
}

impl D3 {
    pub fn new(uuid: u128, error: Port) -> Result<(Evaluator, Batch), Error> {
        // xxx - looks like the 'run' variant is no longer really accessible because
        // of namespace collisions?
        let config = Config::new().with_timely_workers(1);
        let (h, init_output) = run_with_config(config, false)?;
        let ad = Arc::new(D3 {
            h,
            uuid,
            error,
            e: Arc::new(Mutex::new(None)),
        });
        *ad.e.lock().expect("lock") = Some(ad.clone());
        Ok((
            ad.clone(),
            ValueSet::from_delta_map(ad.clone(), init_output),
        ))
    }
}

// xxx - remove globals
impl EvaluatorTrait for D3 {
    // FromRecord for DDValue not implemented. is there another official path here?
    fn ddvalue_from_record(&self, name: String, r: Record) -> Result<DDValue, Error> {
        let id = self.id_from_relation_name(name.clone())?;
        let t: RelIdentifier = RelIdentifier::RelId(id);
        let rel = Relations::try_from(&t).expect("huh");
        relval_from_record(rel, &r)
            .map_err(|x| Error::new("bad record conversion: ".to_string() + &x.to_string()))
    }

    fn myself(&self) -> Node {
        self.uuid
    }

    fn error(&self, text: Record, line: Record, filename: Record, functionname: Record) {
        let f = fact!(d3_application::Error,
                      text => text,
                      line => line,
                      instance => self.uuid.clone().into_record(),
                      filename => filename,
                      functionname => functionname);
        self.error.clone().send(f);
    }

    // input shouldn't be a value batch with a different evaluator?
    fn eval(&self, input: Batch) -> Result<Batch, Error> {
        let eval = (*self.e.lock().expect("lock")).clone().unwrap().clone();
        let vb = ValueSet::from(eval.clone(), input)?;
        let mut upd = Vec::new();

        let bv = Batch {
            metadata: HashMap::new(),
            body: BatchBody::Value(vb.clone()),
        }
        .serialize()
        .expect("serialize");

        Batch::deserialize(bv, eval.clone()).expect("deser");

        for (relid, v, _) in &vb {
            upd.push(Update::Insert { relid, v });
        }

        self.h.transaction_start()?;
        self.h.apply_updates(&mut upd.clone().drain(..))?;
        Ok(ValueSet::from_delta_map(
            eval.clone(),
            self.h.transaction_commit_dump_changes()?,
        ))
    }

    fn id_from_relation_name(&self, s: String) -> Result<usize, Error> {
        let s: &str = &s;
        match Relations::try_from(s) {
            Ok(r) => Ok(r as usize),
            Err(_) => Err(Error::new(format!("bad relation {}", s))),
        }
    }

    fn localize(&self, rel: usize, v: DDValue) -> Option<(Node, usize, DDValue)> {
        match self.h.d3log_localize_val(rel, v.clone()) {
            Ok((Some(n), r, v)) => Some((n, r, v)),
            Ok((None, _, _)) => None,
            Err(_) => None,
        }
    }

    // doesn't belong here. but we'd like a monotonic wallclock
    // to sequence system events. Also - it would be nice if ddlog
    // had some basic time functions (format)
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    }

    fn record_from_ddvalue(&self, d: DDValue) -> Result<Record, Error> {
        Ok(d.into_record())
    }

    fn relation_name_from_id(&self, id: usize) -> Result<String, Error> {
        Ok(self.h.inventory.get_table_name(id)?.to_string())
    }

    fn relation_deserializer(&self, id: usize) -> Result<AnyDeserializeSeed, Error> {
        if let Some(x) = self
            .h
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

pub struct DebugPort {}

impl Transport for DebugPort {
    fn send(&self, b: Batch) {
        // print meta
        for (_r, f, w) in &RecordSet::from(b.clone()).expect("batch translate") {
            println!("{} {}", f, w);
        }
    }
}

pub struct JsonDebugPort {
    eval: Evaluator,
}

impl Transport for JsonDebugPort {
    fn send(&self, b: Batch) {
        let vb = Batch {
            metadata: HashMap::new(),
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

pub fn start_d3log(debug_broadcast: bool, inputfile: Option<String>) -> Result<(), Error> {
    let (uuid, is_parent) = if let Some(uuid) = std::env::var_os("uuid") {
        if let Some(uuid) = uuid.to_str() {
            let my_uuid = uuid.parse::<u128>().unwrap();
            (my_uuid, false)
        } else {
            panic!("bad uuid");
        }
    } else {
        // use uuid crate
        (
            u128::from_be_bytes(rand::thread_rng().gen::<[u8; 16]>()),
            true,
        )
    };

    let d =
        move |id: u128, error: Port| -> Result<(Evaluator, Batch), Error> { D3::new(id, error) };

    let rt = Arc::new(Runtime::new()?);
    let instance = Instance::new(rt.clone(), Arc::new(d), uuid)?;

    if debug_broadcast {
        instance.broadcast.clone().subscribe(
            Arc::new(JsonDebugPort {
                eval: instance.eval.clone(),
            }),
            u128::MAX,
        );
    }

    if is_parent {
        let debug_uuid = u128::from_be_bytes(rand::thread_rng().gen::<[u8; 16]>());

        instance
            .broadcast
            .clone()
            .send(fact!(d3_application::Stdout, target=>debug_uuid.into_record()));
        instance.broadcast.clone().send(
            fact!(d3_application::Forward, target=>debug_uuid.into_record(), intermediate => uuid.into_record()),
        );
    }

    if let Some(f) = inputfile {
        match read_json_file(instance.eval.clone(), f, &mut |b: Batch| {
            instance.eval_port.send(b.clone());
        }) {
            Err(x) => {
                println!("json err {}", x);
                panic!("json");
            }
            _ => (),
        }
    }

    loop {
        std::thread::park();
    }
}
