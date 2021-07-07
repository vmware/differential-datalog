use d3log::{
    broadcast::Broadcast,
    ddvalue_batch::DDValueBatch,
    error::Error,
    process::{FileDescriptorPort, MANAGEMENT_OUTPUT_FD},
    start_instance, Batch, Evaluator, EvaluatorTrait, Node, Port, Transport,
};

use crate::{api::HDDlog, relid2name, relval_from_record, Relations, UpdateSerializer};

use differential_datalog::{
    ddval::DDValue, program::Update, record::IntoRecord, record::Record, D3log, DDlog, DDlogDynamic,
};
use rand::Rng;
use serde::{de, de::SeqAccess, de::Visitor, Deserialize, Deserializer};
use serde::{ser::SerializeTuple, Serialize, Serializer};
use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json;
use tokio::runtime::Runtime;

pub struct Null {}
impl Transport for Null {
    fn send(&self, _rb: Batch) {}
}

pub struct Print(pub Port);

impl Transport for Print {
    fn send(&self, b: Batch) {
        println!("{}", b);
        self.0.send(b);
    }
}

pub struct D3 {
    h: HDDlog,
}

struct SerializeBatchWrapper {
    b: DDValueBatch,
}

struct BatchVisitor {}

impl<'de> Visitor<'de> for BatchVisitor {
    type Value = DDValueBatch;

    // his just formats an error message..in advance?
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "batch")
    }

    fn visit_seq<E>(self, mut e: E) -> Result<Self::Value, E::Error>
    where
        E: SeqAccess<'de>,
    {
        let bn = DDValueBatch::new();
        {
            let mut b = bn.0.lock().unwrap();

            let timestamp: Option<u64> = e.next_element()?;
            match timestamp {
                Some(timestamp) => b.timestamp = timestamp,
                None => return Err(de::Error::custom("expected integer timestamp")),
            }

            let updates: Option<Vec<UpdateSerializer>> = e.next_element()?;
            match updates {
                Some(updates) => {
                    for i in updates {
                        let u = Update::<DDValue>::from(i);
                        match u {
                            // insert method?
                            Update::Insert { relid, v } => b.deltas.update(relid, &v, 1),
                            _ => return Err(de::Error::custom("invalid value")),
                        }
                    }
                }
                None => return Err(de::Error::custom("unable to parse update set")),
            }
        }
        Ok(bn)
    }
}

impl<'de> Deserialize<'de> for SerializeBatchWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let b: DDValueBatch = deserializer.deserialize_any(BatchVisitor {})?;
        Ok(SerializeBatchWrapper { b })
    }
}

impl Serialize for SerializeBatchWrapper {
    // i would _like_ to expose an interface that used the names for relations
    // so that external users dont have to be privy to the compiler id assignemnt

    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tup = serializer.serialize_tuple(3)?; //maybe map at the top level is better
        let mut updates = Vec::new();
        let b = &self.b;
        for (relid, v, _) in b {
            updates.push(UpdateSerializer::from(Update::Insert {
                relid,
                v: v.clone(),
            }));
        }
        tup.serialize_element(&b.0.lock().unwrap().timestamp)?;
        tup.serialize_element(&updates)?;
        tup.end()
    }
}

impl D3 {
    pub fn new(_uuid: Node) -> Result<(Evaluator, Batch), Error> {
        let (h, init_output) = HDDlog::run(1, false)?;
        Ok((
            Arc::new(D3 { h }),
            DDValueBatch::from_delta_map(init_output),
        ))
    }
}

impl EvaluatorTrait for D3 {
    fn ddvalue_from_record(&self, id: usize, r: Record) -> Result<DDValue, Error> {
        //  as Relations
        let rel = Relations::try_from(id).map_err(|_| Error::new("bad relation id".to_string()))?;
        relval_from_record(rel, &r).map_err(|_| Error::new("bad record conversion".to_string()))
    }

    //  can demux on record batch and call the record interface instead of translating - is that
    // desirable in some way? record in record out?
    fn eval(&self, input: Batch) -> Result<Batch, Error> {
        // would like to implicitly convert batch to ddvalue_batch, but i cant, because i need an
        // evaluator, and its been deconstructed before we get here...
        match input {
            Batch::Value(b) => {
                let mut upd = Vec::new();
                for (relid, v, _) in &b {
                    upd.push(Update::Insert { relid, v });
                }

                // wrapper to translate hddlog's string error to our standard-by-default std::io::Error
                match (|| -> Result<Batch, String> {
                    self.h.transaction_start()?;
                    self.h.apply_updates(&mut upd.clone().drain(..))?;
                    Ok(Batch::from(DDValueBatch::from_delta_map(
                        self.h.transaction_commit_dump_changes()?,
                    )))
                })() {
                    Ok(x) => Ok(x),
                    Err(e) => Err(Error::new(
                        format!("Failed to update differential datalog: {}", e).to_string(),
                    )),
                }
            }
            _ => panic!("bad batch"),
        }
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
        match relid2name(id) {
            Some(x) => Ok(x.to_string()),
            None => Err(Error::new("unknown relation id".to_string())),
        }
    }

    // xxx - actually we want to parameterize on format, not on internal representation
    fn serialize_batch(&self, b: DDValueBatch) -> Result<Vec<u8>, Error> {
        let w = SerializeBatchWrapper { b };
        let encoded = serde_json::to_string(&w)?;
        Ok(encoded.as_bytes().to_vec())
    }

    fn deserialize_batch(&self, s: Vec<u8>) -> Result<DDValueBatch, Error> {
        let s = std::str::from_utf8(&s)?;
        let v: SerializeBatchWrapper = serde_json::from_str(&s)?;
        Ok(v.b)
    }
}

pub fn start_d3log() -> Result<(), Error> {
    let management_management = Arc::new(Print(Arc::new(Null {})));
    let (uuid, is_parent) = if let Some(uuid) = std::env::var_os("uuid") {
        if let Some(uuid) = uuid.to_str() {
            let uuid = uuid.parse::<u128>().unwrap();
            (uuid, false)
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

    let (d, init_batch) = D3::new(uuid)?;
    let m = if is_parent {
        Arc::new(Broadcast::new()) as Port
    } else {
        let m = FileDescriptorPort {
            management: management_management.clone(),
            eval: d.clone(),
            fd: MANAGEMENT_OUTPUT_FD,
        };
        Arc::new(m) as Port
    };

    let rt = Arc::new(Runtime::new()?);
    let (port, instance_future) = start_instance(rt.clone(), d.clone(), uuid, m.clone())?;

    // XXX: we really kind of want the initial evaluation to happen at one ingress node
    // find the ddlog ticket against and reference
    if is_parent {
        rt.spawn(async move {
            port.send(init_batch);
        });
    }
    rt.block_on(instance_future)?;
    Ok(())
}
