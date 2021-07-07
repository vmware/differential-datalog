// use crate::ddvalue_batch::DDValueBatch;
use crate::{error::Error, Batch, Evaluator};
use differential_datalog::record::{CollectionKind, Record};
use num::bigint::ToBigInt;
use num::ToPrimitive;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::string::String;

use serde::{
    de, de::SeqAccess, de::Visitor, ser::SerializeTuple, Deserialize, Deserializer, Serialize,
    Serializer,
};

use serde_json::{Value, Value::*};

#[derive(Clone)]
pub struct RecordBatch {
    pub timestamp: u64,
    pub records: Vec<(Record, isize)>,
}

// xx - factor out the Cow so we dont need to use it everywhere. guess that means a copy
// maybe $crate can help?
#[macro_export]
macro_rules! fact {
    ( $rel:path,  $($n:ident => $v:expr),* ) => {
        Batch::Rec(RecordBatch::singleton(
            Record::NamedStruct(
                Cow::from(stringify!($rel).to_string()),
                vec![$((Cow::from(stringify!($n)), $v),)*])))
    }
}

impl Display for RecordBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut m = HashMap::new();
        for (r, _w) in self.records.clone() {
            match r {
                Record::Bool(_b) => println!("bad record type bool"),
                Record::Int(_i) => println!("bad record type int"),
                Record::Float(_f) => println!("bad record type float"),
                Record::Double(_dbl) => println!("bad record type double"),
                Record::String(string_name) => println!("{}", string_name),
                Record::Serialized(name, s) => println!("serialized {}", name),
                Record::Tuple(t) => {
                    println!("tuple {:?}", t);
                }
                Record::Array(_, _record_vec) => println!("bad record type array"),
                Record::PosStruct(name, record_vec) => println!("{}", name),

                Record::NamedStruct(r, _attributes) => *m.entry(r).or_insert(0) += 1,
            }
        }

        f.write_str(&"<")?;
        // group by!
        for (r, c) in m {
            f.write_str(&format!("({} {})", r, c))?;
        }
        f.write_str(&">")?;
        Ok(())
    }
}

fn value_to_record(v: Value) -> Result<Record, Error> {
    match v {
        Null => panic!("we dont null here"),
        Value::Bool(b) => Ok(Record::Bool(b)),
        // going to have to deal with floats and i guess maybe bignums ?
        // serde wants a u64 or a float here...fix
        Value::Number(n) => Ok(Record::Int(n.as_u64().unwrap().to_bigint().unwrap())),
        Value::String(s) => Ok(Record::String(s)),
        Value::Array(a) => {
            let mut values = Vec::new();
            for v in a {
                values.push(value_to_record(v)?);
            }
            // ok, well this is a problem..oh wait, this is {vector, set,map}, not the
            // domain over which it collects. idk why that isn't just the tag
            Ok(Record::Array(CollectionKind::Vector, values))
        }
        Value::Object(m) => {
            let mut properties = Vec::new();
            for (k, v) in m {
                properties.push((Cow::from(k), value_to_record(v)?));
            }
            Ok(Record::NamedStruct(Cow::from(""), properties))
        }
    }
}

fn record_to_value(r: Record) -> Result<Value, Error> {
    match r {
        Record::Bool(b) => Ok(Value::Bool(b)),
        Record::Int(n) => {
            let num = n
                .to_bigint()
                .ok_or_else(|| Error::new("json bigint conversion".to_string()))?;
            let fixed = num
                .to_i64()
                .ok_or_else(|| Error::new("json bigint conversion".to_string()))?;
            Ok(serde_json::Value::Number(serde_json::Number::from(fixed)))
        }

        Record::String(s) => Ok(Value::String(s)),
        Record::Array(_i, _v) => panic!("foo"),
        Record::NamedStruct(_collection_kind, _v) => panic!("bbar"),
        _ => Err(Error::new("unhanded record format".to_string())),
    }
}

struct RecordBatchVisitor {}

impl<'de> Visitor<'de> for RecordBatchVisitor {
    type Value = RecordBatch;

    // this just formats an error message..in advance?
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "batch")
    }

    fn visit_seq<E>(self, mut e: E) -> Result<Self::Value, E::Error>
    where
        E: SeqAccess<'de>,
    {
        {
            let mut bn = RecordBatch::new();
            let timestamp: Option<u64> = e.next_element()?;
            match timestamp {
                Some(timestamp) => bn.timestamp = timestamp,
                None => return Err(de::Error::custom("expected integer timestamp")),
            }

            // can we do this directly into Record? doesn't seem like we can address
            // NamedStruct directly here. Can parse into Vec<Record>, but .. i think
            // the relation name is in the wrong spot?
            let records: Option<HashMap<String, Vec<HashMap<String, Value>>>> = e.next_element()?;
            match records {
                Some(r) => {
                    let mut records = Vec::new();
                    for (r, valueset) in r.into_iter() {
                        for fact in valueset {
                            let mut properties = Vec::new();
                            for (k, v) in fact {
                                properties.push((
                                    Cow::from(k),
                                    value_to_record(v).expect("value translation"),
                                ));
                            }
                            // xxx weight
                            records
                                .push((Record::NamedStruct(Cow::from(r.clone()), properties), 1));
                        }
                    }
                    bn.records = records;
                }
                // can't figure out how to return an error here.
                None => panic!("bad record batch syntax".to_string()),
            }
            Ok(bn)
        }
    }
}

impl<'de> Deserialize<'de> for RecordBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let b: RecordBatch = deserializer.deserialize_any(RecordBatchVisitor {})?;
        Ok(b)
    }
}

impl Serialize for RecordBatch {
    // i would _like_ to expose an interface that used the names for relations
    // so that external users dont have to be privy to the compiler id assignment
    // schema across the top for each relation

    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut m = HashMap::<String, Vec<HashMap<String, Record>>>::new();
        let mut tup = serializer.serialize_tuple(2)?;
        tup.serialize_element(&self.timestamp)?;
        // need to encode w !
        for (v, _w) in &self.records {
            match v {
                Record::NamedStruct(relname, v) => {
                    m.entry(relname.to_string()).or_insert(Vec::new()).push({
                        // wanted to use map...but some trait bound something something
                        let mut out = HashMap::<String, Record>::new();
                        for (k, v) in v {
                            out.insert(k.to_string(), v.clone());
                        }
                        out
                    })
                }
                _ => panic!("weird stuff in record batch"),
            }
        }
        tup.serialize_element(&m)?;
        tup.end()
    }
}

impl RecordBatch {
    pub fn new() -> RecordBatch {
        RecordBatch {
            timestamp: 0,
            records: Vec::new(),
        }
    }

    pub fn singleton(rec: Record) -> RecordBatch {
        RecordBatch {
            timestamp: 0,
            records: vec![(rec, 1)],
        }
    }

    // Record::NamedStruct((_r, _)) = v
    pub fn insert(&mut self, _r: String, v: Record, weight: isize) {
        self.records.push((v, weight))
    }

    // tried to use impl From<Batch> for RecordBatch, but no error path, other type issues
    // why no err?
    pub fn from(e: Evaluator, b: Batch) -> RecordBatch {
        match b {
            Batch::Value(x) => {
                let mut rb = RecordBatch::new();
                for (r, v, w) in &x {
                    let r = e.clone().relation_name_from_id(r).unwrap();
                    let _record: Record = e.clone().record_from_ddvalue(v).unwrap();
                    let v = match _record {
                        // [ weight, actual_record ]
                        Record::Tuple(t) => t[1].clone(),
                        Record::NamedStruct(name, rec) => Record::NamedStruct(name, rec),
                        _ => panic!("unknown type!"),
                    };
                    rb.insert(r, v, w);
                }
                rb
            }
            Batch::Rec(x) => x,
        }
    }
}

pub struct RecordBatchIterator<'a> {
    items: Box<dyn Iterator<Item = (Record, isize)> + Send + 'a>,
}

impl<'a> Iterator for RecordBatchIterator<'a> {
    type Item = (String, Record, isize);

    fn next(&mut self) -> Option<(String, Record, isize)> {
        match self.items.next() {
            Some((Record::NamedStruct(name, val), w)) => {
                Some(((*name).to_string(), Record::NamedStruct(name, val), w))
            }
            _ => None,
        }
    }
}

impl<'a> IntoIterator for &'a RecordBatch {
    type Item = (String, Record, isize);
    type IntoIter = RecordBatchIterator<'a>;

    fn into_iter(self: Self) -> RecordBatchIterator<'a> {
        RecordBatchIterator {
            items: Box::new(self.records.clone().into_iter()),
        }
    }
}

// idk why i dont want to make these associated...i guess holding on to the idea
// that the external representation doesn't need to be tied to the internal. so
// quaint
pub fn record_serialize_batch(r: RecordBatch) -> Result<Vec<u8>, Error> {
    let encoded = serde_json::to_string(&r)?;
    Ok(encoded.as_bytes().to_vec())
}

pub fn deserialize_record_batch(v: Vec<u8>) -> Result<Batch, Error> {
    let s = std::str::from_utf8(&v)?;
    let v: RecordBatch = serde_json::from_str(&s)?;
    Ok(Batch::Rec(v))
}
