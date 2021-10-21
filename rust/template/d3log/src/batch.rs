// Batch.rs
// Support for moving facts (tuples that belong to relations) between different processes.

use crate::{Error, Evaluator, RecordSet, ValueSet};
use core::fmt;
use core::fmt::Display;
use differential_datalog::{
    ddval::Any,
    ddval::DDValue,
    program::{RelId, Weight},
};
use serde::{
    de::{DeserializeSeed, Error as DeError, MapAccess, SeqAccess, Visitor},
    ser::{Error as SeError, SerializeMap},
    Deserializer, Serialize, Serializer,
};
use std::collections::HashMap;
use std::string::String;

/// `Properties` describes metadata as key-value pairs.
/// Is is used for debugging.
#[derive(Clone)]
pub struct Properties {
    properties: HashMap<String, String>,
}

impl Properties {
    pub fn new() -> Properties {
        Properties {
            properties: HashMap::new(),
        }
    }
}

/// A batch contains a set of facts (in the BatchBody) together with some
/// metadata  describing the facts.
#[derive(Clone)]
pub struct Batch {
    pub metadata: Properties,
    pub body: BatchBody,
}

/// A set of facts --- rows from tables.
// There are two possible representations of facts
// that represent the same information in different ways.
#[derive(Clone)]
pub enum BatchBody {
    /// A ValueSet is essentially a vector of ddval::DDvalue objects;
    /// these are Rust objects that represent rows in a relation.  These
    /// are quite efficient, but carry no run-time type information.
    Value(ValueSet),
    /// In contrast, a RecordSet is a vector of (Record,Weight) objects.
    /// Each Record carries run-time type information and the relation name
    /// that is belongs to, allowing interchange of records between
    /// DDlog programs that are not necessarily compiled from the same source.
    Record(RecordSet),
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Batch [").unwrap();
        match &self.body {
            BatchBody::Value(v) => Display::fmt(&v, f),
            BatchBody::Record(r) => Display::fmt(&r, f),
            // TODO: it would be lovely if this were columnated - could use tabular::{Table, Row}
        }?;
        writeln!(f, "\n]\n")
    }
}

struct ExtendValueWeight {
    eval: Evaluator,
    relid: usize,
}

impl<'de, 'a> DeserializeSeed<'de> for ExtendValueWeight {
    type Value = (Any, Weight);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ExtendSeqVisitor {
            eval: Evaluator,
            relid: RelId,
        }
        impl<'de, 'a> Visitor<'de> for ExtendSeqVisitor {
            type Value = (Any, Weight);

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a (value, weight) pair")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let d = self
                    .eval
                    .clone()
                    .relation_deserializer(self.relid)
                    .map_err(A::Error::custom)?;

                // xxx - need to translate this error
                if let Some(v) = seq.next_element_seed(d)? {
                    if let Some(w) = seq.next_element()? {
                        return Ok((v, w));
                    }
                    return Err(DeError::missing_field("integer weight"));
                }
                Err(DeError::missing_field("DDvalue formatted json"))
            }
        }
        deserializer.deserialize_seq(ExtendSeqVisitor {
            eval: self.eval.clone(),
            relid: self.relid,
        })
    }
}

struct ExtendValues {
    eval: Evaluator,
    relid: RelId,
}

impl<'de, 'a> DeserializeSeed<'de> for ExtendValues {
    type Value = Vec<(Any, Weight)>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ExtendValuesVisitor {
            eval: Evaluator,
            relid: RelId,
        }
        impl<'de, 'a> Visitor<'de> for ExtendValuesVisitor {
            type Value = Vec<(Any, Weight)>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a map of relations")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut result = Vec::new();
                while let Some(x) = seq.next_element_seed(ExtendValueWeight {
                    eval: self.eval.clone(),
                    relid: self.relid,
                })? {
                    result.push(x)
                }
                return Ok(result);
            }
        }
        deserializer.deserialize_seq(ExtendValuesVisitor {
            eval: self.eval.clone(),
            relid: self.relid,
        })
    }
}

struct ExtendRelations {
    eval: Evaluator,
}

// copied from https://docs.serde.rs/serde/de/trait.DeserializeSeed.html
impl<'de, 'a> DeserializeSeed<'de> for ExtendRelations {
    type Value = ValueSet;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ExtendMapVisitor {
            eval: Evaluator,
        }
        impl<'de, 'a> Visitor<'de> for ExtendMapVisitor {
            type Value = ValueSet;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a map of relations")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut out = ValueSet::new(self.eval.clone());
                while let Some(k) = map.next_key()? {
                    let z: String = k;

                    let relid = self
                        .eval
                        .id_from_relation_name(z.clone())
                        .map_err(A::Error::custom)?;

                    let vwvec = map.next_value_seed(ExtendValues {
                        eval: self.eval.clone(),
                        relid: relid,
                    })?;
                    for (v, w) in vwvec {
                        out.insert(relid, DDValue::from(v), w);
                    }
                }
                Ok(out)
            }
        }
        deserializer.deserialize_map(ExtendMapVisitor { eval: self.eval })
    }
}

impl Serialize for Batch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut rels = HashMap::new();

        match self.body.clone() {
            BatchBody::Value(d) => {
                for (r, v, w) in &d {
                    let relation_name = match d.eval.relation_name_from_id(r) {
                        Ok(r) => r,
                        Err(_e) => {
                            return Err(SeError::custom(
                                format!("invalid relation {}", r).to_string(),
                            ))
                        }
                    };
                    let f = rels.entry(relation_name).or_insert_with(|| Vec::new());
                    f.push((v, w))
                }
            }
            BatchBody::Record(_) => {
                panic!("unsupported serialization");
            }
        }

        let mut map = serializer.serialize_map(Some(rels.len()))?;
        for (r, vw) in rels {
            map.serialize_entry(&r, &vw)?;
        }
        map.end()
    }
}

impl Batch {
    pub fn deserialize(body: Vec<u8>, eval: Evaluator) -> Result<Batch, Error> {
        let s = std::str::from_utf8(&body)?;
        let seed = ExtendRelations { eval: eval.clone() };
        let mut de = serde_json::Deserializer::from_str(s);
        let b: ValueSet = seed.deserialize(&mut de)?;
        Ok(Batch {
            metadata: Properties::new(),
            body: BatchBody::Value(b),
        })
    }

    pub fn serialize(self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_string(&self)?.as_bytes().to_vec())
    }
}
