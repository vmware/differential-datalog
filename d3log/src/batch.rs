use differential_datalog::{ddval::DDValue, program::RelId, program::Update, DeltaMap};

// not really, we aren't going to be compiling against the user program
use mm_ddlog::*;

use serde::{
    de, de::SeqAccess, de::Visitor, ser::SerializeTuple, Deserialize, Deserializer, Serialize,
    Serializer,
};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Display;
use std::io::{Error, ErrorKind};

// the other choice here would be a Vec<Update>?
// #[derive(Serialize, Deserialize)] - deltamap isn't serialize
pub struct Batch {
    // one might consider augmenting the values w/ t - kind of a
    // performance problem for multisets
    timestamp: u64,
    pub b: DeltaMap<differential_datalog::ddval::DDValue>,
    // timestamp
}

impl Serialize for Batch {
    // i would _like_ to expose an interface that used the names for relations
    // so that external users dont have to be privy to the compiler id assignemnt

    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tup = serializer.serialize_tuple(3)?; //maybe map at the top level is better
        let mut updates = Vec::new();
        // use the batch iterator
        for (relid, vees) in self.b.clone() {
            for (v, _) in vees {
                updates.push(UpdateSerializer::from(Update::Insert {
                    relid,
                    v: v.clone(),
                }));
            }
        }
        tup.serialize_element(&self.timestamp)?;
        tup.serialize_element(&updates)?;
        tup.end()
    }
}

struct BatchVisitor {}

impl<'de> Visitor<'de> for BatchVisitor {
    type Value = Batch;

    // his just formats an error message..in advance?
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "batch")
    }

    fn visit_seq<E>(self, mut e: E) -> Result<Self::Value, E::Error>
    where
        E: SeqAccess<'de>,
    {
        let mut b = Batch::new(DeltaMap::new());

        let t: Option<u64> = e.next_element()?;
        match t {
            Some(t) => b.timestamp = t,
            None => return Err(de::Error::custom("expected integer timestamp")),
        }

        let k: Option<Vec<UpdateSerializer>> = e.next_element()?;
        match k {
            Some(x) => {
                for i in x {
                    let u = Update::<DDValue>::from(i);
                    match u {
                        Update::Insert { relid, v } => b.b.update(relid, &v, 1),
                        _ => return Err(de::Error::custom("invalid value")),
                    }
                }
            }
            None => return Err(de::Error::custom("unable to parse update set")),
        }

        Ok(b)
    }
}

impl<'de> Deserialize<'de> for Batch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let b: Batch = deserializer.deserialize_any(BatchVisitor {})?;
        Ok(b)
    }
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format!("<{}", self.timestamp))?;
        for (relid, vees) in self.b.clone() {
            f.write_str(&format!("({}", relid2name(relid).expect("relation")))?; // name
            let mut m = 0;
            for (_v, _) in vees {
                m += 1;
            }
            f.write_str(&format!(" {})", m))?;
        }
        f.write_str(&format!(">"))?;
        Ok(())
    }
}

// this is not send because box<!Sized> is not send
pub struct BatchIterator {
    relid: RelId,
    relations: Box<dyn Iterator<Item = (RelId, BTreeMap<DDValue, isize>)> + Send>,
    items: Option<Box<dyn Iterator<Item = (DDValue, isize)> + Send>>,
}

impl Iterator for BatchIterator {
    type Item = (RelId, DDValue, isize);

    fn next(&mut self) -> Option<(RelId, DDValue, isize)> {
        match &mut self.items {
            Some(x) => match x.next() {
                Some((v, w)) => Some((self.relid, v, w)),
                None => {
                    self.items = None;
                    self.next()
                }
            },
            None => {
                // what about the empty batch?
                let (relid, items) = self.relations.next()?;
                self.relid = relid;
                self.items = Some(Box::new(items.into_iter()));
                self.next()
            }
        }
    }
}

impl IntoIterator for Batch {
    type Item = (RelId, DDValue, isize);
    type IntoIter = BatchIterator;

    fn into_iter(self) -> BatchIterator {
        BatchIterator {
            relid: 0,
            relations: Box::new(self.b.into_iter()),
            items: None,
        }
    }
}

impl Batch {
    // there should be a new that allocates it own and a from deltamap for
    // the wrap case

    pub fn new(b: DeltaMap<differential_datalog::ddval::DDValue>) -> Batch {
        Batch { b, timestamp: 0 }
    }

    pub fn insert(&mut self, r: RelId, v: differential_datalog::ddval::DDValue, weight: u32) {
        self.b.update(r, &v, weight as isize);
    }
}

// make associated
pub fn singleton(
    rel: &str,
    v: &differential_datalog::ddval::DDValue,
) -> Result<Batch, std::io::Error> {
    let mrel = match Relations::try_from(rel) {
        Ok(x) => x as usize,
        Err(_x) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("bad relation {}", rel),
            ))
        }
    };

    let mut d = Batch::new(DeltaMap::<differential_datalog::ddval::DDValue>::new());
    d.insert(mrel, v.clone(), 1);
    Ok(d)
}
