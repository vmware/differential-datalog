// functions to allow a set of Records, a dynamically typed alternative to DDValue, to act as
// Batch for interchange between different ddlog programs

use crate::{Batch, BatchBody, Error};
use differential_datalog::program::Weight;
use differential_datalog::record::Record;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

#[derive(Clone, Default)]
pub struct RecordSet {
    pub records: Vec<(Record, Weight)>,
}

#[macro_export]
macro_rules! fact {
    ( $rel:path,  $($n:ident => $v:expr),* ) => {
        Batch{body: BatchBody::Record(RecordSet::singleton(
            Record::NamedStruct(
                std::borrow::Cow::from(stringify!($rel).to_string()),
                vec![$((std::borrow::Cow::from(stringify!($n)), $v),)*]))),
              metadata: batch::Properties::new(),
        }
    }
}

impl Display for RecordSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut m = HashMap::new();
        for (r, _w) in self.records.clone() {
            match r {
                Record::PosStruct(name, _record_vec) => println!("{}", name),
                Record::NamedStruct(r, _attributes) => *m.entry(r).or_insert(0) += 1,
                _ => panic!("unhandled DD Record type in Display"),
            }
        }

        f.write_str(&"<")?;
        for (r, c) in m {
            f.write_str(&format!("({} {})", r, c))?;
        }
        f.write_str(&">")?;
        Ok(())
    }
}

impl RecordSet {
    pub fn new() -> RecordSet {
        RecordSet {
            records: Vec::new(),
        }
    }

    pub fn singleton(rec: Record) -> RecordSet {
        RecordSet {
            records: vec![(rec, 1)],
        }
    }

    pub fn insert(&mut self, v: Record, weight: Weight) {
        self.records.push((v, weight))
    }

    pub fn from(batch: Batch) -> Result<RecordSet, Error> {
        match batch.body {
            BatchBody::Value(d) => {
                let mut rb = RecordSet::new();
                for (_relid, val, weight) in &d {
                    let record: Record = d.eval.clone().record_from_ddvalue(val).unwrap();
                    let val = match record {
                        // [ weight, actual_record ]
                        Record::Tuple(t) => t[1].clone(),
                        Record::NamedStruct(name, rec) => Record::NamedStruct(name, rec),
                        _ => panic!("unknown type!"),
                    };
                    rb.insert(val, weight);
                }
                Ok(rb)
            }
            BatchBody::Record(x) => Ok(x),
        }
    }
}

pub struct RecordSetIterator<'a> {
    items: Box<dyn Iterator<Item = (Record, Weight)> + Send + 'a>,
}

impl<'a> Iterator for RecordSetIterator<'a> {
    type Item = (String, Record, Weight);

    fn next(&mut self) -> Option<(String, Record, Weight)> {
        match self.items.next() {
            Some((Record::NamedStruct(name, val), w)) => {
                Some(((*name).to_string(), Record::NamedStruct(name, val), w))
            }
            _ => None,
        }
    }
}

impl<'a> IntoIterator for &'a RecordSet {
    type Item = (String, Record, Weight);
    type IntoIter = RecordSetIterator<'a>;

    fn into_iter(self) -> RecordSetIterator<'a> {
        RecordSetIterator {
            items: Box::new(self.records.clone().into_iter()),
        }
    }
}
