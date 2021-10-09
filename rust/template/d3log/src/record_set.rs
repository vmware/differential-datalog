//! record_set.rs: represent a set of rows in a multiset relation,
//! with their associated weights.  A RecordSet can contains rows from
//! many different relations.

use crate::{Batch, BatchBody, Error};
use differential_datalog::program::Weight;
use differential_datalog::record::Record;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

/// A RecordSet is a vector of Record object with associated Weights.
/// Each Record object describes a row from some relation.
#[derive(Clone, Default)]
pub struct RecordSet {
    pub records: Vec<(Record, Weight)>,
}

/// This macro creates a Batch object that contains a single record with weight 1.
/// The arguments are: relation name, and key => value pairs for the 'columns' of
/// the relation.  Here is an example using this macro to create a row in the
/// relation Person(name: string, age: u32).
/// ```
/// let f = fact!("Person",
///               name => "Bob",
///               age => 23);
/// ```
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
    /// Create an empty RecordSet
    pub fn new() -> RecordSet {
        RecordSet {
            records: Vec::new(),
        }
    }

    /// Create a RecordSet that contains a single Record, with weight 1.
    pub fn singleton(rec: Record) -> RecordSet {
        RecordSet {
            records: vec![(rec, 1)],
        }
    }

    /// Add a new record with the specified weight to this RecordSet.
    pub fn insert(&mut self, v: Record, weight: Weight) {
        self.records.push((v, weight))
    }

    /// Extract a RecordSet from a Batch.
    pub fn from(batch: Batch) -> Result<RecordSet, Error> {
        match batch.body {
            // If the Batch contains a ValueSet, we have to convert each Value to a corresponding Record
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

/// This trait allows RecordSet objects to be iterated on
impl<'a> IntoIterator for &'a RecordSet {
    type Item = (String, Record, Weight);
    type IntoIter = RecordSetIterator<'a>;

    fn into_iter(self) -> RecordSetIterator<'a> {
        RecordSetIterator {
            items: Box::new(self.records.clone().into_iter()),
        }
    }
}
