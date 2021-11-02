//! Represents a vector of ddval::Values: a value can be used to
//! represent a row in a multiset relation (with the associated weight).
//! A ValueSet can contain values from multiple different relations.

use crate::{Batch, BatchBody, Error, Evaluator, Properties};
use differential_datalog::{
    ddval::DDValue,
    program::{RelId, Weight},
    DeltaMap,
};
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Display;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct ValueSet {
    // The evaluator is a handle to the DDlog runtime that "understands" the values.
    pub eval: Evaluator, // used when translating a Value to a Record object
    /// A DeltaMap is a DDlog structure representing a set of changes.
    pub deltas: Arc<Mutex<DeltaMap<differential_datalog::ddval::DDValue>>>,
}

impl Display for ValueSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<")?;
        let iter = self.clone();
        let mut m: usize = 0;
        // TODO: this code is incomplete, it does not display the ValueSet.
        for (relid, _v, _w) in iter.into_iter() {
            // TODO: I would really prefer a readable name..but we need an Evaluator and it doesn't seem right
            // to add it to all the batches or globalize it
            f.write_str(&format!("({}", relid))?;
            m += 1;
            f.write_str(&format!(" {})", m))?;
        }
        f.write_str(">")?;
        Ok(())
    }
}

/// An iterator that returns all values in a ValueSet.
pub struct ValueSetIterator<'a> {
    relid: RelId,
    // sadly, BTreeMap is defined over isize and not differential_datalog::program::Weight
    relations: Box<dyn Iterator<Item = (RelId, BTreeMap<DDValue, isize>)> + Send + 'a>,
    items: Option<Box<dyn Iterator<Item = (DDValue, isize)> + Send>>,
}

impl<'a> Iterator for ValueSetIterator<'a> {
    type Item = (RelId, DDValue, Weight);
    fn next(&mut self) -> Option<(RelId, DDValue, Weight)> {
        match &mut self.items {
            Some(x) => match x.next() {
                Some((v, w)) => Some((self.relid, v, w as Weight)),
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

impl<'a> IntoIterator for &'a ValueSet {
    type Item = (RelId, DDValue, Weight);
    type IntoIter = ValueSetIterator<'a>;

    fn into_iter(self) -> ValueSetIterator<'a> {
        ValueSetIterator {
            relid: 0,
            relations: Box::new(self.clone().deltas.lock().expect("").clone().into_iter()),
            items: None,
        }
    }
}

impl ValueSet {
    /// Create a ValueSet from a DeltaMap.
    pub fn from_delta_map(
        eval: Evaluator,
        deltas: DeltaMap<differential_datalog::ddval::DDValue>,
    ) -> Batch {
        let n = ValueSet {
            eval: eval.clone(),
            deltas: Arc::new(Mutex::new(deltas)),
        };
        Batch {
            metadata: Properties::new(),
            body: BatchBody::Value(n),
        }
    }

    /// Create an empty ValueSet
    pub fn new(eval: Evaluator) -> ValueSet {
        ValueSet {
            eval,
            deltas: Arc::new(Mutex::new(
                DeltaMap::<differential_datalog::ddval::DDValue>::new(),
            )),
        }
    }

    /// Add some elements to a ValueSet.
    pub fn insert(&mut self, r: RelId, v: differential_datalog::ddval::DDValue, weight: Weight) {
        self.deltas
            .lock()
            .expect("lock")
            .update(r, &v, weight as isize);
    }

    /// Create a ValueSet from a batch.
    pub fn from(e: Evaluator, b: Batch) -> Result<ValueSet, Error> {
        match b.body {
            BatchBody::Value(x) => Ok(x),
            BatchBody::Record(rb) => {
                let mut ddval_batch = ValueSet::new(e.clone());
                let e2 = e.clone();
                for (r, v, w) in &rb {
                    let rid = e2.id_from_relation_name(r.clone())?;
                    ddval_batch.insert(rid, e.ddvalue_from_record(r, v)?, w);
                }
                Ok(ddval_batch)
            }
        }
    }
}
