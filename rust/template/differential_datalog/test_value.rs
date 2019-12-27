use abomonation::Abomonation;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::program::*;
use crate::record::*;
use crate::uint::*;

/// `Value` type that implements `trait DDVal` and is thus useful for testing Rust modules that
/// interact with the DDlog API, but do not define their own `DDVal` type.
#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub enum Value {
    Empty(),
    Bool(bool),
    Uint(Uint),
    String(String),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I64(i64),
    BoolTuple((bool, bool)),
    Tuple2(Box<Value>, Box<Value>),
    Q(Q),
    S(S),
}

impl Abomonation for Value {}

impl Default for Value {
    fn default() -> Value {
        Value::Bool(false)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl Value {
    pub fn from_val_ref(v: &dyn DDVal) -> &Value {
        v.as_any().downcast_ref::<Value>().unwrap()
    }
    pub fn from_ddval_ref(v: &DDValue) -> &Value {
        Self::from_val_ref(v.val())
    }
    pub fn from_val(v: Arc<dyn DDVal>) -> Arc<Value> {
        v.into_any().downcast::<Value>().unwrap()
    }
    pub fn from_ddval(v: DDValue) -> Arc<Value> {
        Self::from_val(v.into_val())
    }
    pub fn into_ddval(self) -> DDValue {
        DDValue::new(Arc::new(self))
    }
}

#[typetag::serde(name = "test_Value")]
impl DDVal for Value {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + 'static + Send + Sync> {
        self
    }
    fn val_eq(&self, other: &dyn DDVal) -> bool {
        self.eq(Value::from_val_ref(other))
    }
    fn val_partial_cmp(&self, other: &dyn DDVal) -> Option<std::cmp::Ordering> {
        self.partial_cmp(Value::from_val_ref(other))
    }
    fn val_cmp(&self, other: &dyn DDVal) -> std::cmp::Ordering {
        self.cmp(Value::from_val_ref(other))
    }
    fn val_clone(&self) -> Arc<dyn DDVal> {
        Arc::new(self.clone())
    }
    fn val_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state)
    }
    fn val_into_record(self: Arc<Self>) -> Record {
        panic!("Value::val_into_record not implemented")
    }
    fn val_mutate(&mut self, _record: &Record) -> Result<(), String> {
        panic!("Value::val_mutate not implemented")
    }
}

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct P {
    pub f1: Q,
    pub f2: bool,
}

impl Abomonation for P {}

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct Q {
    pub f1: bool,
    pub f2: String,
}

impl Abomonation for Q {}

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub enum S {
    S1 {
        f1: u32,
        f2: String,
        f3: Q,
        f4: Uint,
    },
    S2 {
        e1: bool,
    },
    S3 {
        g1: Q,
        g2: Q,
    },
}

impl Abomonation for S {}

impl S {
    pub fn f1(&mut self) -> &mut u32 {
        match self {
            S::S1 { ref mut f1, .. } => f1,
            _ => panic!(""),
        }
    }
}
