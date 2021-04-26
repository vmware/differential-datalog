use abomonation::Abomonation;
use abomonation_derive::Abomonation;
use datalog_example_ddlog::ddlog_bigint;
use ddlog_derive::{FromRecord, IntoRecord, Mutator};
use differential_datalog::record::{IntoRecord, Mutator, Record};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display, Formatter};

/// `Value` type that implements `trait DDValConvert` and is thus useful for testing Rust modules that
/// interact with the DDlog API, but do not define their own value type.

macro_rules! debug_display {
    ($($ty:ident),* $(,)?) => {
        $(
            impl Display for $ty {
                fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
                    Debug::fmt(self, fmt)
                }
            }
        )*
    };
}

macro_rules! ddlog_struct {
    ($($type:ident = $inner:ty),* $(,)?) => {
        $(
            #[derive(
                Default,
                Eq,
                Ord,
                Clone,
                Hash,
                PartialEq,
                PartialOrd,
                Serialize,
                Deserialize,
                Debug,
                Abomonation,
                IntoRecord,
                FromRecord,
                Mutator,
            )]
            pub struct $type(pub $inner);

            impl From<$inner> for $type {
                fn from(inner: $inner) -> Self {
                    Self(inner)
                }
            }

            impl Display for $type {
                fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
                    Debug::fmt(self, fmt)
                }
            }
        )*
    };
}

ddlog_struct! {
    Bool = bool,
    Uint = ddlog_bigint::Uint,
    String = std::string::String,
    U8 = u8,
    U16 = u16,
    U32 = u32,
    U64 = u64,
    I64 = i64,
    BoolTuple = (bool, bool),
}

#[derive(
    Default,
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    Debug,
    Abomonation,
    IntoRecord,
    FromRecord,
    Mutator,
)]
pub struct Empty {}

debug_display!(Empty);

#[derive(
    Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug, Abomonation,
)]
pub struct Tuple2<T>(pub Box<T>, pub Box<T>);

impl<T> Tuple2<T> {
    pub fn new(left: T, right: T) -> Self {
        Self(Box::new(left), Box::new(right))
    }
}

impl<T: Debug> Display for Tuple2<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl<T> IntoRecord for Tuple2<T> {
    fn into_record(self) -> Record {
        unimplemented!("Tuple2::IntoRecord");
    }
}

impl<T> Mutator<Tuple2<T>> for Record {
    fn mutate(&self, _v: &mut Tuple2<T>) -> Result<(), std::string::String> {
        unimplemented!("Tuple2::Mutator");
    }
}

#[derive(
    Default,
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    Debug,
    Abomonation,
    FromRecord,
    IntoRecord,
    Mutator,
)]
pub struct Q {
    pub f1: bool,
    pub f2: String,
}

debug_display!(Q);

#[derive(
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    Debug,
    Abomonation,
    FromRecord,
    IntoRecord,
    Mutator,
)]
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

impl S {
    pub fn f1(&mut self) -> &mut u32 {
        match self {
            S::S1 { ref mut f1, .. } => f1,
            _ => panic!(""),
        }
    }
}

impl Default for S {
    fn default() -> S {
        S::S1 {
            f1: u32::default(),
            f2: String::default(),
            f3: Q::default(),
            f4: Uint::default(),
        }
    }
}

debug_display!(S);

#[derive(
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    Debug,
    Abomonation,
    FromRecord,
    IntoRecord,
    Mutator,
)]
pub struct P {
    pub f1: Q,
    pub f2: bool,
}

debug_display!(P);
