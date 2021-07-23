use ddlog_derive::{FromRecord, IntoRecord, Mutator};
use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};
use std::sync::Arc;

#[derive(
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    IntoRecord,
    Mutator,
    Serialize,
    Deserialize,
    FromRecord,
)]
#[ddlog(rename = "List")]
pub enum List<N> {
    #[ddlog(rename = "List")]
    List { node: N, nxt: ListNxt<N> },
    #[ddlog(rename = "EMPTY")]
    EMPTY,
}

impl<T> Default for List<T> {
    fn default() -> Self {
        Self::EMPTY
    }
}

#[derive(
    Eq,
    Ord,
    Clone,
    Hash,
    PartialEq,
    PartialOrd,
    IntoRecord,
    Mutator,
    Serialize,
    Deserialize,
    FromRecord,
    Default,
)]
#[ddlog(rename = "ListNxt")]
pub struct ListNxt<N> {
    nxt: Ref<List<N>>,
}

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, IntoRecord, Mutator, FromRecord, Default)]
pub struct Ref<T> {
    inner: Arc<T>,
}

impl<'a, T> Deserialize<'a> for Ref<T> {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        unimplemented!()
    }
}

impl<T> Serialize for Ref<T> {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        unimplemented!()
    }
}

fn main() {}
