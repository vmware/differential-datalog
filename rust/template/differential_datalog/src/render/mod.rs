use crate::{ddval::DDValue, program::config::Config};
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use std::borrow::Cow;
use timely::dataflow::ScopeParent;

pub mod arrange_by;

pub type Str<'a> = Cow<'a, str>;

pub type Offset = u32;

pub type TraceValue<S, R, O = Offset> =
    OrdValSpine<DDValue, DDValue, <S as ScopeParent>::Timestamp, R, O>;

pub type TraceKey<S, R, O = Offset> = OrdKeySpine<DDValue, <S as ScopeParent>::Timestamp, R, O>;

#[derive(Debug)]
pub struct RenderContext {
    pub config: Config,
}

impl RenderContext {
    pub const fn new(config: Config) -> Self {
        Self { config }
    }
}
