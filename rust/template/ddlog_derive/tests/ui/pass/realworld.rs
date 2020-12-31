use ddlog_derive::{FromRecord, IntoRecord, Mutator};
use serde::{Deserialize, Serialize};

fn main() {}

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
#[ddlog(rename = "ast::ExprKind")]
enum ExprKind {
    #[ddlog(rename = "ast::ExprLit")]
    ExprLit { kind: LitKind },
    #[ddlog(rename = "ast::ExprNameRef")]
    ExprNameRef,
    #[ddlog(rename = "ast::ExprYield")]
    ExprYield,
    #[ddlog(rename = "ast::ExprAwait")]
    ExprAwait,
    #[ddlog(rename = "ast::ExprArrow")]
    ExprArrow,
    #[ddlog(rename = "ast::ExprUnaryOp")]
    ExprUnaryOp,
    #[ddlog(rename = "ast::ExprBinOp")]
    ExprBinOp,
    #[ddlog(rename = "ast::ExprTernary")]
    ExprTernary,
    #[ddlog(rename = "ast::ExprThis")]
    ExprThis,
    #[ddlog(rename = "ast::ExprTemplate")]
    ExprTemplate,
    #[ddlog(rename = "ast::ExprArray")]
    ExprArray,
    #[ddlog(rename = "ast::ExprObject")]
    ExprObject,
    #[ddlog(rename = "ast::ExprGrouping")]
    ExprGrouping { inner: ddlog_std::Option<ExprId> },
    #[ddlog(rename = "ast::ExprBracket")]
    ExprBracket,
    #[ddlog(rename = "ast::ExprDot")]
    ExprDot,
    #[ddlog(rename = "ast::ExprNew")]
    ExprNew,
    #[ddlog(rename = "ast::ExprCall")]
    ExprCall,
    #[ddlog(rename = "ast::ExprAssign")]
    ExprAssign,
    #[ddlog(rename = "ast::ExprSequence")]
    ExprSequence { exprs: ddlog_std::Vec<ExprId> },
    #[ddlog(rename = "ast::ExprNewTarget")]
    ExprNewTarget,
    #[ddlog(rename = "ast::ExprImportMeta")]
    ExprImportMeta,
    #[ddlog(rename = "ast::ExprInlineFunc")]
    ExprInlineFunc,
    #[ddlog(rename = "ast::ExprSuperCall")]
    ExprSuperCall {
        args: ddlog_std::Option<ddlog_std::Vec<ExprId>>,
    },
    #[ddlog(rename = "ast::ExprImportCall")]
    ExprImportCall { arg: ddlog_std::Option<ExprId> },
    #[ddlog(rename = "ast::ExprClass")]
    ExprClass,
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
)]
enum LitKind {
    Integer,
}

impl Default for LitKind {
    fn default() -> Self {
        Self::Integer
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
struct ExprId;

mod ddlog_std {
    use super::*;

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
    pub enum Option<T> {
        Some(T),
        None,
    }

    impl<T> Default for Option<T> {
        fn default() -> Self {
            Self::None
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
    pub struct Vec<T>(T);
}
