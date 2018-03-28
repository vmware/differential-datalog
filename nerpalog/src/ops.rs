pub enum BOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
    Impl,
    Plus,
    Minus,
    Mod,
    ShiftR,
    ShiftL,
    BAnd,
    BOr,
    Concat
}

//bopReturnsBool :: BOp -> Bool
//bopReturnsBool op = elem op [Eq, Neq, Lt, Gt, Lte, Gte, And, Or, Impl]

pub enum UOp {
    Not,
    BNeg
}
