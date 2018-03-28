use pos::Pos;
use ops::{UOp, BOp};
use std::vec::Vec;
use std::collections::HashMap;
use num::bigint::{BigUint, BigInt};

pub struct Field {
    pos:  Pos,
    name: String,
    typ:  Type
}

pub struct Constructor {
    pos:  Pos,
    name: String,
    args: Vec<Field>
}

pub enum Type {
    Bool  {pos: Pos},
    Int   {pos: Pos},
    Str   {pos: Pos},
    Bit   {pos: Pos, width: usize},
    Struct{pos: Pos, cons: Vec<Constructor>},
    Tuple {pos: Pos, fields: Vec<Type>},
    TUser {pos: Pos, name: String}
}

pub struct TypeDef { 
    pos:  Pos,
    name: String,
    typ:  Type
}

pub struct Relation {
    pos:    Pos,
    name:   String,
    ground: bool,
    args:   Vec<Field>
}

pub struct Atom {
    pos:      Pos,
    relation: String,
    args:     Vec<Expr>
}

pub struct Rule {
    pos:          Pos,
    lhs:          Vec<Atom>,
    rhs_literals: Vec<(bool, Atom)>,
    rhs_conds:    Vec<Expr>
}

pub enum Expr {
    EVar          {pos: Pos, var: String},
    EBuiltin      {pos: Pos, func: String, args: Vec<Expr>},
    EField        {pos: Pos, expr: Box<Expr>, field: String},
    EBool         {pos: Pos, val: bool},
    EInt          {pos: Pos, val: BigInt},
    EString       {pos: Pos, val: String},
    EBit          {pos: Pos, width: usize, val: BigUint},
    EStruct       {pos: Pos, constructor: String, fields: Vec<Expr>},
    ETuple        {pos: Pos, fields: Vec<Expr>},
    ESlice        {pos: Pos, arg: Box<Expr>, h: usize, l: usize},
    EMatch        {pos: Pos, match_expr: Box<Expr>, cases: Vec<(Expr, Expr)>},
    EITE          {pos: Pos, cond: Box<Expr>, th: Box<Expr>, el: Option<Box<Expr>>},
    EBinOp        {pos: Pos, op: BOp, left: Box<Expr>, right: Box<Expr>},
    EUnOp         {pos: Pos, op: UOp, arg: Box<Expr>},
    EPHolder      {pos: Pos}
}

pub struct DatalogProgram {
    typedefs:  HashMap<String, TypeDef>,
    relations: HashMap<String, Relation>,
    rules:     Vec<Rule>
}
