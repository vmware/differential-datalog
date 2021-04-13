use crate::ast::rewrites::{BlockSimplifier, ExpressionRewriter, NestingSimplifier};
use std::{
    fmt::{self, Debug, Write},
    slice, vec,
};

pub use crate::resolve::Symbol;

// TODO: Source locations

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Declaration<I> {
    pub attributes: Vec<Attribute<I>>,
    pub kind: DeclarationKind<I>,
}

impl<I> Declaration<I> {
    pub const fn new(attributes: Vec<Attribute<I>>, kind: DeclarationKind<I>) -> Self {
        Self { attributes, kind }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DeclarationKind<I> {
    Relation(Relation<I>),
    Rule(Rule<I>),
    Fact(Fact<I>),
    Function(Function<I>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Relation<I> {
    pub kind: RelationKind,
    pub name: I,
    pub args: Vec<(I, Type<I>)>,
}

impl<I> Relation<I> {
    pub const fn new(kind: RelationKind, name: I, args: Vec<(I, Type<I>)>) -> Self {
        Self { kind, name, args }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelationKind {
    Input,
    Output,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Rule<I> {
    pub heads: Vec<RuleHead<I>>,
    pub clauses: Vec<RuleClause<I>>,
}

impl<I> Rule<I> {
    pub const fn new(heads: Vec<RuleHead<I>>, clauses: Vec<RuleClause<I>>) -> Self {
        Self { heads, clauses }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuleHead<I> {
    pub relation: I,
    pub fields: Vec<Expr<I>>,
}

impl<I> RuleHead<I> {
    pub const fn new(relation: I, fields: Vec<Expr<I>>) -> Self {
        Self { relation, fields }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RuleClause<I> {
    Relation {
        binding: Option<I>,
        relation: I,
        fields: Vec<Expr<I>>,
    },
    Negated(Box<Self>),
    Expr(Expr<I>),
}

impl<I> RuleClause<I> {
    pub fn relation(binding: Option<I>, relation: I, fields: Vec<Expr<I>>) -> Self {
        Self::Relation {
            binding,
            relation,
            fields,
        }
    }

    pub fn negated(clause: Self) -> Self {
        Self::Negated(Box::new(clause))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fact<I> {
    pub heads: Vec<RuleHead<I>>,
}

impl<I> Fact<I> {
    pub fn new(heads: Vec<RuleHead<I>>) -> Self {
        Self { heads }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Function<I> {
    pub name: Path<I>,
    pub args: Vec<(I, Type<I>)>,
    pub ret: Type<I>,
    pub body: Expr<I>,
}

impl<I> Function<I> {
    pub fn new(name: Path<I>, args: Vec<(I, Type<I>)>, ret: Type<I>, body: Expr<I>) -> Self {
        Self {
            name,
            args,
            ret,
            body,
        }
    }
}

// TODO: Custom debug for types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Type<I> {
    Unit,
    BigInt,
    Bool,
    String,
    BitVec(u16),
    Signed(u16),
    Double,
    Float,
    Tuple(Vec<Type<I>>),
    Named {
        path: Path<I>,
        generics: Vec<Type<I>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute<I> {
    pub ident: Ident,
    pub value: Option<Expr<I>>,
}

impl<I> Attribute<I> {
    pub const fn new(ident: Ident, value: Option<Expr<I>>) -> Self {
        Self { ident, value }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Path<I> {
    pub path: Vec<I>,
}

impl<I> Path<I> {
    pub const fn new(path: Vec<I>) -> Self {
        Self { path }
    }
}

// Custom debug implementations to help with the readability of asts
impl<I> Debug for Path<I>
where
    I: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(&self.path).finish()
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ident {
    pub ident: String,
    pub kind: IdentKind,
}

impl Ident {
    pub fn new(ident: String, kind: IdentKind) -> Self {
        debug_assert!(!ident.is_empty());
        debug_assert!({
            let first_char = ident.chars().next().unwrap();

            match kind {
                IdentKind::Uppercase => first_char.is_uppercase(),
                IdentKind::Lowercase => first_char.is_lowercase() || first_char == '_',
            }
        });

        Self { ident, kind }
    }

    pub fn lowercase(ident: String) -> Self {
        Self::new(ident, IdentKind::Lowercase)
    }

    pub fn uppercase(ident: String) -> Self {
        Self::new(ident, IdentKind::Uppercase)
    }
}

// A custom debug implementation to help with the readability of asts
impl Debug for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ident({:?})", &self.ident)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IdentKind {
    Uppercase,
    Lowercase,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Expr<I> {
    pub kind: ExprKind<I>,
}

// TODO: Macro this
#[allow(clippy::should_implement_trait)]
impl<I> Expr<I> {
    pub const fn wildcard() -> Self {
        Self {
            kind: ExprKind::Wildcard,
        }
    }

    pub const fn empty() -> Self {
        Self {
            kind: ExprKind::Empty,
        }
    }

    pub fn nested(expr: Expr<I>) -> Self {
        Self {
            kind: ExprKind::Nested(Box::new(expr)),
        }
    }

    pub fn block(block: Block<I>) -> Self {
        Self {
            kind: ExprKind::Block(block),
        }
    }

    pub fn literal(literal: Literal) -> Self {
        Self {
            kind: ExprKind::Literal(literal),
        }
    }

    pub fn ident(ident: I) -> Self {
        Self {
            kind: ExprKind::Ident(ident),
        }
    }

    pub fn neg(expr: Expr<I>) -> Self {
        Self {
            kind: ExprKind::Neg(Box::new(expr)),
        }
    }

    pub fn bit_not(expr: Expr<I>) -> Self {
        Self {
            kind: ExprKind::BitNot(Box::new(expr)),
        }
    }

    pub fn not(expr: Expr<I>) -> Self {
        Self {
            kind: ExprKind::Not(Box::new(expr)),
        }
    }

    pub fn mul(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::Mul, rhs)
    }

    pub fn div(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::Div, rhs)
    }

    pub fn add(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::Add, rhs)
    }

    pub fn sub(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::Sub, rhs)
    }

    pub fn equal(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::cmp(lhs, CmpKind::Eq, rhs)
    }

    pub fn not_equal(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::cmp(lhs, CmpKind::Neq, rhs)
    }

    pub fn greater(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::cmp(lhs, CmpKind::Greater, rhs)
    }

    pub fn less(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::cmp(lhs, CmpKind::Less, rhs)
    }

    pub fn greater_equal(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::cmp(lhs, CmpKind::GreaterEq, rhs)
    }

    pub fn less_equal(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::cmp(lhs, CmpKind::LessEq, rhs)
    }

    pub fn cmp(lhs: Expr<I>, kind: CmpKind, rhs: Expr<I>) -> Self {
        Self {
            kind: ExprKind::Cmp(Box::new(lhs), kind, Box::new(rhs)),
        }
    }

    pub fn and(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self {
            kind: ExprKind::And(Box::new(lhs), Box::new(rhs)),
        }
    }

    pub fn or(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self {
            kind: ExprKind::Or(Box::new(lhs), Box::new(rhs)),
        }
    }

    pub fn ret(ret: Option<Expr<I>>) -> Self {
        Self {
            kind: ExprKind::Return(ret.map(Box::new)),
        }
    }

    pub fn brk(brk: Option<Expr<I>>) -> Self {
        Self {
            kind: ExprKind::Break(brk.map(Box::new)),
        }
    }

    pub fn cont(cont: Option<Expr<I>>) -> Self {
        Self {
            kind: ExprKind::Continue(cont.map(Box::new)),
        }
    }

    pub fn shr(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::Shr, rhs)
    }

    pub fn shl(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::Shl, rhs)
    }

    pub fn concat(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::Concat, rhs)
    }

    pub fn bit_or(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::BitOr, rhs)
    }

    pub fn bit_and(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::BitAnd, rhs)
    }

    pub fn rem(lhs: Expr<I>, rhs: Expr<I>) -> Self {
        Self::bin_op(lhs, BinaryOperand::Rem, rhs)
    }

    pub fn if_(cond: Expr<I>, then: Expr<I>, else_: Expr<I>) -> Self {
        Self {
            kind: ExprKind::If(Box::new(cond), Box::new(then), Box::new(else_)),
        }
    }

    pub fn bin_op(lhs: Expr<I>, op: BinaryOperand, rhs: Expr<I>) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), op, Box::new(rhs)),
        }
    }

    pub const fn is_nested(&self) -> bool {
        matches!(self.kind, ExprKind::Nested(_))
    }

    pub const fn is_empty(&self) -> bool {
        matches!(self.kind, ExprKind::Empty)
    }

    pub const fn is_literal(&self) -> bool {
        matches!(self.kind, ExprKind::Literal(_))
    }

    pub fn simplify_nesting(self) -> Self {
        NestingSimplifier::new().rewrite(self)
    }

    pub fn simplify_blocks(self) -> Self {
        BlockSimplifier::new().rewrite(self)
    }
}

impl<I> Default for Expr<I> {
    fn default() -> Self {
        Self {
            kind: ExprKind::Empty,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ExprKind<I> {
    Wildcard,
    Empty,

    Nested(Box<Expr<I>>),
    Block(Block<I>),

    Literal(Literal),
    Ident(I),

    Neg(Box<Expr<I>>),
    Not(Box<Expr<I>>),
    BitNot(Box<Expr<I>>),

    BinOp(Box<Expr<I>>, BinaryOperand, Box<Expr<I>>),

    Cmp(Box<Expr<I>>, CmpKind, Box<Expr<I>>),

    And(Box<Expr<I>>, Box<Expr<I>>),
    Or(Box<Expr<I>>, Box<Expr<I>>),

    Return(Option<Box<Expr<I>>>),
    Break(Option<Box<Expr<I>>>),
    Continue(Option<Box<Expr<I>>>),

    If(Box<Expr<I>>, Box<Expr<I>>, Box<Expr<I>>),
}

// A custom debug implementation to help with the readability of asts
impl<I> Debug for ExprKind<I>
where
    I: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprKind::Wildcard => f.write_str("Wildcard"),
            ExprKind::Empty => f.write_str("Empty"),
            ExprKind::Nested(inner) => {
                f.write_str("Nested(")?;
                Debug::fmt(&**inner, f)?;
                f.write_char(')')
            }
            ExprKind::Block(block) => {
                f.write_str("Block(")?;
                Debug::fmt(block, f)?;
                f.write_char(')')
            }
            ExprKind::Literal(literal) => Debug::fmt(literal, f),
            ExprKind::Ident(ident) => Debug::fmt(ident, f),
            ExprKind::Neg(negated) => {
                f.write_str("Neg(")?;
                Debug::fmt(&**negated, f)?;
                f.write_char(')')
            }
            ExprKind::Not(negated) => {
                f.write_str("Not(")?;
                Debug::fmt(&**negated, f)?;
                f.write_char(')')
            }
            ExprKind::BitNot(negated) => {
                f.write_str("BitNot(")?;
                Debug::fmt(&**negated, f)?;
                f.write_char(')')
            }

            ExprKind::BinOp(lhs, op, rhs) => f
                .debug_tuple("BinOp")
                .field(&**lhs)
                .field(op)
                .field(&**rhs)
                .finish(),

            ExprKind::Cmp(lhs, kind, rhs) => f
                .debug_tuple("Cmp")
                .field(&**lhs)
                .field(kind)
                .field(&**rhs)
                .finish(),

            ExprKind::And(lhs, rhs) => f.debug_tuple("And").field(&**lhs).field(&**rhs).finish(),

            ExprKind::Or(lhs, rhs) => f.debug_tuple("Or").field(&**lhs).field(&**rhs).finish(),

            ExprKind::Return(ret) => {
                f.write_str("Return(")?;
                Debug::fmt(&ret.as_deref(), f)?;
                f.write_char(')')
            }
            ExprKind::Break(brk) => {
                f.write_str("Break(")?;
                Debug::fmt(&brk.as_deref(), f)?;
                f.write_char(')')
            }
            ExprKind::Continue(cont) => {
                f.write_str("Continue(")?;
                Debug::fmt(&cont.as_deref(), f)?;
                f.write_char(')')
            }

            ExprKind::If(cond, then, else_) => f
                .debug_tuple("If")
                .field(&**cond)
                .field(&**then)
                .field(&**else_)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Block<I> {
    pub exprs: Vec<Expr<I>>,
    pub semicolon_terminated: bool,
}

impl<I> Block<I> {
    pub fn new(exprs: Vec<Expr<I>>, semicolon_terminated: bool) -> Self {
        Self {
            exprs,
            semicolon_terminated,
        }
    }

    pub fn len(&self) -> usize {
        self.exprs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.exprs.is_empty()
    }

    pub fn remove(&mut self, idx: usize) -> Expr<I> {
        self.exprs.remove(idx)
    }

    pub fn get(&self, idx: usize) -> Option<&Expr<I>> {
        self.exprs.get(idx)
    }

    pub fn get_mut(&mut self, idx: usize) -> Option<&mut Expr<I>> {
        self.exprs.get_mut(idx)
    }

    pub fn iter(&self) -> slice::Iter<'_, Expr<I>> {
        self.exprs.iter()
    }

    pub fn iter_mut(&mut self) -> slice::IterMut<'_, Expr<I>> {
        self.exprs.iter_mut()
    }
}

impl<I> Default for Block<I> {
    fn default() -> Self {
        Self::new(Vec::new(), false)
    }
}

impl<I> IntoIterator for Block<I> {
    type IntoIter = vec::IntoIter<Expr<I>>;
    type Item = Expr<I>;

    fn into_iter(self) -> Self::IntoIter {
        self.exprs.into_iter()
    }
}

impl<'a, I> IntoIterator for &'a Block<I> {
    type IntoIter = slice::Iter<'a, Expr<I>>;
    type Item = &'a Expr<I>;

    fn into_iter(self) -> Self::IntoIter {
        self.exprs.iter()
    }
}

impl<'a, I> IntoIterator for &'a mut Block<I> {
    type IntoIter = slice::IterMut<'a, Expr<I>>;
    type Item = &'a mut Expr<I>;

    fn into_iter(self) -> Self::IntoIter {
        self.exprs.iter_mut()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOperand {
    Mul,
    Div,
    Add,
    Sub,
    Rem,
    Shr,
    Shl,
    BitOr,
    BitAnd,
    Concat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmpKind {
    Eq,
    Neq,
    Greater,
    Less,
    GreaterEq,
    LessEq,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Literal {
    Int(u64),
    Bool(bool),
}

// A custom debug implementation to help with the readability of asts
impl Debug for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(int) => write!(f, "Int({})", int),
            Self::Bool(boolean) => write!(f, "Bool({})", boolean),
        }
    }
}
