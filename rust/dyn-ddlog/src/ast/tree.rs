use std::mem;

// TODO: Source locations

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Declaration {
    pub attributes: Vec<Attribute>,
    pub kind: DeclarationKind,
}

impl Declaration {
    pub const fn new(attributes: Vec<Attribute>, kind: DeclarationKind) -> Self {
        Self { attributes, kind }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DeclarationKind {
    Relation(Relation),
    Rule(Rule),
    Fact(Fact),
    Function(Function),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Relation {
    pub kind: RelationKind,
    pub name: Ident,
    pub args: Vec<(Ident, Type)>,
}

impl Relation {
    pub const fn new(kind: RelationKind, name: Ident, args: Vec<(Ident, Type)>) -> Self {
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
pub struct Rule {
    pub head: Vec<RuleHead>,
    pub clauses: Vec<RuleClause>,
}

impl Rule {
    pub const fn new(head: Vec<RuleHead>, clauses: Vec<RuleClause>) -> Self {
        Self { head, clauses }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuleHead {
    pub relation: Ident,
    pub fields: Vec<Expr>,
}

impl RuleHead {
    pub const fn new(relation: Ident, fields: Vec<Expr>) -> Self {
        Self { relation, fields }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RuleClause {
    Relation {
        binding: Option<Ident>,
        relation: Ident,
        fields: Vec<Expr>,
    },
    Negated(Box<Self>),
    Expr(Expr),
}

impl RuleClause {
    pub fn relation(binding: Option<Ident>, relation: Ident, fields: Vec<Expr>) -> Self {
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
pub struct Fact {
    pub head: Vec<RuleHead>,
}

impl Fact {
    pub fn new(head: Vec<RuleHead>) -> Self {
        Self { head }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Function {
    pub name: IdentPath,
    pub args: Vec<(Ident, Type)>,
    pub ret: Type,
    pub body: Expr,
}

impl Function {
    pub fn new(name: IdentPath, args: Vec<(Ident, Type)>, ret: Type, body: Expr) -> Self {
        Self {
            name,
            args,
            ret,
            body,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Type {
    BigInt,
    Bool,
    String,
    BitVec(u16),
    Signed(u16),
    Double,
    Float,
    Tuple(Vec<Type>),
    Named {
        path: IdentPath,
        generics: Vec<Type>,
    },
    Unit,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    pub ident: Ident,
    pub value: Option<Expr>,
}

impl Attribute {
    pub const fn new(ident: Ident, value: Option<Expr>) -> Self {
        Self { ident, value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdentPath {
    pub path: Vec<Ident>,
}

impl IdentPath {
    pub const fn new(path: Vec<Ident>) -> Self {
        Self { path }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdentKind {
    Uppercase,
    Lowercase,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Expr {
    pub kind: ExprKind,
}

// TODO: Macro this
#[allow(clippy::should_implement_trait)]
impl Expr {
    pub const fn wildcard() -> Self {
        Self {
            kind: ExprKind::Wildcard,
        }
    }

    pub fn nested(expr: Expr) -> Self {
        Self {
            kind: ExprKind::Nested(Box::new(expr)),
        }
    }

    pub fn block(exprs: Vec<Expr>) -> Self {
        Self {
            kind: ExprKind::Block(exprs),
        }
    }

    pub fn literal(literal: Literal) -> Self {
        Self {
            kind: ExprKind::Literal(literal),
        }
    }

    pub fn ident(ident: Ident) -> Self {
        Self {
            kind: ExprKind::Ident(ident),
        }
    }

    pub fn neg(expr: Expr) -> Self {
        Self {
            kind: ExprKind::Neg(Box::new(expr)),
        }
    }

    pub fn bit_not(expr: Expr) -> Self {
        Self {
            kind: ExprKind::BitNot(Box::new(expr)),
        }
    }

    pub fn not(expr: Expr) -> Self {
        Self {
            kind: ExprKind::Not(Box::new(expr)),
        }
    }

    pub fn mul(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::Mul, Box::new(rhs)),
        }
    }

    pub fn div(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::Div, Box::new(rhs)),
        }
    }

    pub fn add(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::Add, Box::new(rhs)),
        }
    }

    pub fn sub(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::Sub, Box::new(rhs)),
        }
    }

    pub fn equal(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::Cmp(Box::new(lhs), CmpKind::Eq, Box::new(rhs)),
        }
    }

    pub fn not_equal(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::Cmp(Box::new(lhs), CmpKind::Neq, Box::new(rhs)),
        }
    }

    pub fn greater(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::Cmp(Box::new(lhs), CmpKind::Greater, Box::new(rhs)),
        }
    }

    pub fn less(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::Cmp(Box::new(lhs), CmpKind::Less, Box::new(rhs)),
        }
    }

    pub fn greater_equal(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::Cmp(Box::new(lhs), CmpKind::GreaterEq, Box::new(rhs)),
        }
    }

    pub fn less_equal(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::Cmp(Box::new(lhs), CmpKind::LessEq, Box::new(rhs)),
        }
    }

    pub fn and(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::And(Box::new(lhs), Box::new(rhs)),
        }
    }

    pub fn or(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::Or(Box::new(lhs), Box::new(rhs)),
        }
    }

    pub fn ret(ret: Option<Expr>) -> Self {
        Self {
            kind: ExprKind::Return(ret.map(Box::new)),
        }
    }

    pub fn brk(brk: Option<Expr>) -> Self {
        Self {
            kind: ExprKind::Break(brk.map(Box::new)),
        }
    }

    pub fn cont(cont: Option<Expr>) -> Self {
        Self {
            kind: ExprKind::Continue(cont.map(Box::new)),
        }
    }

    pub fn shr(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::Shr, Box::new(rhs)),
        }
    }

    pub fn shl(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::Shl, Box::new(rhs)),
        }
    }

    pub fn concat(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::Concat, Box::new(rhs)),
        }
    }

    pub fn bit_or(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::BitOr, Box::new(rhs)),
        }
    }

    pub fn bit_and(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::BitAnd, Box::new(rhs)),
        }
    }

    pub fn rem(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::BinOp(Box::new(lhs), BinaryOperand::Rem, Box::new(rhs)),
        }
    }

    pub fn if_(cond: Expr, then: Expr, else_: Expr) -> Self {
        Self {
            kind: ExprKind::If(Box::new(cond), Box::new(then), Box::new(else_)),
        }
    }

    pub const fn is_nested(&self) -> bool {
        matches!(self.kind, ExprKind::Nested(_))
    }

    pub const fn is_empty(&self) -> bool {
        matches!(self.kind, ExprKind::Empty)
    }

    pub fn simplify_nesting(self) -> Self {
        struct NestingSimplifier;

        // TODO: Make sure to collapse spans whenever those are implemented
        impl Rewriter for NestingSimplifier {
            /// Collapse nested nestings, e.g. ((1)) => 1
            fn rewrite_nested(&mut self, mut nested: Expr) -> Expr {
                if let ExprKind::Nested(mut inner) = nested.kind {
                    let rewritten = self.rewrite(mem::take(&mut *inner));

                    if let ExprKind::Nested(inner) = rewritten.kind {
                        nested = *inner;

                    // Nesting around nothing does nothing
                    // TODO: This could interact badly with unit literals
                    } else if rewritten.is_empty() {
                        nested.kind = ExprKind::Empty;
                    } else {
                        nested.kind = ExprKind::Nested(Box::new(rewritten));
                    }
                }

                nested
            }

            /// Remove instances of nesting from within blocks, e.g.
            /// { (1) } => { 1 }
            fn rewrite_block(&mut self, mut block: Expr) -> Expr {
                if let ExprKind::Block(block) = &mut block.kind {
                    for expr in block {
                        let rewritten = self.rewrite(mem::take(expr));

                        // Nesting within block expressions is redundant
                        if let ExprKind::Nested(inner) = rewritten.kind {
                            *expr = *inner;
                        } else {
                            *expr = rewritten;
                        }
                    }
                }

                block
            }
        }

        NestingSimplifier.rewrite(self)
    }

    pub fn simplify_blocks(self) -> Self {
        struct BlockSimplifier;

        // TODO: Make sure to collapse spans whenever those are implemented
        impl Rewriter for BlockSimplifier {
            /// Collapse redundant blocks, e.g. { 1 } => 1
            fn rewrite_block(&mut self, mut block: Expr) -> Expr {
                if let ExprKind::Block(block_exprs) = &mut block.kind {
                    let mut idx = 0;
                    while let Some(expr) = block_exprs.get_mut(idx) {
                        *expr = self.rewrite(mem::take(expr));

                        if expr.is_empty() {
                            block_exprs.remove(idx);
                        } else {
                            idx += 1;
                        }
                    }

                    if block_exprs.is_empty() {
                        block.kind = ExprKind::Empty;
                    } else if block_exprs.len() == 1 {
                        block = block_exprs.remove(0);
                    }
                }

                block
            }
        }

        BlockSimplifier.rewrite(self)
    }
}

impl Default for Expr {
    fn default() -> Self {
        Self {
            kind: ExprKind::Empty,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExprKind {
    Wildcard,
    Empty,

    Nested(Box<Expr>),
    Block(Vec<Expr>),

    Literal(Literal),
    Ident(Ident),

    Neg(Box<Expr>),
    Not(Box<Expr>),
    BitNot(Box<Expr>),

    BinOp(Box<Expr>, BinaryOperand, Box<Expr>),

    Cmp(Box<Expr>, CmpKind, Box<Expr>),

    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),

    Return(Option<Box<Expr>>),
    Break(Option<Box<Expr>>),
    Continue(Option<Box<Expr>>),

    If(Box<Expr>, Box<Expr>, Box<Expr>),
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

macro_rules! noop_rewrite {
    ($($rewrite:ident),* $(,)?) => {
        $(
            fn $rewrite(&mut self, expr: Expr) -> Expr {
                expr
            }
        )*
    };
}

macro_rules! unary_rewrite {
    ($kind:ident => $rewrite:ident $(, $($rest:tt)*)?) => {
        fn $rewrite(&mut self, mut expr: Expr) -> Expr {
            if let ExprKind::$kind(inner) = expr.kind {
                // FIXME: Avoid the alloc
                expr.kind = ExprKind::$kind(Box::new(self.rewrite(*inner)));
            }

            expr
        }

        $(unary_rewrite!($($rest)*);)?
    };

    ($kind:ident opt => $rewrite:ident $(, $($rest:tt)*)?) => {
        fn $rewrite(&mut self, mut expr: Expr) -> Expr {
            if let ExprKind::$kind(Some(inner)) = expr.kind {
                // FIXME: Avoid the alloc
                expr.kind = ExprKind::$kind(Some(Box::new(self.rewrite(*inner))));
            }

            expr
        }

        $(unary_rewrite!($($rest)*);)?
    };

    () => { };
}

macro_rules! binary_rewrite {
    ($kind:ident => $rewrite:ident $(, $($rest:tt)*)?) => {
        fn $rewrite(&mut self, mut expr: Expr) -> Expr {
            if let ExprKind::$kind(lhs, rhs) = expr.kind {
                // FIXME: Avoid the allocs
                expr.kind = ExprKind::$kind(
                    Box::new(self.rewrite(*lhs)),
                    Box::new(self.rewrite(*rhs)),
                );
            }

            expr
        }

        $(binary_rewrite!($($rest)*);)?
    };

    ($kind:ident middle => $rewrite:ident $(, $($rest:tt)*)?) => {
        fn $rewrite(&mut self, mut expr: Expr) -> Expr {
            if let ExprKind::$kind(lhs, middle, rhs) = expr.kind {
                // FIXME: Avoid the allocs
                expr.kind = ExprKind::$kind(
                    Box::new(self.rewrite(*lhs)),
                    middle,
                    Box::new(self.rewrite(*rhs)),
                );
            }

            expr
        }

        $(binary_rewrite!($($rest)*);)?
    };

    () => { };
}

pub trait Rewriter {
    fn rewrite(&mut self, expr: Expr) -> Expr {
        let apply = match expr.kind {
            ExprKind::Wildcard => Self::rewrite_wildcard,
            ExprKind::Empty => Self::rewrite_empty,
            ExprKind::Nested(_) => Self::rewrite_nested,
            ExprKind::Block(_) => Self::rewrite_block,
            ExprKind::Literal(_) => Self::rewrite_literal,
            ExprKind::Ident(_) => Self::rewrite_ident,
            ExprKind::Neg(_) => Self::rewrite_neg,
            ExprKind::Not(_) => Self::rewrite_not,
            ExprKind::BitNot(_) => Self::rewrite_bitnot,
            ExprKind::BinOp(_, _, _) => Self::rewrite_binop,
            ExprKind::Cmp(_, _, _) => Self::rewrite_cmp,
            ExprKind::And(_, _) => Self::rewrite_and,
            ExprKind::Or(_, _) => Self::rewrite_or,
            ExprKind::Return(_) => Self::rewrite_ret,
            ExprKind::Break(_) => Self::rewrite_brk,
            ExprKind::Continue(_) => Self::rewrite_cont,
            ExprKind::If(_, _, _) => Self::rewrite_if,
        };

        apply(self, expr)
    }

    noop_rewrite! {
        rewrite_wildcard,
        rewrite_empty,
        rewrite_literal,
        rewrite_ident,
    }

    unary_rewrite! {
        Nested => rewrite_nested,
        Neg => rewrite_neg,
        Not => rewrite_not,
        BitNot => rewrite_bitnot,
        Return opt => rewrite_ret,
        Break opt => rewrite_brk,
        Continue opt => rewrite_cont,
    }

    binary_rewrite! {
        BinOp middle => rewrite_binop,
        Cmp middle => rewrite_cmp,
        And => rewrite_and,
        Or => rewrite_or,
    }

    fn rewrite_block(&mut self, mut block: Expr) -> Expr {
        if let ExprKind::Block(block) = &mut block.kind {
            for expr in block {
                *expr = self.rewrite(mem::take(expr));
            }
        }

        block
    }

    fn rewrite_if(&mut self, mut if_: Expr) -> Expr {
        if let ExprKind::If(cond, then, else_) = if_.kind {
            // FIXME: Avoid the allocs
            if_.kind = ExprKind::If(
                Box::new(self.rewrite(*cond)),
                Box::new(self.rewrite(*then)),
                Box::new(self.rewrite(*else_)),
            );
        }

        if_
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal {
    Int(u64),
    Bool(bool),
}
