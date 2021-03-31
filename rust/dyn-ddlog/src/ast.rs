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
    Named(Ident),
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

#[allow(clippy::should_implement_trait)]
impl Expr {
    pub const fn wildcard() -> Self {
        Self {
            kind: ExprKind::Wildcard,
        }
    }

    pub fn and(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::And(Box::new(lhs), Box::new(rhs)),
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
            kind: ExprKind::Mul(Box::new(lhs), Box::new(rhs)),
        }
    }

    pub fn div(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::Div(Box::new(lhs), Box::new(rhs)),
        }
    }

    pub fn add(lhs: Expr, rhs: Expr) -> Self {
        Self {
            kind: ExprKind::Add(Box::new(lhs), Box::new(rhs)),
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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExprKind {
    Wildcard,
    And(Box<Expr>, Box<Expr>),
    Nested(Box<Expr>),
    Block(Vec<Expr>),
    Neg(Box<Expr>),
    BitNot(Box<Expr>),
    Not(Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Add(Box<Expr>, Box<Expr>),
    Literal(Literal),
    Ident(Ident),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal {
    Int(u128),
    Bool(bool),
}
