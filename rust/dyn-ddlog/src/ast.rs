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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    pub ident: Ident,
    pub value: Expr,
}

impl Attribute {
    pub const fn new(ident: Ident, value: Expr) -> Self {
        Self { ident, value }
    }

    pub fn from_many(attributes: Vec<(Ident, Expr)>) -> Vec<Self> {
        attributes
            .into_iter()
            .map(|(ident, value)| Self::new(ident, value))
            .collect()
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

    pub fn parens(expr: Expr) -> Self {
        Self {
            kind: ExprKind::Parens(Box::new(expr)),
        }
    }

    pub fn neg(expr: Expr) -> Self {
        Self {
            kind: ExprKind::Neg(Box::new(expr)),
        }
    }

    pub fn bit_neg(expr: Expr) -> Self {
        Self {
            kind: ExprKind::BitNeg(Box::new(expr)),
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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExprKind {
    Wildcard,
    And(Box<Expr>, Box<Expr>),
    Parens(Box<Expr>),
    Neg(Box<Expr>),
    BitNeg(Box<Expr>),
    Not(Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
}
