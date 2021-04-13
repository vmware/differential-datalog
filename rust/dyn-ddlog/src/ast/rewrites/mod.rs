mod block_simplifier;
mod nesting_eliminator;
mod nesting_simplifier;

pub use block_simplifier::BlockSimplifier;
pub use nesting_eliminator::NestingEliminator;
pub use nesting_simplifier::NestingSimplifier;

use crate::ast::{Declaration, DeclarationKind, Expr, ExprKind, Ident, RuleClause};
use std::mem;

pub fn default_ast_rewrites() -> Vec<Box<dyn ExpressionRewriter<Ident>>> {
    vec![
        Box::new(NestingEliminator::new()),
        Box::new(BlockSimplifier::new()),
        // TODO: Fixpoint optimization
    ]
}

pub fn rewrite_declarations(
    rewrite: &mut dyn ExpressionRewriter<Ident>,
    declarations: &mut Vec<Declaration<Ident>>,
) {
    for declaration in declarations {
        // Rewrite all attribute expressions
        if rewrite.valid_for(ExprLocation::Attribute) {
            for attr in declaration.attributes.iter_mut() {
                if let Some(value) = attr.value.as_mut() {
                    *value = rewrite.rewrite(mem::take(value));
                }
            }
        }

        match &mut declaration.kind {
            DeclarationKind::Relation(_relation) => {}

            DeclarationKind::Rule(rule) => {
                // Rewrite rule heads
                if rewrite.valid_for(ExprLocation::RuleHead) {
                    for head in rule.heads.iter_mut() {
                        for field in head.fields.iter_mut() {
                            *field = rewrite.rewrite(mem::take(field));
                        }
                    }
                }

                // Rewrite rule clauses
                if rewrite.valid_for(ExprLocation::RuleClause) {
                    for clause in rule.clauses.iter_mut() {
                        rewrite_clause(rewrite, clause);
                    }
                }
            }

            DeclarationKind::Fact(fact) => {
                // Rewrite the heads of facts
                if rewrite.valid_for(ExprLocation::FactHead) {
                    for head in fact.heads.iter_mut() {
                        for field in head.fields.iter_mut() {
                            *field = rewrite.rewrite(mem::take(field));
                        }
                    }
                }
            }

            // Rewrite function bodies
            DeclarationKind::Function(function) if rewrite.valid_for(ExprLocation::Function) => {
                function.body = rewrite.rewrite(mem::take(&mut function.body));
            }

            _ => {}
        }
    }
}

fn rewrite_clause(rewrite: &mut dyn ExpressionRewriter<Ident>, clause: &mut RuleClause<Ident>) {
    match clause {
        RuleClause::Relation { fields, .. } => {
            for field in fields {
                *field = rewrite.rewrite(mem::take(field));
            }
        }

        RuleClause::Negated(negated) => rewrite_clause(rewrite, negated),

        RuleClause::Expr(expr) => {
            *expr = rewrite.rewrite(mem::take(expr));
        }
    }
}

macro_rules! noop_rewrite {
    ($($rewrite:ident),* $(,)?) => {
        $(
            fn $rewrite(&mut self, expr: Expr<I>) -> Expr<I> {
                expr
            }
        )*
    };
}

macro_rules! unary_rewrite {
    ($kind:ident => $rewrite:ident $(, $($rest:tt)*)?) => {
        fn $rewrite(&mut self, mut expr: Expr<I>) -> Expr<I> {
            if let ExprKind::$kind(inner) = expr.kind {
                // FIXME: Avoid the alloc
                expr.kind = ExprKind::$kind(Box::new(self.rewrite(*inner)));
            }

            expr
        }

        $(unary_rewrite!($($rest)*);)?
    };

    ($kind:ident ? => $rewrite:ident $(, $($rest:tt)*)?) => {
        fn $rewrite(&mut self, mut expr: Expr<I>) -> Expr<I> {
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
        fn $rewrite(&mut self, mut expr: Expr<I>) -> Expr<I> {
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
        fn $rewrite(&mut self, mut expr: Expr<I>) -> Expr<I> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExprLocation {
    Attribute,
    Function,
    RuleHead,
    RuleClause,
    FactHead,
}

pub trait ExpressionRewriter<I> {
    fn valid_for(&self, _location: ExprLocation) -> bool {
        true
    }

    fn rewrite(&mut self, expr: Expr<I>) -> Expr<I> {
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
        Return? => rewrite_ret,
        Break? => rewrite_brk,
        Continue? => rewrite_cont,
    }

    binary_rewrite! {
        BinOp middle => rewrite_binop,
        Cmp middle => rewrite_cmp,
        And => rewrite_and,
        Or => rewrite_or,
    }

    fn rewrite_block(&mut self, mut block: Expr<I>) -> Expr<I> {
        if let ExprKind::Block(block) = &mut block.kind {
            for expr in block {
                *expr = self.rewrite(mem::take(expr));
            }
        }

        block
    }

    fn rewrite_if(&mut self, mut if_: Expr<I>) -> Expr<I> {
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
