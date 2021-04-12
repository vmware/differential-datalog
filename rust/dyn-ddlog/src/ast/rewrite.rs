use crate::ast::{Expr, ExprKind};
use std::mem;

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

pub trait ExpressionRewriter<I> {
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

pub struct BlockSimplifier;

impl BlockSimplifier {
    pub fn new() -> Self {
        Self
    }
}

// TODO: Make sure to collapse spans whenever those are implemented
impl<I> ExpressionRewriter<I> for BlockSimplifier {
    /// Collapse redundant blocks, e.g. { 1 } => 1
    fn rewrite_block(&mut self, mut block: Expr<I>) -> Expr<I> {
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

impl Default for BlockSimplifier {
    fn default() -> Self {
        Self::new()
    }
}

pub struct NestingSimplifier;

impl NestingSimplifier {
    pub fn new() -> Self {
        Self
    }
}

// TODO: Make sure to collapse spans whenever those are implemented
impl<I> ExpressionRewriter<I> for NestingSimplifier {
    /// Collapse nested nestings, e.g. ((1)) => 1
    fn rewrite_nested(&mut self, mut nested: Expr<I>) -> Expr<I> {
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
    fn rewrite_block(&mut self, mut block: Expr<I>) -> Expr<I> {
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

impl Default for NestingSimplifier {
    fn default() -> Self {
        Self::new()
    }
}
