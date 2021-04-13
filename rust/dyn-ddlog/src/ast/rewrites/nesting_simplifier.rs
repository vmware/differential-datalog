use super::ExpressionRewriter;
use crate::ast::{Expr, ExprKind};
use std::mem;

/// Simplifies redundant nesting in the ast
pub struct NestingSimplifier;

impl NestingSimplifier {
    pub fn new() -> Self {
        Self
    }
}

// TODO: Make sure to collapse spans whenever those are implemented
// TODO: Emit lints on redundant nesting
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
