use super::ExpressionRewriter;
use crate::ast::{Expr, ExprKind};
use std::mem;

/// Completely removes all `Nested` expressions since they're redundant in the ast
pub struct NestingEliminator;

impl NestingEliminator {
    pub fn new() -> Self {
        Self
    }
}

// TODO: Make sure to collapse spans whenever those are implemented
impl<I> ExpressionRewriter<I> for NestingEliminator {
    /// Collapse nested nestings, e.g. ((1)) => 1
    fn rewrite_nested(&mut self, mut nested: Expr<I>) -> Expr<I> {
        if let ExprKind::Nested(mut inner) = nested.kind {
            nested = self.rewrite(mem::take(&mut *inner));
        }

        nested
    }
}

impl Default for NestingEliminator {
    fn default() -> Self {
        Self::new()
    }
}
