use super::ExpressionRewriter;
use crate::ast::{Expr, ExprKind};
use std::mem;

/// Simplifies block statements
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
