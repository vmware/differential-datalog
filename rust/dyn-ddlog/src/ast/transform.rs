use crate::ast::{Expr, ExprKind};

pub trait ExpressionTransformer<I> {
    type Output;

    fn transform(&mut self, expr: &Expr<I>) -> Self::Output {
        let apply = match expr.kind {
            ExprKind::Wildcard => Self::transform_wildcard,
            ExprKind::Empty => Self::transform_empty,
            ExprKind::Nested(_) => Self::transform_nested,
            ExprKind::Block(_) => Self::transform_block,
            ExprKind::Literal(_) => Self::transform_literal,
            ExprKind::Ident(_) => Self::transform_ident,
            ExprKind::Neg(_) => Self::transform_neg,
            ExprKind::Not(_) => Self::transform_not,
            ExprKind::BitNot(_) => Self::transform_bitnot,
            ExprKind::BinOp(_, _, _) => Self::transform_binop,
            ExprKind::Cmp(_, _, _) => Self::transform_cmp,
            ExprKind::And(_, _) => Self::transform_and,
            ExprKind::Or(_, _) => Self::transform_or,
            ExprKind::Return(_) => Self::transform_ret,
            ExprKind::Break(_) => Self::transform_brk,
            ExprKind::Continue(_) => Self::transform_cont,
            ExprKind::If(_, _, _) => Self::transform_if,
        };

        apply(self, expr)
    }

    fn transform_wildcard(&mut self, wildcard: &Expr<I>) -> Self::Output;

    fn transform_empty(&mut self, empty: &Expr<I>) -> Self::Output;

    fn transform_literal(&mut self, literal: &Expr<I>) -> Self::Output;

    fn transform_ident(&mut self, ident: &Expr<I>) -> Self::Output;

    fn transform_nested(&mut self, nested: &Expr<I>) -> Self::Output;

    fn transform_neg(&mut self, neg: &Expr<I>) -> Self::Output;

    fn transform_not(&mut self, not: &Expr<I>) -> Self::Output;

    fn transform_bitnot(&mut self, bitnot: &Expr<I>) -> Self::Output;

    fn transform_ret(&mut self, ret: &Expr<I>) -> Self::Output;

    fn transform_brk(&mut self, brk: &Expr<I>) -> Self::Output;

    fn transform_cont(&mut self, cont: &Expr<I>) -> Self::Output;

    fn transform_binop(&mut self, binop: &Expr<I>) -> Self::Output;

    fn transform_cmp(&mut self, cmp: &Expr<I>) -> Self::Output;

    fn transform_and(&mut self, and: &Expr<I>) -> Self::Output;

    fn transform_or(&mut self, or: &Expr<I>) -> Self::Output;

    fn transform_block(&mut self, block: &Expr<I>) -> Self::Output;

    fn transform_if(&mut self, if_: &Expr<I>) -> Self::Output;
}
