//! Query and expression optimization using [`egg`]
//!
//! [`egg`]: https://egraphs-good.github.io/

use crate::ast::{BinaryOperand, Expr as AstExpr, ExprKind, Ident, Literal};
use egg::{rewrite, Analysis, Id, Language, Subst};

type EGraph = egg::EGraph<Expr, ExprAnalysis>;
type Rewrite = egg::Rewrite<Expr, ExprAnalysis>;

egg::define_language! {
    pub enum Expr {
        "-"   = Neg(Id),

        "*"   = Mul([Id; 2]),
        "/"   = Div([Id; 2]),
        "-"   = Sub([Id; 2]),
        "+"   = Add([Id; 2]),

        ">>"  = Shr([Id; 2]),
        "<<"  = Shl([Id; 2]),

        "++"  = Concat([Id; 2]),

        "eq"  = Eq([Id; 2]),
        "neq"  = Neq([Id; 2]),
        ">"   = Greater([Id; 2]),
        "<"   = Less([Id; 2]),
        ">="  = GreaterEq([Id; 2]),
        "<="  = LessEq([Id; 2]),

        "~"   = BitNot(Id),
        "&"   = BitAnd([Id; 2]),
        "|"   = BitOr([Id; 2]),

        "not" = Not(Id),
        "and" = And([Id; 2]),
        "or"  = Or([Id; 2]),

        "if"  = If([Id; 3]),

        "pow" = Pow([Id; 2]),

        "block" = Block(Vec<Id>),

        Bool(bool),
        Int(u64),
        String(String),
        Ident(egg::Symbol),
    }
}

impl Expr {
    fn int(&self) -> Option<u64> {
        match *self {
            Self::Int(int) => Some(int),
            _ => None,
        }
    }
}

pub fn build_egraph(expr: &AstExpr<Ident>) -> (EGraph, Id) {
    let mut egraph = EGraph::default();
    let root = translate_expr(&mut egraph, expr);

    (egraph, root)
}

// TODO: Use iteration instead of recursion
// TODO: Make this use canonical ids
fn translate_expr(egraph: &mut EGraph, expr: &AstExpr<Ident>) -> Id {
    match &expr.kind {
        ExprKind::Nested(expr) => translate_expr(egraph, &**expr),

        ExprKind::Block(exprs) => {
            let ids = exprs
                .iter()
                .map(|expr| translate_expr(egraph, expr))
                .collect();

            egraph.add(Expr::Block(ids))
        }

        ExprKind::BinOp(lhs, BinaryOperand::Mul, rhs) => {
            let (lhs, rhs) = (
                translate_expr(egraph, &**lhs),
                translate_expr(egraph, &**rhs),
            );

            egraph.add(Expr::Mul([lhs, rhs]))
        }

        ExprKind::BinOp(lhs, BinaryOperand::Div, rhs) => {
            let (lhs, rhs) = (
                translate_expr(egraph, &**lhs),
                translate_expr(egraph, &**rhs),
            );

            egraph.add(Expr::Div([lhs, rhs]))
        }

        ExprKind::BinOp(lhs, BinaryOperand::Add, rhs) => {
            let (lhs, rhs) = (
                translate_expr(egraph, &**lhs),
                translate_expr(egraph, &**rhs),
            );

            egraph.add(Expr::Add([lhs, rhs]))
        }

        ExprKind::BinOp(lhs, BinaryOperand::Sub, rhs) => {
            let (lhs, rhs) = (
                translate_expr(egraph, &**lhs),
                translate_expr(egraph, &**rhs),
            );

            egraph.add(Expr::Sub([lhs, rhs]))
        }

        ExprKind::Neg(expr) => {
            let expr = translate_expr(egraph, &**expr);
            egraph.add(Expr::Neg(expr))
        }

        ExprKind::And(lhs, rhs) => {
            let (lhs, rhs) = (
                translate_expr(egraph, &**lhs),
                translate_expr(egraph, &**rhs),
            );

            egraph.add(Expr::And([lhs, rhs]))
        }

        ExprKind::Not(expr) => {
            let expr = translate_expr(egraph, &**expr);
            egraph.add(Expr::Not(expr))
        }

        ExprKind::BitNot(expr) => {
            let expr = translate_expr(egraph, &**expr);
            egraph.add(Expr::BitNot(expr))
        }

        ExprKind::Literal(literal) => egraph.add(match *literal {
            Literal::Int(int) => Expr::Int(int),
            Literal::Bool(boolean) => Expr::Bool(boolean),
        }),

        ExprKind::Ident(ident) => egraph.add(Expr::Ident(egg::Symbol::from(&ident.ident))),

        expr => todo!("unimplemented expression: {:?}", expr),
    }
}

#[test]
fn test_translate() {
    use crate::{ast::DeclarationKind, DatalogParser};
    use egg::{AstSize, Extractor, Runner};

    let function = DatalogParser::new()
        .parse(
            "
            function foo() {
                (10 + 10) / 11 - (100 * 0)
            }
            ",
        )
        .unwrap()[0]
        .clone();

    if let DeclarationKind::Function(func) = function.kind {
        let rules = rules();
        let (egraph, root) = build_egraph(&func.body);

        let runner = Runner::default().with_egraph(egraph).run(&rules);

        let mut extractor = Extractor::new(&runner.egraph, AstSize);
        let (best_cost, best) = extractor.find_best(root);

        println!("Best Cost: {}\nRewrite: {}", best_cost, best);
    } else {
        unreachable!()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExprAnalysis;

#[derive(Debug, Clone)]
pub struct ExprData {
    constant: Option<Expr>,
}

fn eval(egraph: &EGraph, enode: &Expr) -> Option<Expr> {
    let x = |i: &Id| egraph[*i].data.constant.clone();

    match enode {
        Expr::Int(_) | Expr::Bool(_) => Some(enode.clone()),
        Expr::Add([a, b]) => Some(Expr::Int(x(a)?.int()? + x(b)?.int()?)),
        Expr::Sub([a, b]) => Some(Expr::Int(x(a)?.int()? - x(b)?.int()?)),
        Expr::Mul([a, b]) => Some(Expr::Int(x(a)?.int()? * x(b)?.int()?)),
        Expr::Div([a, b]) if x(b).and_then(|e| e.int()) != Some(0) => {
            Some(Expr::Int(x(a)?.int()? / x(b)?.int()?))
        }
        Expr::Eq([a, b]) => Some(Expr::Bool(x(a)? == x(b)?)),
        _ => None,
    }
}

impl Analysis<Expr> for ExprAnalysis {
    type Data = ExprData;

    fn make(egraph: &egg::EGraph<Expr, Self>, enode: &Expr) -> Self::Data {
        ExprData {
            constant: eval(egraph, enode),
        }
    }

    fn merge(&self, to: &mut Self::Data, from: Self::Data) -> bool {
        if to.constant.is_none() && from.constant.is_some() {
            to.constant = from.constant;
            true
        } else {
            false
        }
    }

    fn modify(egraph: &mut EGraph, id: Id) {
        let class = &mut egraph[id];

        if let Some(c) = class.data.constant.clone() {
            let added = egraph.add(c);
            let (id, _did_something) = egraph.union(id, added);

            // to not prune, comment this out
            egraph[id].nodes.retain(|n| n.is_leaf());

            assert!(
                !egraph[id].nodes.is_empty(),
                "empty eclass! {:#?}",
                egraph[id],
            );

            #[cfg(debug_assertions)]
            egraph[id].assert_unique_leaves();
        }
    }
}

fn is_not_zero(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let var = var.parse().unwrap();
    let zero = Expr::Int(0);

    move |egraph, _, subst| !egraph[subst[var]].nodes.contains(&zero)
}

fn rules() -> Vec<Rewrite> {
    let mut rules = vec![
        rewrite!("comm-eq"; "(eq ?a ?b)" => "(eq ?b ?a)"),
        rewrite!("comm-neq"; "(neq ?a ?b)" => "(neq ?b ?a)"),
        rewrite!("sub-canon"; "(- ?a ?b)" => "(+ ?a (* -1 ?b))"),
        rewrite!("div-canon"; "(/ ?a ?b)" => "(* ?a (pow ?b -1))" if is_not_zero("?b")),
        rewrite!("distribute"; "(* ?a (+ ?b ?c))" => "(+ (* ?a ?b) (* ?a ?c))"),
        rewrite!("factor"; "(+ (* ?a ?b) (* ?a ?c))" => "(* ?a (+ ?b ?c))"),
        rewrite!("recip-mul-div"; "(* ?x (/ 1 ?x))" => "1" if is_not_zero("?x")),
        rewrite!("if-true"; "(if true ?then ?else)" => "?then"),
        rewrite!("if-false"; "(if false ?then ?else)" => "?else"),
        rewrite!("true-is-true"; "(eq true true)" => "true"),
        rewrite!("false-is-false"; "(eq false false)" => "true"),
    ];

    // Algebraic laws
    rules.extend(vec![
        // x + y = y + x
        rewrite!(
            "commutative-add";
            "(+ ?x ?y)" => "(+ ?y ?x)"
        ),
        // x × y = y × x
        rewrite!(
            "commutative-mul";
            "(* ?x ?y)" => "(* ?y ?x)"
        ),
        // x + (y + z) = (x + y) + z
        rewrite!(
            "associative-add";
            "(+ ?x (+ ?y ?z))" => "(+ (+ ?x ?y) ?z)"
        ),
        // x × (y × z) = (x × y) × z
        rewrite!(
            "associative-mul";
            "(* ?x (* ?y ?z))" => "(* (* ?x ?y) ?z)"
        ),
        // x + 0 = x
        rewrite!(
            "add-identity";
            "(+ ?x 0)" => "?x"
        ),
        // x × 1 = x
        rewrite!(
            "mul-identity";
            "(* ?x 1)" => "?x"
        ),
        // x + (-x) = 0
        rewrite!(
            "add-annihilator";
            "(+ ?x (- ?x))" => "0"
        ),
        // x × 0 = 0
        rewrite!(
            "mul-annihilator";
            "(* ?x 0)" => "0"
        ),
        // x - x = 0
        rewrite!(
            "sub-annihilator";
            "(- ?x ?x)" => "0"
        ),
        // x ÷ x = 1
        rewrite!(
            "div-annihilator";
            "(/ ?x ?x)" => "1"
                if is_not_zero("?x")
        ),
        // -(-x) = x
        rewrite!(
            "double-negative";
            "(- (- ?x))" => "?x"
        ),
        // x¹ = x
        rewrite!(
            "pow-1";
            "(pow ?x 1)" => "?x"
        ),
        // x⁰ = 0
        rewrite!(
            "pow-0";
            "(pow ?x 0)" => "0"
                if is_not_zero("?x")
        ),
        // 1ˣ = 1
        rewrite!(
            "powers-of-1";
            "(pow 1 ?x)" => "1"
        ),
    ]);
    // x² = x × x
    rules.extend(rewrite!("pow2"; "(pow ?x 2)" <=> "(* ?x ?x)"));
    // (x × y)ⁿ = xⁿ × yⁿ
    rules.extend(rewrite!(
        "commutative-pow-mul";
        "(pow (* ?x ?y) ?z)" <=> "(* (pow ?x ?z) (pow ?y ?z))"
    ));
    // (x ÷ y)ⁿ = xⁿ ÷ yⁿ
    rules.extend(rewrite!(
        "commutative-pow-div";
        "(pow (/ ?x ?y) ?z)" <=> "(/ (pow ?x ?z) (pow ?y ?z))"
    ));
    // (xʸ)ⁿ = xʸ×ⁿ
    rules.extend(rewrite!(
        "exponentiation-identity-mul";
        "(pow (pow ?x ?y) ?z)" <=> "(pow ?x (* ?y ?z))"
            if is_not_zero("?x")
    ));
    // xʸ⁺ⁿ = xʸ × xⁿ
    rules.extend(rewrite!(
        "exponentiation-identity-add";
        "(pow ?x (+ ?y ?z))" <=> "(* (pow ?x ?y) (pow ?x ?z))"
            if is_not_zero("?x")
    ));
    // xʸ⁻ⁿ = xʸ ÷ xⁿ
    rules.extend(rewrite!(
        "exponentiation-identity-sub";
        "(pow ?x (- ?y ?z))" <=> "(/ (pow ?x ?y) (pow ?x ?z))"
            if is_not_zero("?x")
    ));

    // Monotone laws of boolean algebra
    rules.extend(vec![
        // x ∧ y = x ∧ y
        rewrite!(
            "commutative-and";
            "(and ?x ?y)" => "(and ?y ?x)"
        ),
        // x ∨ y = y ∨ x
        rewrite!(
            "commutative-or";
            "(or ?x ?y)" => "(or ?y ?x)"
        ),
        // x ∧ (y ∧ z) = (x ∧ y) ∧ z
        rewrite!(
            "associative-and";
            "(and ?x (and ?y ?z))" => "(and (and ?x ?y) ?z)"
        ),
        // x ∨ (y ∨ z) = (x ∨ y) ∨ z
        rewrite!(
            "associative-or";
            "(or ?x (or ?y ?z))" => "(or (or ?x ?y) ?z)"
        ),
        // x ∧ true = x
        rewrite!(
            "and-identity";
            "(and ?x true)" => "?x"
        ),
        // x ∨ false = x
        rewrite!(
            "or-identity";
            "(or ?x false)" => "?x"
        ),
        // x ∧ false = false
        rewrite!(
            "and-annihilator";
            "(and ?x false)" => "false"
        ),
        // x ∨ true = true
        rewrite!(
            "or-annihilator";
            "(or ?x true)" => "true"
        ),
        // x ∧ x = x
        rewrite!(
            "and-idempotence";
            "(and ?x ?x)" => "?x"
        ),
        // x ∨ x = x
        rewrite!(
            "or-idempotence";
            "(or ?x ?x)" => "?x"
        ),
        // x ∧ (x ∨ y) = x
        rewrite!(
            "and-absorption";
            "(and ?x (or ?x ?y))" => "?x"
        ),
        // x ∨ (x ∧ y) = x
        rewrite!(
            "or-absorption";
            "(or ?x (and ?x ?y))" => "?x"
        ),
    ]);
    // x ∧ (y ∨ z) = (x ∧ y) ∨ (x ∧ z)
    rules.extend(rewrite!(
        "distribute-and-over-or";
        "(and ?x (or ?y ?z))" <=> "(or (and ?x ?y) (and ?x ?z))"
    ));
    // x ∨ (y ∧ z) = (x ∨ y) ∧ (x ∨ z)
    rules.extend(rewrite!(
        "distribute-or-over-and";
        "(or ?x (and ?y ?z))" <=> "(and (or ?x ?y) (or ?x ?z))"
    ));

    // Nonmonotone laws of boolean algebra
    rules.extend(vec![
        // x ∧ ¬x = false
        rewrite!(
            "complementation-and";
            "(and ?x (not ?x))" => "false"
        ),
        // x ∨ ¬x = true
        rewrite!(
            "complementation-or";
            "(or ?x (not ?x))" => "true"
        ),
        // ¬(¬x) = x
        rewrite!(
            "double-negation";
            "(not (not ?x))" => "?x"
        ),
    ]);

    // De Morgan's laws for boolean algebra
    // ¬x ∧ ¬y = ¬(x ∨ y)
    rules.extend(rewrite!(
        "negation-of-conjunction-and";
        "(and (not ?x) (not ?y))" <=> "(not (or ?x ?y))"
    ));
    // ¬x ∨ ¬y = ¬(x ∧ y)
    rules.extend(rewrite!(
        "negation-of-conjunction-or";
        "(or (not ?x) (not ?y))" <=> "(not (and ?x ?y))"
    ));

    rules.extend(rewrite!(
        "simplify-greater-than";
        "(>= ?x ?y)" <=> "(or (> ?x ?y) (eq ?x ?y))"
    ));
    rules.extend(rewrite!(
        "simplify-less-than";
        "(<= ?x ?y)" <=> "(or (< ?x ?y) (eq ?x ?y))"
    ));

    rules
}
