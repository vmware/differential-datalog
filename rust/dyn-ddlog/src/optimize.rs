//! Query and expression optimization using [`egg`]
//!
//! [`egg`]: https://egraphs-good.github.io/

use egg::{rewrite, Analysis, Id, Subst};
use std::collections::HashSet;

type EGraph = egg::EGraph<DDlogExpr, DDlogExprAnalysis>;
pub type Rewrite = egg::Rewrite<DDlogExpr, ConstantFold>;

egg::define_language! {
    pub enum DDlogExpr {
        "-"   = Neg(Id),

        "*"   = Mul([Id; 2]),
        "/"   = Div([Id; 2]),
        "-"   = Sub([Id; 2]),
        "+"   = Add([Id; 2]),

        ">>"  = Shr([Id; 2]),
        "<<"  = Shl([Id; 2]),

        "++"  = Concat([Id; 2]),

        "=="  = Eq([Id; 2]),
        "!="  = Neq([Id; 2]),
        ">"   = Greater([Id; 2]),
        "<"   = Less([Id; 2]),
        ">="  = GreaterEq([Id; 2]),
        "<="  = LessEq([Id; 2]),

        "~"   = BitNot(Id),
        "&"   = BitAnd([Id; 2]),
        "|"   = BitOr([Id; 2]),

        "not" = LogNot(Id),
        "and" = LogAnd([Id; 2]),
        "or"  = LogOr([Id; 2]),

        "if"  = If([Id; 3]),

        "pow" = Pow([Id; 2]),

        Bool(bool),
        Int(i64),
        String(String),
        Symbol(egg::Symbol),
    }
}

impl DDlogExpr {
    fn int(&self) -> Option<i64> {
        match *self {
            Self::Int(int) => Some(int),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct DDlogExprAnalysis;

#[derive(Debug)]
struct ExprData {
    free: HashSet<Id>,
    constant: Option<DDlogExpr>,
}

fn eval(egraph: &EGraph, enode: &DDlogExpr) -> Option<DDlogExpr> {
    let x = |i: &Id| egraph[*i].data.constant.clone();

    match enode {
        DDlogExpr::Int(_) | DDlogExpr::Bool(_) => Some(enode.clone()),
        DDlogExpr::Add([a, b]) => Some(DDlogExpr::Int(x(a)?.int()? + x(b)?.int()?)),
        DDlogExpr::Eq([a, b]) => Some(DDlogExpr::Bool(x(a)? == x(b)?)),
        _ => None,
    }
}

impl Analysis<DDlogExpr> for DDlogExprAnalysis {
    type Data = ExprData;

    fn make(egraph: &egg::EGraph<DDlogExpr, Self>, enode: &DDlogExpr) -> Self::Data {
        todo!()
    }

    fn merge(&self, to: &mut Self::Data, from: Self::Data) -> bool {
        todo!()
    }
}

fn is_not_zero(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let var = var.parse().unwrap();
    let zero = DDlogExpr::Int(0.into());

    move |egraph, _, subst| !egraph[subst[var]].nodes.contains(&zero)
}

pub fn rules() -> Vec<Rewrite> {
    let mut rules = vec![
        rewrite!("comm-eq"; "(= ?a ?b)" => "(= ?b ?a)"),
        rewrite!("comm-neq"; "(!= ?a ?b)" => "(!= ?b ?a)"),
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
    // (x × y)ⁿ = xⁿ * yⁿ
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
    // (x × y)ⁿ = xⁿ × yⁿ
    rules.extend(rewrite!(
        "exponentiation-identity-add";
        "(pow (* ?x ?y) ?z)" <=> "(* (pow ?x ?z) (pow ?y ?z))"
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

    rules.extend(rewrite!("pow2"; "(pow ?x 2)" <=> "(* ?x ?x)"));
    rules.extend(rewrite!("pow-mul"; "(* (pow ?a ?b) (pow ?a ?c))" <=> "(pow ?a (+ ?b ?c))"));
    rules.extend(rewrite!("zero-add"; "(+ ?a 0)" <=> "?a"));
    rules.extend(rewrite!("one-mul"; "(* ?a 1)" <=> "?a"));
    rules.extend(rewrite!("add-zero"; "?a" <=> "(+ ?a 0)"));
    rules.extend(rewrite!("pow-recip"; "(pow ?x -1)" <=> "(/ 1 ?x)" if is_not_zero("?x")));
    rules.extend(rewrite!(
        "simplify-greater-than";
        "(>= ?x ?y)" <=> "(or (> ?x ?y) (= ?x ?y))"
    ));
    rules.extend(rewrite!(
        "simplify-less-than";
        "(<= ?x ?y)" <=> "(or (< ?x ?y) (= ?x ?y))"
    ));

    rules
}
