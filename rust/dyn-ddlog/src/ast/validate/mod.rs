use crate::ast::{
    Attribute, Declaration, DeclarationKind, Expr, Fact, Function, Relation, Rule, RuleClause,
    RuleHead, Symbol,
};

pub fn default_ast_validators() -> Vec<Box<dyn Validator<Symbol>>> {
    vec![Box::new(ValidAttributeValues::new())]
}

pub trait Validator<I> {
    fn validate_declaration(&mut self, decl: &Declaration<I>) {
        for attr in decl.attributes.iter() {
            self.validate_attribute(attr);
        }

        match &decl.kind {
            DeclarationKind::Relation(relation) => self.validate_relation(relation),
            DeclarationKind::Rule(rule) => self.validate_rule(rule),
            DeclarationKind::Fact(fact) => self.validate_fact(fact),
            DeclarationKind::Function(function) => self.validate_function(function),
        }
    }

    fn validate_attribute(&mut self, attr: &Attribute<I>) {
        if let Some(value) = attr.value.as_ref() {
            self.validate_expr(value);
        }
    }

    fn validate_relation(&mut self, _relation: &Relation<I>) {}

    fn validate_rule(&mut self, rule: &Rule<I>) {
        for head in rule.heads.iter() {
            self.validate_rule_head(head);
        }

        for clause in rule.clauses.iter() {
            self.validate_rule_clause(clause);
        }
    }

    fn validate_fact(&mut self, fact: &Fact<I>) {
        for head in fact.heads.iter() {
            self.validate_rule_head(head);
        }
    }

    fn validate_rule_head(&mut self, head: &RuleHead<I>) {
        for field in head.fields.iter() {
            self.validate_expr(field);
        }
    }

    fn validate_rule_clause(&mut self, clause: &RuleClause<I>) {
        match clause {
            RuleClause::Relation { fields, .. } => {
                for field in fields {
                    self.validate_expr(field);
                }
            }
            RuleClause::Negated(negated) => self.validate_rule_clause(negated),
            RuleClause::Expr(expr) => self.validate_expr(expr),
        }
    }

    fn validate_function(&mut self, function: &Function<I>) {
        self.validate_expr(&function.body);
    }

    fn validate_expr(&mut self, _expr: &Expr<I>) {}
}

#[derive(Debug)]
pub struct ValidAttributeValues;

impl ValidAttributeValues {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ValidAttributeValues {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> Validator<I> for ValidAttributeValues {
    fn validate_attribute(&mut self, attr: &Attribute<I>) {
        if let Some(value) = attr.value.as_ref() {
            if !value.is_literal() {
                // FIXME: Make this an actual error
                panic!("invalid attribute value")
            }
        }
    }
}
