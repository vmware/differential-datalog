use crate::ast::{
    transform::ExpressionTransformer, Attribute, Declaration, DeclarationKind, Expr, ExprKind,
    Fact, Function, Ident, Path, Relation, Rule, RuleClause, RuleHead, Type,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::{self, Debug},
    hash::Hash,
    num::NonZeroU32,
};

#[derive(Debug)]
pub struct Resolver {
    frames: Vec<ContextFrame>,
    generator: u32,
}

impl Resolver {
    pub fn new() -> Self {
        Self {
            frames: vec![ContextFrame::new()],
            generator: 1,
        }
    }

    fn next_symbol(&mut self) -> Symbol {
        let id = Symbol::new(NonZeroU32::new(self.generator).unwrap());
        self.generator += 1;

        id
    }

    fn frame(&self) -> &ContextFrame {
        self.frames
            .last()
            .expect("zero context frames within the resolver")
    }

    fn frame_mut(&mut self) -> &mut ContextFrame {
        self.frames
            .last_mut()
            .expect("zero context frames within the resolver")
    }

    pub fn get_relation(&self, relation: &Ident) -> Option<&PartialRelation> {
        self.frames
            .iter()
            .rev()
            .find_map(|frame| frame.get_relation(relation))
    }

    fn bind_var(&mut self, variable: Ident) -> Symbol {
        let variable_id = self.next_symbol();
        self.bind_var_raw(variable, variable_id);

        variable_id
    }

    /// Bind a variable, pushing a context frame if necessary
    fn bind_var_raw(&mut self, variable: Ident, variable_id: Symbol) {
        let needs_scope = self.frame().contains_var(&variable);
        if needs_scope {
            self.push_scope();
        }

        let frame = self.frame_mut();
        debug_assert!(!frame.contains_var(&variable));

        frame.variables.insert(variable, variable_id);
    }

    pub fn get_var(&self, variable: &Ident) -> Option<Symbol> {
        self.frames
            .iter()
            .rev()
            .find_map(|frame| frame.get_var(variable))
    }

    pub fn var_in_scope(&self, variable: &Ident) -> bool {
        self.frames
            .iter()
            .rev()
            .any(|frame| frame.contains_var(variable))
    }

    fn bind_relation(&mut self, relation: Ident, args: HashMap<Ident, Symbol>) -> Symbol {
        let relation_id = self.next_symbol();
        let frame = self.frame_mut();

        // FIXME: Relations should probably search up scopes too
        match frame.relations.entry(relation) {
            Entry::Occupied(_) => {
                // FIXME: Make this an actual error
                panic!("made two relations with the same name, make a real error")
            }

            Entry::Vacant(vacant) => {
                vacant.insert(PartialRelation::new(relation_id, args));
                relation_id
            }
        }
    }

    fn bind_function(&mut self, function: Path<Ident>) -> Symbol {
        let function_id = self.next_symbol();
        let frame = self.frame_mut();

        // TODO: Clarify function scoping rules, is this valid?
        //       ```
        //       function foo() {
        //           function foo() { }
        //       }
        //       ```
        // TODO: The function's path probably needs a module-based lookup
        match frame.functions.entry(function) {
            Entry::Occupied(_) => {
                // FIXME: Make this an actual error
                panic!("made two functions with the same name, make a real error")
            }

            Entry::Vacant(vacant) => {
                vacant.insert(Path::new(vec![function_id]));
                function_id
            }
        }
    }

    pub fn get_function(&self, function: &Path<Ident>) -> Option<&Path<Symbol>> {
        // TODO: The function's path probably needs a module-based lookup
        self.frames
            .iter()
            .rev()
            .find_map(|frame| frame.get_function(function))
    }

    fn scope<F, R>(&mut self, scoped: F) -> R
    where
        F: FnOnce(&mut Self) -> R,
    {
        // Remember how many scopes were previously on the stack
        let safepoint_len = self.frames.len();

        self.push_scope();
        let result = scoped(self);

        // Pop all extra scopes added within this one, restoring the
        // frames to be the same as when we started
        while self.frames.len() > safepoint_len {
            self.pop_scope();
        }

        result
    }

    fn push_scope(&mut self) {
        self.frames.push(ContextFrame::new());
    }

    fn pop_scope(&mut self) {
        debug_assert!(self.frames.pop().is_some());
    }

    pub fn resolve_declarations(
        &mut self,
        decls: &[Declaration<Ident>],
    ) -> Vec<Declaration<Symbol>> {
        // Collect declarations
        // TODO: Rayon
        for decl in decls {
            match &decl.kind {
                DeclarationKind::Relation(relation) => {
                    let args = relation
                        .args
                        .iter()
                        .map(|(name, _)| (name.clone(), self.next_symbol()))
                        .collect();

                    self.bind_relation(relation.name.clone(), args);
                }

                DeclarationKind::Function(func) => {
                    self.bind_function(func.name.clone());
                }

                DeclarationKind::Rule(_rule) => {
                    // // If all the relations & functions are on-hand to attempt to resolve
                    // // the rule then do it now while we're here
                    // if rule.head.iter().all(|head| self.can_resolve_head(head))
                    //     && rule
                    //         .clauses
                    //         .iter()
                    //         .all(|clause| self.can_resolve_clause(clause))
                    // {}
                }

                DeclarationKind::Fact(_fact) => {
                    // // If all the relations & functions are on-hand to attempt to resolve
                    // // the fact then do it now while we're here
                    // if fact.head.iter().all(|head| self.can_resolve_head(head)) {}
                }
            }
        }

        // Now we should have everything we need
        // TODO: Rayon
        // TODO: Attach relation & function ids to them in the former loop
        let mut resolved_decls = Vec::with_capacity(decls.len());
        for decl in decls {
            let attributes = decl
                .attributes
                .iter()
                .map(|attr| Attribute {
                    ident: attr.ident.clone(),
                    value: attr.value.as_ref().map(|val| self.resolve_expr(val)),
                })
                .collect();

            let kind = match &decl.kind {
                DeclarationKind::Relation(relation) => {
                    let resolved_rel = self
                        .get_relation(&relation.name)
                        .expect("a relation's name should always be available to itself");

                    let args = relation
                        .args
                        .iter()
                        .map(|(name, ty)| {
                            (
                                resolved_rel
                                    .get_arg(name)
                                    .expect("a relation's argument name should always be available to itself"),
                                self.resolve_type(ty),
                            )
                        })
                        .collect();

                    DeclarationKind::Relation(Relation {
                        kind: relation.kind,
                        name: resolved_rel.relation_id,
                        args,
                    })
                }

                DeclarationKind::Function(function) => {
                    let name = self
                        .get_function(&function.name)
                        .expect("a function's name should always be available to itself")
                        .to_owned();

                    let args: Vec<_> = function
                        .args
                        .iter()
                        .map(|(_, ty)| (self.next_symbol(), self.resolve_type(ty)))
                        .collect();

                    let ret = self.resolve_type(&function.ret);

                    let body = self.scope(|this| {
                        // Add the function arguments to scope
                        for ((name, _), (id, _)) in function.args.iter().zip(args.iter()) {
                            this.bind_var_raw(name.to_owned(), *id);
                        }

                        this.resolve_expr(&function.body)
                    });

                    DeclarationKind::Function(Function {
                        name,
                        args,
                        body,
                        ret,
                    })
                }

                DeclarationKind::Rule(rule) => {
                    let (clauses, heads) = self.scope(|this| {
                        let clauses = this.resolve_rule_clauses(&rule.clauses);

                        // Heads can only use things declared within the clauses, so we
                        // resolve it after resolving all of the clauses
                        let heads = this.resolve_rule_head(&rule.heads);

                        (clauses, heads)
                    });

                    DeclarationKind::Rule(Rule { heads, clauses })
                }

                DeclarationKind::Fact(fact) => {
                    let heads = self.resolve_rule_head(&fact.heads);
                    DeclarationKind::Fact(Fact { heads })
                }
            };

            resolved_decls.push(Declaration { attributes, kind });
        }

        resolved_decls
    }

    fn resolve_rule_head(&mut self, heads: &[RuleHead<Ident>]) -> Vec<RuleHead<Symbol>> {
        let mut resolved_heads = Vec::with_capacity(heads.len());

        for head in heads {
            // FIXME: Real errors
            let relation = self
                .get_relation(&head.relation)
                .expect("make this an error")
                .relation_id;
            let fields = self.resolve_exprs(&head.fields);

            resolved_heads.push(RuleHead { relation, fields });
        }

        resolved_heads
    }

    fn resolve_rule_clauses(&mut self, clauses: &[RuleClause<Ident>]) -> Vec<RuleClause<Symbol>> {
        let mut resolved_clauses = Vec::with_capacity(clauses.len());

        for clause in clauses {
            resolved_clauses.push(self.resolve_rule_clause(clause));
        }

        resolved_clauses
    }

    fn resolve_rule_clause(&mut self, clause: &RuleClause<Ident>) -> RuleClause<Symbol> {
        match clause {
            RuleClause::Relation {
                binding,
                relation,
                fields,
            } => {
                let binding = binding.clone().map(|binding| self.bind_var(binding));
                // FIXME: Real errors
                let relation = self
                    .get_relation(relation)
                    .expect("make this an error")
                    .relation_id;

                // Raw idents not in scope & pattern bindings are variable declarations, not references
                let mut resolved_fields = Vec::with_capacity(fields.len());
                for unresolved in fields {
                    // TODO: Patterns
                    if let ExprKind::Ident(ident) = &unresolved.kind {
                        if !self.var_in_scope(ident) {
                            let symbol = self.bind_var(ident.to_owned());
                            resolved_fields.push(Expr::ident(symbol));
                            continue;
                        }
                    }

                    resolved_fields.push(self.resolve_expr(unresolved));
                }

                RuleClause::Relation {
                    binding,
                    relation,
                    fields: resolved_fields,
                }
            }

            RuleClause::Negated(negated) => {
                RuleClause::Negated(Box::new(self.resolve_rule_clause(negated)))
            }

            RuleClause::Expr(expr) => RuleClause::Expr(self.resolve_expr(expr)),
        }
    }

    fn resolve_type(&self, ty: &Type<Ident>) -> Type<Symbol> {
        match ty {
            Type::Unit => Type::Unit,
            Type::BigInt => Type::BigInt,
            Type::Bool => Type::Bool,
            Type::String => Type::String,
            &Type::BitVec(width) => Type::BitVec(width),
            &Type::Signed(width) => Type::Signed(width),
            Type::Double => Type::Double,
            Type::Float => Type::Float,
            Type::Tuple(elements) => Type::Tuple(self.resolve_types(elements)),

            Type::Named { path, generics } => {
                let path = self.resolve_type_path(path);
                let generics = self.resolve_types(generics);

                Type::Named { path, generics }
            }
        }
    }

    fn resolve_types(&self, types: &[Type<Ident>]) -> Vec<Type<Symbol>> {
        let mut resolved_types = Vec::with_capacity(types.len());
        for ty in types {
            resolved_types.push(self.resolve_type(ty));
        }

        resolved_types
    }

    fn resolve_type_path(&self, _path: &Path<Ident>) -> Path<Symbol> {
        // TODO: make a searchable module hierarchy
        todo!("make a searchable module hierarchy")
    }

    fn resolve_exprs(&mut self, exprs: &[Expr<Ident>]) -> Vec<Expr<Symbol>> {
        let mut resolved_exprs = Vec::with_capacity(exprs.len());
        for expr in exprs {
            resolved_exprs.push(self.resolve_expr(expr));
        }

        resolved_exprs
    }

    fn resolve_expr(&mut self, expr: &Expr<Ident>) -> Expr<Symbol> {
        self.transform(expr)
    }

    // TODO: Are things too complex to be able to do this kind of eager work?
    //       Does stuff like function pointers preclude that?
    //
    //    fn has_relation(&self, relation_name: &Ident) -> bool {
    //        self.relations.contains_key(relation_name)
    //    }
    //
    // fn can_resolve_head(&self, head: &RuleHead<Ident>) -> bool {
    //     self.has_relation(&head.relation)
    //         && head.fields.iter().all(|field| self.can_resolve_expr(field))
    // }
    //
    // fn can_resolve_clause(&self, clause: &RuleClause<Ident>) -> bool {
    //     match clause {
    //         RuleClause::Relation {
    //             relation, fields, ..
    //         } => {
    //             self.has_relation(relation)
    //                 && fields.iter().all(|field| self.can_resolve_expr(field))
    //         }
    //
    //         RuleClause::Negated(negated) => self.can_resolve_clause(negated),
    //
    //         RuleClause::Expr(expr) => self.can_resolve_expr(expr),
    //     }
    // }
    //
    // fn can_resolve_expr(&self, expr: &Expr) -> bool { }
}

impl Default for Resolver {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpressionTransformer<Ident> for Resolver {
    type Output = Expr<Symbol>;

    fn transform_wildcard(&mut self, _wildcard: &Expr<Ident>) -> Self::Output {
        Expr::wildcard()
    }

    fn transform_empty(&mut self, _empty: &Expr<Ident>) -> Self::Output {
        Expr::empty()
    }

    fn transform_literal(&mut self, literal: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Literal(literal) = literal.kind.clone() {
            Expr::literal(literal)
        } else {
            unreachable!()
        }
    }

    fn transform_ident(&mut self, ident: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Ident(ident) = &ident.kind {
            // FIXME: Real errors
            Expr::ident(self.get_var(ident).unwrap_or_else(|| {
                panic!("couldn't find var (make this an error): {:?}", ident);
            }))
        } else {
            unreachable!()
        }
    }

    fn transform_nested(&mut self, nested: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Nested(inner) = &nested.kind {
            Expr::nested(self.transform(inner))
        } else {
            unreachable!()
        }
    }

    fn transform_neg(&mut self, neg: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Neg(inner) = &neg.kind {
            Expr::neg(self.transform(inner))
        } else {
            unreachable!()
        }
    }

    fn transform_not(&mut self, not: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Not(inner) = &not.kind {
            Expr::not(self.transform(inner))
        } else {
            unreachable!()
        }
    }

    fn transform_bitnot(&mut self, bitnot: &Expr<Ident>) -> Self::Output {
        if let ExprKind::BitNot(inner) = &bitnot.kind {
            Expr::bit_not(self.transform(inner))
        } else {
            unreachable!()
        }
    }

    fn transform_ret(&mut self, ret: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Return(inner) = &ret.kind {
            Expr::ret(inner.as_deref().map(|val| self.transform(val)))
        } else {
            unreachable!()
        }
    }

    fn transform_brk(&mut self, brk: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Break(inner) = &brk.kind {
            Expr::brk(inner.as_deref().map(|val| self.transform(val)))
        } else {
            unreachable!()
        }
    }

    fn transform_cont(&mut self, cont: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Continue(inner) = &cont.kind {
            Expr::cont(inner.as_deref().map(|val| self.transform(val)))
        } else {
            unreachable!()
        }
    }

    fn transform_binop(&mut self, binop: &Expr<Ident>) -> Self::Output {
        if let ExprKind::BinOp(lhs, op, rhs) = &binop.kind {
            Expr::bin_op(self.transform(lhs), *op, self.transform(rhs))
        } else {
            unreachable!()
        }
    }

    fn transform_cmp(&mut self, cmp: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Cmp(lhs, kind, rhs) = &cmp.kind {
            Expr::cmp(self.transform(lhs), *kind, self.transform(rhs))
        } else {
            unreachable!()
        }
    }

    fn transform_and(&mut self, and: &Expr<Ident>) -> Self::Output {
        if let ExprKind::And(lhs, rhs) = &and.kind {
            Expr::and(self.transform(lhs), self.transform(rhs))
        } else {
            unreachable!()
        }
    }

    fn transform_or(&mut self, or: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Or(lhs, rhs) = &or.kind {
            Expr::or(self.transform(lhs), self.transform(rhs))
        } else {
            unreachable!()
        }
    }

    fn transform_block(&mut self, block: &Expr<Ident>) -> Self::Output {
        if let ExprKind::Block(block) = &block.kind {
            let mut resolved_block = Vec::with_capacity(block.len());
            for expr in block {
                resolved_block.push(self.transform(expr));
            }

            Expr::block(resolved_block)
        } else {
            unreachable!()
        }
    }

    fn transform_if(&mut self, if_: &Expr<Ident>) -> Self::Output {
        if let ExprKind::If(cond, then, else_) = &if_.kind {
            Expr::if_(
                self.transform(cond),
                self.transform(then),
                self.transform(else_),
            )
        } else {
            unreachable!()
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Symbol(NonZeroU32);

impl Symbol {
    pub const fn new(id: NonZeroU32) -> Self {
        Self(id)
    }
}

// A custom debug implementation to help with the readability of asts
impl Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Symbol({})", self.0.get())
    }
}

#[derive(Debug)]
struct ContextFrame {
    // TODO: Fast hasher
    relations: HashMap<Ident, PartialRelation>,
    functions: HashMap<Path<Ident>, Path<Symbol>>,
    variables: HashMap<Ident, Symbol>,
}

impl ContextFrame {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            functions: HashMap::new(),
            variables: HashMap::new(),
        }
    }

    pub fn get_relation(&self, relation: &Ident) -> Option<&PartialRelation> {
        self.relations.get(relation)
    }

    fn contains_var(&self, variable: &Ident) -> bool {
        self.variables.contains_key(variable)
    }

    pub fn get_var(&self, variable: &Ident) -> Option<Symbol> {
        self.variables.get(variable).copied()
    }

    pub fn get_function(&self, function: &Path<Ident>) -> Option<&Path<Symbol>> {
        self.functions.get(function)
    }
}

#[derive(Debug)]
pub struct PartialRelation {
    relation_id: Symbol,
    args: HashMap<Ident, Symbol>,
}

impl PartialRelation {
    pub fn new(relation_id: Symbol, args: HashMap<Ident, Symbol>) -> Self {
        Self { relation_id, args }
    }

    pub fn get_arg(&self, arg: &Ident) -> Option<Symbol> {
        self.args.get(arg).copied()
    }
}
