use crate::ast::{Declaration, DeclarationKind, Fact, Function, Ident, IdentPath, Relation, Rule};
use std::collections::HashMap;

#[derive(Debug)]
pub struct SymbolTable {
    // TODO: Intern strings
    relations: HashMap<Ident, (Relation, Vec<Rule>, Vec<Fact>)>,
    functions: HashMap<IdentPath, Function>,
    // TODO: Types
    // TODO: Constructors
}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            functions: HashMap::new(),
        }
    }

    pub fn resolve(&mut self, declarations: Vec<Declaration>) -> Result<(), ()> {
        for declaration in declarations {
            match declaration.kind {
                DeclarationKind::Relation(relation) => {
                    // TODO: Check for double-insertion
                    self.relations
                        .insert(relation.name.clone(), (relation, Vec::new(), Vec::new()));
                }

                DeclarationKind::Rule(_) => {}

                DeclarationKind::Fact(_) => {}

                DeclarationKind::Function(function) => {
                    // TODO: Check for double-insertion
                    self.functions.insert(function.name.clone(), function);
                }
            }
        }

        Ok(())
    }
}
