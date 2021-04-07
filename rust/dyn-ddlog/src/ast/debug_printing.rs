#![cfg(feature = "debug-printing")]

use crate::ast::{
    Attribute, Declaration, DeclarationKind, Expr, Fact, Function, Ident, IdentKind, Relation,
    Rule, RuleClause, RuleHead,
};
use pretty::{Arena, DocAllocator, DocBuilder};
use std::io::{self, Write};

pub trait DebugAst {
    fn debug_ast(&self) -> String {
        let mut output = Vec::new();
        self.debug_ast_into(&mut output)
            .expect("pretty printing failed");

        String::from_utf8(output).expect("pretty printing generated invalid utf8")
    }

    fn debug_ast_into<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: Write,
    {
        let alloc = Arena::<()>::new();
        self.debug_ast_raw(&alloc).1.render(70, writer)
    }

    #[doc(hidden)]
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone;
}

impl DebugAst for Declaration {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc
            .text("DECLARATION")
            .append(alloc.hardline())
            .append(if self.attributes.is_empty() {
                alloc.nil()
            } else {
                alloc
                    .text(format!("ATTRIBUTES: {}", self.attributes.len()))
                    .append(alloc.hardline())
                    .append(alloc.intersperse(
                        self.attributes.iter().map(|attr| attr.debug_ast_raw(alloc)),
                        alloc.hardline(),
                    ))
                    .append(alloc.hardline())
                    .nest(2)
            })
            .append(self.kind.debug_ast_raw(alloc))
            .nest(2)
    }
}

impl DebugAst for Vec<Declaration> {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc.intersperse(
            self.iter().map(|decl| decl.debug_ast_raw(alloc)),
            alloc.hardline(),
        )
    }
}

impl DebugAst for Attribute {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc
            .text("ATTRIBUTE")
            .append(alloc.hardline())
            .append(self.ident.debug_ast_raw(alloc))
            .nest(2)
            .append(if let Some(value) = self.value.as_ref() {
                alloc
                    .hardline()
                    .append(alloc.text("VALUE:"))
                    .append(alloc.space())
                    .append(value.debug_ast_raw(alloc))
                    .nest(2)
            } else {
                alloc.nil()
            })
    }
}

impl DebugAst for DeclarationKind {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        match self {
            Self::Relation(relation) => relation.debug_ast_raw(alloc),
            Self::Rule(rule) => rule.debug_ast_raw(alloc),
            Self::Fact(fact) => fact.debug_ast_raw(alloc),
            Self::Function(function) => function.debug_ast_raw(alloc),
        }
    }
}

impl DebugAst for Relation {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc.text("RELATION").append(
            alloc
                .hardline()
                .append(self.name.debug_ast_raw(alloc))
                .append(alloc.hardline())
                .append(alloc.text(format!("KIND: {:?}", self.kind)))
                .append(alloc.hardline())
                .append(alloc.text("ARGS"))
                .append(alloc.hardline())
                .nest(2),
        )
    }
}

impl DebugAst for Rule {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc
            .text("RULE")
            .append(alloc.hardline())
            .append(alloc.intersperse(
                self.head.iter().map(|head| head.debug_ast_raw(alloc)),
                alloc.hardline(),
            ))
            .append(alloc.hardline())
            .append(alloc.text(format!("CLAUSES: {}", self.clauses.len())))
            .append(alloc.hardline())
            .append(
                alloc.intersperse(
                    self.clauses
                        .iter()
                        .map(|clause| clause.debug_ast_raw(alloc)),
                    alloc.hardline(),
                ),
            )
            .append(alloc.hardline())
            .nest(2)
    }
}

impl DebugAst for Fact {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc
            .text("FACT")
            .append(alloc.hardline())
            .append(alloc.intersperse(
                self.head.iter().map(|head| head.debug_ast_raw(alloc)),
                alloc.hardline(),
            ))
            .nest(2)
    }
}

impl DebugAst for RuleHead {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc
            .text("RULE HEAD")
            .append(alloc.hardline())
            .append(self.relation.debug_ast_raw(alloc))
            .append(alloc.hardline())
            .append(alloc.text("FIELDS"))
            .nest(2)
    }
}

impl DebugAst for RuleClause {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        match self {
            Self::Relation { .. } => alloc.text("RELATION"),

            Self::Negated(clause) => alloc
                .text("NEGATED")
                .append(alloc.hardline())
                .append(clause.debug_ast_raw(alloc))
                .nest(2),

            Self::Expr(expr) => alloc
                .text("EXPR")
                .append(alloc.hardline())
                .append(expr.debug_ast_raw(alloc))
                .nest(2),
        }
    }
}

impl DebugAst for Function {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc.text("FUNCTION")
    }
}

impl DebugAst for Expr {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc.text("EXPR")
    }
}

impl DebugAst for Ident {
    fn debug_ast_raw<'b, D, A>(&self, alloc: &'b D) -> DocBuilder<'b, D, A>
    where
        D: DocAllocator<'b, A>,
        D::Doc: Clone,
        A: Clone,
    {
        alloc
            .text("IDENT:")
            .append(alloc.space())
            .append(alloc.text(self.ident.clone()).double_quotes())
            .append(alloc.text(","))
            .append(alloc.space())
            .append(match self.kind {
                IdentKind::Uppercase => alloc.text("Lowercase"),
                IdentKind::Lowercase => alloc.text("Uppercase"),
            })
    }
}
