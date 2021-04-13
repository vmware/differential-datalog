use dyn_ddlog::{
    ast::{rewrites, validate},
    DatalogParser, Resolver,
};
use std::{fs, path::PathBuf};
use structopt::StructOpt;

fn main() {
    let options = Options::from_args();

    match options {
        Options::Build {
            file,
            unstable_args,
        } => {
            let source = fs::read_to_string(&file).unwrap();

            let parser = DatalogParser::new();
            let mut ast = parser.parse(&source).unwrap();

            if unstable_args.iter().any(|arg| arg == "dump-ast") {
                // #[cfg(not(feature = "debug-printing"))]
                println!("{:#?}", ast);

                // #[cfg(feature = "debug-printing")]
                // {
                //     use dyn_ddlog::DebugAst;
                //
                //     let stdout = std::io::stdout();
                //     let mut stdout = stdout.lock();
                //
                //     ast.debug_ast_into(&mut stdout)
                //         .expect("failed to debug ast");
                // }
            }

            if !unstable_args.iter().any(|arg| arg == "skip-ast-opt") {
                let rewrites = rewrites::default_ast_rewrites();

                for mut rewrite in rewrites {
                    rewrites::rewrite_declarations(&mut *rewrite, &mut ast);
                }

                if unstable_args.iter().any(|arg| arg == "dump-ast-opt") {
                    // #[cfg(not(feature = "debug-printing"))]
                    println!("{:#?}", ast);

                    // #[cfg(feature = "debug-printing")]
                    // {
                    //     use dyn_ddlog::DebugAst;
                    //
                    //     let stdout = std::io::stdout();
                    //     let mut stdout = stdout.lock();
                    //
                    //     ast.debug_ast_into(&mut stdout)
                    //         .expect("failed to debug ast");
                    // }
                }
            }

            let mut resolver = Resolver::new();
            let resolved_ast = resolver.resolve_declarations(&ast);

            if unstable_args.iter().any(|arg| arg == "dump-resolved-ast") {
                // #[cfg(not(feature = "debug-printing"))]
                println!("{:#?}", resolved_ast);

                // #[cfg(feature = "debug-printing")]
                // {
                //     use dyn_ddlog::DebugAst;
                //
                //     let stdout = std::io::stdout();
                //     let mut stdout = stdout.lock();
                //
                //     ast.debug_ast_into(&mut stdout)
                //         .expect("failed to debug ast");
                // }
            }

            let validators = validate::default_ast_validators();
            for mut validator in validators {
                for decl in resolved_ast.iter() {
                    validator.validate_declaration(decl);
                }
            }
        }
    }
}

#[derive(Debug, Clone, StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum Options {
    Build {
        file: PathBuf,

        #[structopt(short = "Z", use_delimiter(true), require_delimiter(true))]
        unstable_args: Vec<String>,
    },
}
