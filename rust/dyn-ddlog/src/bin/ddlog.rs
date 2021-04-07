use dyn_ddlog::DatalogParser;
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
            let ast = parser.parse(&source).unwrap();

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
