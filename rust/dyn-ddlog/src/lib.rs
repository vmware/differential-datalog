pub mod ast;
mod grammar;
mod optimize;
mod resolve;

pub use grammar::DatalogParser;
pub use resolve::{Resolver, Symbol};
