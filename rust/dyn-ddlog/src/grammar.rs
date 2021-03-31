pub use ddlog::DatalogParser;

use lalrpop_util::lalrpop_mod;

lalrpop_mod!(ddlog);

// TODO: Use file tests for this
#[cfg(test)]
mod tests {
    use crate::grammar::DatalogParser;

    #[test]
    fn incorrectly_cased_relation() {
        DatalogParser::new()
            .parse("input relation test()")
            .unwrap_err();

        DatalogParser::new()
            .parse("input relation _Test()")
            .unwrap_err();
    }

    #[test]
    fn relation_arg_casing() {
        DatalogParser::new()
            .parse("relation Test(Foo: bool)")
            .unwrap_err();

        DatalogParser::new()
            .parse("relation Test(foo: bool, _bar: bool, Baz: bigint)")
            .unwrap_err();
    }
}
