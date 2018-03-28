use syntax::*;
use nom::*;
use std::collections::HashSet;
use std::iter::FromIterator;
use nom::AsChar;
use std::str::from_utf8;
use std::borrow::Borrow;
use std::string::String;

enum Decl {
    TypeDef(TypeDef),
    Relation(Relation),
    Rule(Rule)
}

struct DatalogParser {
    reserved_words: HashSet<&'static str>
}


named!(letter<&[u8], char>, 
       verify!(map!(take!(1), |cs| cs[0].as_char()), |c:char|c.is_alphabetic()));

/* Anything that looks like identifier: sequence of letters and numbers starting 
 * with a letter */
named!(identifier<&[u8], String >,  
       map!(tuple!(letter, alphanumeric), |(c,cs)| format!("{}{}", c, from_utf8(cs).unwrap())));



impl DatalogParser {
    fn new() -> DatalogParser {
        let keywords = ["typedef"];
        DatalogParser {reserved_words: HashSet::from_iter(keywords.iter().cloned())}
    }


    /* Identifier that is not a reserved keyword */
    method!(ident<DatalogParser, &[u8], String >, self, 
            verify!(identifier, |s: String| !self.reserved_words.contains(String::as_str(&s))));


    //method!(identifier<DatalogParser>(&[u8], &str), ), 
}

/*
fn keyword<'a>(input: &'a[u8], k: &str) -> IResult<&'a[u8],&'a[u8]> {
    tag!(input, k)
}

named!(reserved, )

    tag!*/
