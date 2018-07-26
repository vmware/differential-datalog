#[macro_use]
extern crate nom;
extern crate num;

use num::bigint::*;
use nom::*;

#[derive(Debug,PartialEq,Eq,Clone)]
pub enum Value {
    Bool(bool),
    Int(BigInt),
    String(String),
    Tuple(Vec<Value>),
    Struct(String, Vec<Value>)
}

#[derive(Debug,PartialEq,Eq,Clone)]
pub enum Update {
    Insert {rel: String, val: Value},
    Delete {rel: String, val: Value}
}

#[derive(Debug,PartialEq,Eq,Clone)]
pub enum Command {
    Start,
    Commit,
    Rollback,
    ApplyUpdates{updates: Vec<Update>}
}

named!(spaces<&[u8], ()>,
    do_parse!(mspace: opt!(complete!(one_of!(b" \t\r\n"))) >>
              cond!(mspace != None, spaces) >>
              ())
    //take_while!(is_space)
    //do_parse!(many0!(one_of!(b" \t\r\n")) >> (()))
);

named_args!(sym<'a>(s: &str)<()>,
    do_parse!(tag!(s) >> spaces >> ())
);

named!(identifier<&[u8], String>,
    do_parse!(bytes: alphanumeric1 >>
              spaces >>
              (String::from_utf8(bytes.to_vec()).unwrap()))
);

named!(pub parse_command<&[u8], Command>,
   alt!(do_parse!(apply!(sym,"start")    >> (Command::Start))    | 
        do_parse!(apply!(sym,"commit")   >> (Command::Commit))   | 
        do_parse!(apply!(sym,"rollback") >> (Command::Rollback)) |
        do_parse!(updates: delimited!(apply!(sym,"["), separated_list_complete!(apply!(sym,","), update), apply!(sym,"]"))
                  >> (Command::ApplyUpdates{updates})))
);

named!(update<&[u8], Update>,
    alt!(do_parse!(apply!(sym,"insert") >> rec: record >> (Update::Insert{rel: rec.0, val: rec.1})) |
         do_parse!(apply!(sym,"delete") >> rec: record >> (Update::Delete{rel: rec.0, val: rec.1})))
);

named!(record<&[u8], (String, Value)>,
    do_parse!(cons: identifier >>
              val: alt!(delimited!(apply!(sym,"["), value, apply!(sym,"]")) | 
                        delimited!(apply!(sym,"("), apply!(constructor_args, cons.clone()), apply!(sym,")"))) >>
              (cons, val))
);

named!(value<&[u8], Value>,
    alt!(bool_val | string_val | tuple_val | struct_val | int_val )
);

named!(bool_val<&[u8], Value>,
    alt!(do_parse!(apply!(sym,"true")  >> (Value::Bool(true))) |
         do_parse!(apply!(sym,"false") >> (Value::Bool(false))))
);

#[test]
fn test_bool() {
    assert_eq!(bool_val(br"true "), Ok((&br""[..], Value::Bool(true))));
    assert_eq!(bool_val(br"false"), Ok((&br""[..], Value::Bool(false))));
    assert_eq!(value(br"false "), Ok((&br""[..], Value::Bool(false))));
}

named!(string_val<&[u8], Value>,
    do_parse!(
        str: delimited!(
            char!('\"'),
            escaped_transform!(map!(many0!(none_of!(b"\\\"")), |v:Vec<char>|{v.into_iter().collect::<String>().into_bytes()}), '\\', 
                               alt!(tag!("\\") => { |_| &b"\\"[..] }   |
                                    tag!("\"") => { |_| &b"\""[..] }   |
                                    tag!("\'") => { |_| &b"\'"[..] }   |
                                    tag!("n")  => { |_| &b"\n"[..] }   |
                                    tag!("a")  => { |_| &b"\x07"[..] } |
                                    tag!("b")  => { |_| &b"\x08"[..] } |
                                    tag!("f")  => { |_| &b"\x0c"[..] } |
                                    tag!("r")  => { |_| &b"\r"[..] }   |
                                    tag!("t")  => { |_| &b"\t"[..] }   |
                                    tag!("v")  => { |_| &b"\x0b"[..] } |
                                    tag!("?")  => { |_| &b"\x3f"[..] })),
            char!('\"'))
        >>
        spaces
        >>
        (Value::String(String::from_utf8(str).unwrap()))
    )
);

named!(tuple_val<&[u8], Value>,
    delimited!(apply!(sym,"("),
               map!(separated_list!(apply!(sym,","), value), |v|Value::Tuple(v)),
               apply!(sym,")"))
);

#[test]
fn test_tuple() {
    assert_eq!(tuple_val(br"( true, false)"), Ok((&br""[..], Value::Tuple(vec![Value::Bool(true), Value::Bool(false)]))));
}

named!(struct_val<&[u8], Value>,
    do_parse!(
        cons: identifier >>
        val: delimited!(apply!(sym,"{"), apply!(constructor_args, cons), apply!(sym,"}")) >>
        (val))
);

named!(int_val<&[u8], Value>,
    alt!(map!(dec_val, Value::Int) | 
         map!(hex_val, Value::Int) | 
         do_parse!(apply!(sym,"-") >> 
                   val: alt!(hex_val | dec_val) >>
                   (Value::Int(-val))
                  )
        )
);

/*
named!(multi<&[u8], Vec<&[u8]> >, many1!( tag!( "abcd" ) ) );

#[test]
fn dummy_test() {
    let a = b"abcdabcdefgh";
    let b = b"azerty";

    let res = vec![&b"abcd"[..], &b"abcd"[..]];
    assert_eq!(multi(&a[..]),Ok((&b"efgh"[..], res)));
    assert_eq!(multi(&b[..]), Err(Err::Error(error_position!(&b[..], ErrorKind::Many1))));
}*/


#[test]
fn test_int() {
    assert_eq!(int_val(br"1 "), Ok((&br""[..], Value::Int(1_i32.to_bigint().unwrap()))));
    assert_eq!(dec_val(br"1 "), Ok((&br""[..], 1_i32.to_bigint().unwrap())));
}


named!(hex_val<&[u8], BigInt>,
    do_parse!(tag_no_case!("0x") >> 
              val: map!(hex_digit, |bs| BigInt::parse_bytes(bs, 16).unwrap()) >>
              spaces >>
              (val))
);


named!(dec_val<&[u8], BigInt>,
    do_parse!(bs: take_while1!(is_digit) >> 
              spaces >>
              (BigInt::parse_bytes(bs, 10).unwrap()))
);

named_args!(constructor_args(constructor: String)<Value>,
    do_parse!(args: separated_list!(apply!(sym,","), value) >>
              (Value::Struct(constructor, args)))
);
