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
    Insert (String, Value),
    Delete (String, Value)
}

#[derive(Debug,PartialEq,Eq,Clone)]
pub enum Command {
    Start,
    Commit,
    Rollback,
    Update(Update, bool)
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
    do_parse!(first: alt!(alpha | tag!("_")) >>
              bytes: alphanumeric0 >>
              spaces >>
              (String::from_utf8(first.to_vec()).unwrap() + &String::from_utf8(bytes.to_vec()).unwrap()))
);

named!(pub parse_command<&[u8], Command>,
    do_parse!(
        spaces >>
        upd: alt!(do_parse!(apply!(sym,"start")    >> apply!(sym,";") >> (Command::Start))    | 
                  do_parse!(apply!(sym,"commit")   >> apply!(sym,";") >> (Command::Commit))   | 
                  do_parse!(apply!(sym,"rollback") >> apply!(sym,";") >> (Command::Rollback)) |
                  do_parse!(upd:  update >>
                            last: alt!(map!(apply!(sym,";"), |_|true) | map!(apply!(sym, ","), |_|false)) >> 
                            (Command::Update(upd, last)))) >>
        (upd)
    )
);

#[test]
fn test_command() {
    assert_eq!(parse_command(br"start;")   , Ok((&br""[..], Command::Start)));
    assert_eq!(parse_command(br"commit;")  , Ok((&br""[..], Command::Commit)));
    assert_eq!(parse_command(br"rollback;"), Ok((&br""[..], Command::Rollback)));
    assert_eq!(parse_command(br"insert Rel1(true);"), 
               Ok((&br""[..], Command::Update(
                   Update::Insert("Rel1".to_string(), Value::Struct("Rel1".to_string(), vec![Value::Bool(true)])),
                   true
               ))));
    assert_eq!(parse_command(br" insert Rel1[true];"), 
               Ok((&br""[..], Command::Update(
                   Update::Insert("Rel1".to_string(), Value::Bool(true)), true
               ))));
    assert_eq!(parse_command(br"delete Rel1[(true,false)];"), 
               Ok((&br""[..], Command::Update(
                   Update::Delete("Rel1".to_string(), Value::Tuple(vec![Value::Bool(true), Value::Bool(false)])), true
               ))));
    assert_eq!(parse_command(br#"   delete Rel2("foo", 0xabcdef1, true) , "#), 
               Ok((&br""[..], Command::Update(
                   Update::Delete("Rel2".to_string(), Value::Struct("Rel2".to_string(), 
                                                                    vec![Value::String("foo".to_string()), 
                                                                         Value::Int(0xabcdef1.to_bigint().unwrap()),
                                                                         Value::Bool(true)])),
                   false
               ))));
}

named!(update<&[u8], Update>,
    alt!(do_parse!(apply!(sym,"insert") >> rec: record >> (Update::Insert(rec.0, rec.1))) |
         do_parse!(apply!(sym,"delete") >> rec: record >> (Update::Delete(rec.0, rec.1))))
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
        str: alt!(
            // I think there is is bug in nom, causing escaped_transform to only work with
            // take_until_either1 but not take_until_either or any other parser that accepts
            // 0-length sequences; therefore we parse empty strings as a special case.
            map!(tag!(b"\"\""), |_| vec![]) |
            delimited!(
                char!('\"'),
                escaped_transform!(take_until_either1!(b"\\\""), '\\', 
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
                char!('\"')
            )
        )
        >>
        spaces
        >>
        (Value::String(String::from_utf8(str).unwrap()))
    )
);

#[test]
fn test_string() {
    assert_eq!(string_val(br###""foo""###), Ok((&br""[..], Value::String("foo".to_string()))));
    assert_eq!(string_val(br###""" "###), Ok((&br""[..], Value::String("".to_string()))));
    assert_eq!(string_val(br###""foo\nbar" "###), Ok((&br""[..], Value::String("foo\nbar".to_string()))));
    assert_eq!(string_val(br###""foo\tbar\t\a" "###), Ok((&br""[..], Value::String("foo\tbar\t\x07".to_string()))));
}

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

#[test]
fn test_struct() {
    assert_eq!(struct_val(br"Constructor { true, false }"), 
               Ok((&br""[..], Value::Struct("Constructor".to_string(),
                                            vec![Value::Bool(true), Value::Bool(false)]))));
    assert_eq!(struct_val(br"_Constructor{true, false}"), 
               Ok((&br""[..], Value::Struct("_Constructor".to_string(),
                                            vec![Value::Bool(true), Value::Bool(false)]))));
    assert_eq!(struct_val(br###"Constructor1 { true, C{false, 25, "foo\nbar"} }"###), 
               Ok((&br""[..], Value::Struct("Constructor1".to_string(),
                                            vec![Value::Bool(true), 
                                                 Value::Struct("C".to_string(),
                                                               vec![Value::Bool(false),
                                                                    Value::Int(25_i32.to_bigint().unwrap()),
                                                                    Value::String("foo\nbar".to_string())])]))));
}

named!(int_val<&[u8], Value>,
    alt!(map!(hex_val, Value::Int) |
         map!(dec_val, Value::Int) | 
         do_parse!(apply!(sym,"-") >> 
                   val: alt!(hex_val | dec_val) >>
                   (Value::Int(-val))
                  )
        )
);

named!(hex_val<&[u8], BigInt>,
    do_parse!(tag_no_case!("0x") >> 
              bs: take_while1!(is_hex_digit) >> 
              spaces >>
              (BigInt::parse_bytes(bs, 16).unwrap()))
);

named!(dec_val<&[u8], BigInt>,
    do_parse!(bs: take_while1!(is_digit) >> 
              spaces >>
              (BigInt::parse_bytes(bs, 10).unwrap()))
);

#[test]
fn test_int() {
    assert_eq!(dec_val(br"1 ")     , Ok((&br""[..], 1_i32.to_bigint().unwrap())));
    assert_eq!(int_val(br"1 ")     , Ok((&br""[..], Value::Int(1_i32.to_bigint().unwrap()))));
    assert_eq!(value(br"-5 ")      , Ok((&br""[..], Value::Int(-5_i32.to_bigint().unwrap()))));
    assert_eq!(hex_val(br"0xabcd "), Ok((&br""[..], 0xabcd.to_bigint().unwrap())));
    assert_eq!(int_val(br"0xabcd "), Ok((&br""[..], Value::Int(0xabcd.to_bigint().unwrap()))));
    assert_eq!(value(br"0xabcd ")  , Ok((&br""[..], Value::Int(0xabcd.to_bigint().unwrap()))));
}

named_args!(constructor_args(constructor: String)<Value>,
    do_parse!(args: separated_list!(apply!(sym,","), value) >>
              (Value::Struct(constructor, args)))
);
