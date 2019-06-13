//! nom-based parser for Datalog values.

use num::bigint::*;
#[cfg(test)]
use num::Num;
use nom::*;
use differential_datalog::record::*;
use std::borrow::Cow;

#[derive(Debug,PartialEq,Eq,Clone)]
pub enum ProfileCmd {
    CPU(bool)
}

#[derive(Debug,PartialEq,Eq,Clone)]
pub enum Command {
    Start,
    Commit(bool),
    Comment,
    Rollback,
    Timestamp,
    Profile(Option<ProfileCmd>),
    Dump(Option<String>),
    Clear(String),
    Exit,
    Echo(String),
    Update(UpdCmd, bool)
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
              bytes: fold_many0!(alt!(alphanumeric1 | tag!("_") | tag!(".")),
                                 "".to_string(),
                                 |acc, bs: &[u8]| acc + (&*String::from_utf8(bs.to_vec()).unwrap())) >>
              spaces >>
              (String::from_utf8(first.to_vec()).unwrap() + &bytes))
);


named!(pub profile_cmd<&[u8], ProfileCmd>,
       do_parse!(apply!(sym,"cpu") >>
                 enable: alt!(do_parse!(apply!(sym,"on") >> (true)) |
                              do_parse!(apply!(sym,"off") >> (false))) >>
                 (ProfileCmd::CPU(enable)))
);

named!(pub parse_command<&[u8], Command>,
    do_parse!(
        spaces >>
        upd: alt!(do_parse!(apply!(sym,"start")     >> apply!(sym,";") >> (Command::Start))     |
                  do_parse!(apply!(sym,"commit")    >>
                            delta: opt!(apply!(sym, "dump_changes"))   >>
                            apply!(sym,";")         >>
                            (Command::Commit(delta.is_some())))                                 |
                  do_parse!(apply!(sym,"timestamp") >> apply!(sym,";") >> (Command::Timestamp)) |
                  do_parse!(apply!(sym,"#")         >> take_until!("\n") >> (Command::Comment)) |
                  do_parse!(apply!(sym,"profile")   >>
                            cmd: opt!(profile_cmd)  >>
                            apply!(sym,";")         >>
                            (Command::Profile(cmd)))                                            |
                  do_parse!(apply!(sym,"dump")      >>
                            rel: opt!(identifier)   >>
                            apply!(sym,";")         >>
                            (Command::Dump(rel)))                                               |
                  do_parse!(apply!(sym,"clear")     >>
                            rel: identifier         >>
                            apply!(sym,";")         >>
                            (Command::Clear(rel)))                                              |
                  do_parse!(apply!(sym,"exit")      >> apply!(sym,";") >> (Command::Exit))      |
                  do_parse!(apply!(sym,"echo")      >>
                            txt: take_until!(";")   >>
                            apply!(sym,";")         >>
                            (Command::Echo(String::from_utf8(txt.to_vec()).unwrap())))          |
                  do_parse!(apply!(sym,"rollback") >> apply!(sym,";") >> (Command::Rollback))   |
                  do_parse!(upd:  update >>
                            last: alt!(map!(apply!(sym,";"), |_|true) | map!(apply!(sym, ","), |_|false)) >>
                            (Command::Update(upd, last)))) >>
        (upd)
    )
);

#[test]
fn test_command() {
    assert_eq!(parse_command(br"start;")    , Ok((&br""[..], Command::Start)));
    assert_eq!(parse_command(br"# comment") , Ok((&br""[..], Command::Comment)));
    assert_eq!(parse_command(br"commit;")   , Ok((&br""[..], Command::Commit)));
    assert_eq!(parse_command(br"timestamp;"), Ok((&br""[..], Command::Timestamp)));
    assert_eq!(parse_command(br"profile cpu on;") , Ok((&br""[..], Command::Profile(Some(ProfileCmd::CPU(true))))));
    assert_eq!(parse_command(br"profile cpu off;"), Ok((&br""[..], Command::Profile(Some(ProfileCmd::CPU(false))))));
    assert_eq!(parse_command(br"profile;")  , Ok((&br""[..], Command::Profile(None))));
    assert_eq!(parse_command(br"dump;")     , Ok((&br""[..], Command::Dump(None))));
    assert_eq!(parse_command(br"dump Tab;") , Ok((&br""[..], Command::Dump(Some("Tab".to_string())))));
    assert_eq!(parse_command(br"clear Tab;"), Ok((&br""[..], Command::Clear("Tab".to_string()))));
    assert_eq!(parse_command(br"exit;")     , Ok((&br""[..], Command::Exit)));
    assert_eq!(parse_command(br"echo test;"), Ok((&br""[..], Command::Echo("test".to_string()))));
    assert_eq!(parse_command(br"echo;")     , Ok((&br""[..], Command::Echo("".to_string()))));
    assert_eq!(parse_command(br"rollback;") , Ok((&br""[..], Command::Rollback)));
    assert_eq!(parse_command(br"insert Rel1(true);"),
               Ok((&br""[..], Command::Update(
                   UpdCmd::Insert(RelIdentifier::RelName(Cow::from("Rel1")), Record::PosStruct(Cow::from("Rel1"), vec![Record::Bool(true)])),
                   true
               ))));
    assert_eq!(parse_command(br" insert Rel1[true];"),
               Ok((&br""[..], Command::Update(
                   UpdCmd::Insert(RelIdentifier::RelName(Cow::from("Rel1")), Record::Bool(true)), true
               ))));
    assert_eq!(parse_command(br"delete Rel1[(true,false)];"),
               Ok((&br""[..], Command::Update(
                   UpdCmd::Delete(RelIdentifier::RelName(Cow::from("Rel1")), Record::Tuple(vec![Record::Bool(true), Record::Bool(false)])), true
               ))));
    assert_eq!(parse_command(br"delete_key Rel1 true;"),
               Ok((&br""[..], Command::Update(
                   UpdCmd::DeleteKey(RelIdentifier::RelName(Cow::from("Rel1")), Record::Bool(true)), true
               ))));
    assert_eq!(parse_command(br"modify Rel1 true <- Rel1{.f1 = 5};"),
               Ok((&br""[..], Command::Update(
                   UpdCmd::Modify(RelIdentifier::RelName(Cow::from("Rel1")), Record::Bool(true),
                                  Record::NamedStruct(Cow::from("Rel1"),
                                  vec![(Cow::from("f1"), Record::Int(5.to_bigint().unwrap()))])), true
               ))));
    assert_eq!(parse_command(br#"   delete NB.Logical_Router("foo", 0xabcdef1, true) , "#),
               Ok((&br""[..], Command::Update(
                   UpdCmd::Delete(RelIdentifier::RelName(Cow::from("NB.Logical_Router")),
                                  Record::PosStruct(Cow::from("NB.Logical_Router"),
                                                    vec![Record::String("foo".to_string()),
                                                         Record::Int(0xabcdef1.to_bigint().unwrap()),
                                                         Record::Bool(true)])),
                   false
               ))));
}

named!(update<&[u8], UpdCmd>,
    alt!(do_parse!(apply!(sym,"insert")     >> rec: rel_record >> (UpdCmd::Insert(RelIdentifier::RelName(rec.0), rec.1)))    |
         do_parse!(apply!(sym,"delete")     >> rec: rel_record >> (UpdCmd::Delete(RelIdentifier::RelName(rec.0), rec.1)))    |
         do_parse!(apply!(sym,"delete_key") >> rec: rel_key    >> (UpdCmd::DeleteKey(RelIdentifier::RelName(rec.0), rec.1))) |
         do_parse!(apply!(sym,"modify") >>
                   rec: rel_key         >>
                   apply!(sym, "<-")    >>
                   mutator: record      >>
                   (UpdCmd::Modify(RelIdentifier::RelName(rec.0), rec.1, mutator)))
         )
);

named!(rel_record<&[u8], (Name, Record)>,
    do_parse!(cons: identifier >>
              val: alt!(delimited!(apply!(sym,"["), record, apply!(sym,"]")) |
                        delimited!(apply!(sym,"("), apply!(constructor_args, Cow::from(cons.clone())), apply!(sym,")"))) >>
              (Cow::from(cons), val))
);

named!(rel_key<&[u8], (Name, Record)>,
    do_parse!(rel: identifier >>
              val: record     >>
              (Cow::from(rel), val))
);

named!(record<&[u8], Record>,
    alt!(bool_val | string_val | tuple_val | array_val | struct_val | int_val )
);

named!(named_record<&[u8], (Name, Record)>,
    do_parse!(apply!(sym,".") >>
              fname: identifier >>
              apply!(sym,"=") >>
              val: record >>
              (Cow::from(fname), val))
);

named!(bool_val<&[u8], Record>,
    alt!(do_parse!(apply!(sym,"true")  >> (Record::Bool(true))) |
         do_parse!(apply!(sym,"false") >> (Record::Bool(false))))
);

#[test]
fn test_bool() {
    assert_eq!(bool_val(br"true "), Ok((&br""[..], Record::Bool(true))));
    assert_eq!(bool_val(br"false"), Ok((&br""[..], Record::Bool(false))));
    assert_eq!(record(br"false ") , Ok((&br""[..], Record::Bool(false))));
}

named!(string_val<&[u8], Record>,
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
        (Record::String(String::from_utf8(str).unwrap()))
    )
);

#[test]
fn test_string() {
    assert_eq!(string_val(br###""foo""###), Ok((&br""[..], Record::String("foo".to_string()))));
    assert_eq!(string_val(br###""" "###), Ok((&br""[..], Record::String("".to_string()))));
    assert_eq!(string_val(br###""foo\nbar" "###), Ok((&br""[..], Record::String("foo\nbar".to_string()))));
    assert_eq!(string_val(br###""foo\tbar\t\a" "###), Ok((&br""[..], Record::String("foo\tbar\t\x07".to_string()))));
}

named!(tuple_val<&[u8], Record>,
    delimited!(apply!(sym,"("),
               map!(separated_list!(apply!(sym,","), record), |v|Record::Tuple(v)),
               apply!(sym,")"))
);

#[test]
fn test_tuple() {
    assert_eq!(tuple_val(br"( true, false)"), Ok((&br""[..], Record::Tuple(vec![Record::Bool(true), Record::Bool(false)]))));
}

named!(array_val<&[u8], Record>,
    delimited!(apply!(sym,"["),
               map!(separated_list!(apply!(sym,","), record), |v|Record::Array(CollectionKind::Unknown, v)),
               apply!(sym,"]"))
);

#[test]
fn test_array() {
    assert_eq!(array_val(br"[ (true, false), (false, false)]"), Ok((&br""[..], Record::Array(CollectionKind::Unknown,
                                                                                             vec![Record::Tuple(vec![Record::Bool(true), Record::Bool(false)]),
                                                                                                  Record::Tuple(vec![Record::Bool(false), Record::Bool(false)])]))));
}

named!(struct_val<&[u8], Record>,
    do_parse!(
        cons: identifier >>
        val: opt!(delimited!(apply!(sym,"{"), apply!(constructor_args, Cow::from(cons.clone())), apply!(sym,"}"))) >>
        (match val {
            None    => Record::PosStruct(Cow::from(cons), vec![]),
            Some(r) => r
         }))
);

#[test]
fn test_struct() {
    assert_eq!(struct_val(br"Constructor { true, false }"),
               Ok((&br""[..], Record::PosStruct(Cow::from("Constructor"),
                                            vec![Record::Bool(true), Record::Bool(false)]))));
    assert_eq!(struct_val(br"Constructor { .f1 = true, .f2 = false }"),
               Ok((&br""[..], Record::NamedStruct(Cow::from("Constructor"),
                                            vec![(Cow::from("f1"), Record::Bool(true)), (Cow::from("f2"), Record::Bool(false))]))));
    assert_eq!(struct_val(br"_Constructor{true, false}"),
               Ok((&br""[..], Record::PosStruct(Cow::from("_Constructor"),
                                            vec![Record::Bool(true), Record::Bool(false)]))));
    assert_eq!(struct_val(br"_Constructor{.f1 = true, .f2=false}"),
               Ok((&br""[..], Record::NamedStruct(Cow::from("_Constructor"),
                                            vec![(Cow::from("f1"), Record::Bool(true)), (Cow::from("f2"),Record::Bool(false))]))));
    assert_eq!(struct_val(br###"Constructor1 { true, C{Constructor3, 25, "foo\nbar"} }"###),
               Ok((&br""[..], Record::PosStruct(Cow::from("Constructor1"),
                                            vec![Record::Bool(true),
                                                 Record::PosStruct(Cow::from("C"),
                                                               vec![Record::PosStruct(Cow::from("Constructor3"), vec![]),
                                                                    Record::Int(25_i32.to_bigint().unwrap()),
                                                                    Record::String("foo\nbar".to_string())])]))));
    assert_eq!(struct_val(br###"Constructor1 { .bfield = true, .cons = C{.cfield = Constructor3, .ifield = 25, .sfield="foo\nbar"} }"###),
               Ok((&br""[..], Record::NamedStruct(Cow::from("Constructor1"),
                                            vec![(Cow::from("bfield"),Record::Bool(true)),
                                                 (Cow::from("cons"), Record::NamedStruct(Cow::from("C"),
                                                               vec![(Cow::from("cfield"), Record::PosStruct(Cow::from("Constructor3"), vec![])),
                                                                    (Cow::from("ifield"), Record::Int(25_i32.to_bigint().unwrap())),
                                                                    (Cow::from("sfield"), Record::String("foo\nbar".to_string()))]))]))));
}

named!(int_val<&[u8], Record>,
    alt!(map!(hex_val, Record::Int) |
         map!(dec_val, Record::Int) |
         do_parse!(apply!(sym,"-") >>
                   val: alt!(hex_val | dec_val) >>
                   (Record::Int(-val))
                  )
        )
);

named!(hex_val<&[u8], BigInt>,
    do_parse!(tag_no_case!("0x") >>
              bs1: take_while1!(|x|is_hex_digit(x)) >>
              bs2: take_while!(|x|is_hex_digit(x) || x == '_' as u8) >>
              spaces >>
              ({let mut bs = bs1.to_vec();
                bs.extend_from_slice(bs2);
                BigInt::parse_bytes(bs.as_slice(), 16).unwrap()}))
);

named!(dec_val<&[u8], BigInt>,
    do_parse!(bs1: take_while1!(|x| is_digit(x)) >>
              bs2: take_while!(|x| is_digit(x) || x == '_' as u8) >>
              spaces >>
              ({let mut bs = bs1.to_vec();
                bs.extend_from_slice(bs2);
                BigInt::parse_bytes(bs.as_slice(), 10).unwrap()}))
);

#[test]
fn test_int() {
    assert_eq!(dec_val(br"1 ")     , Ok((&br""[..], 1_i32.to_bigint().unwrap())));
    assert_eq!(int_val(br"1 ")     , Ok((&br""[..], Record::Int(1_i32.to_bigint().unwrap()))));
    assert_eq!(record(br"-5 ")     , Ok((&br""[..], Record::Int(-5_i32.to_bigint().unwrap()))));
    assert_eq!(hex_val(br"0xabcd "), Ok((&br""[..], 0xabcd.to_bigint().unwrap())));
    assert_eq!(int_val(br"0xabcd "), Ok((&br""[..], Record::Int(0xabcd.to_bigint().unwrap()))));
    assert_eq!(record(br"0xabcd ") , Ok((&br""[..], Record::Int(0xabcd.to_bigint().unwrap()))));
    assert_eq!(record(br"0xc3226515_018c_48e6_957d_afee358a8a10 ")
                                   , Ok((&br""[..], Record::Int(BigInt::from_str_radix("c3226515018c48e6957dafee358a8a10",16).unwrap()))));
    assert_eq!(record(br"1_000_000_ ")
                                   , Ok((&br""[..], Record::Int( 1000000.to_bigint().unwrap() ))));
}

named_args!(constructor_args(constructor: Name)<Record>,
    alt!(do_parse!(args: separated_nonempty_list!(apply!(sym,","), named_record) >>
                   (Record::NamedStruct(constructor.clone(), args)))
         |
        do_parse!(args: separated_list!(apply!(sym,","), record) >>
                   (Record::PosStruct(constructor, args)))
        )
);
