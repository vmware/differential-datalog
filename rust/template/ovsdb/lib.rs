//! Parse OVSDB database update messages and convert them into DDlog table update commands

extern crate differential_datalog;
extern crate serde_json;
extern crate num;
extern crate uuid;

use differential_datalog::record::*;
use serde_json::Value;
use serde_json::map::Map;
use serde_json::Number;
use num::{BigInt, Signed, ToPrimitive};
use std::borrow::Cow;

#[cfg(test)]
mod test;

/*
 * Functions to parse JSON into DDlog commands
 */

pub fn cmds_from_table_updates_str(prefix: &str, s: &str) -> Result<Vec<UpdCmd>, String> {
    if let Value::Object(json_val) = serde_json::from_str(s).map_err(|e|e.to_string())? {
        cmds_from_table_updates(prefix, json_val)
    } else {
        Err(format!("JSON value is not an object: {}", s))
    }
}

fn parse_uuid(s: &str) -> Result<BigInt, String> {
    let digits: Vec<u8> = s.as_bytes().iter().map(|x|*x).filter(|x|*x != b'-').collect();
    BigInt::parse_bytes(digits.as_slice(), 16)
        .ok_or_else(||format!("invalid uuid string \"{}\"", s))
}

/* <table-updates> is an object that maps from a table name to a <table-update>.
 *
 * `prefix` is the DB name prepended to all table names
 */
pub fn cmds_from_table_updates(prefix: &str, upds: Map<String,Value>) -> Result<Vec<UpdCmd>, String> {
    let mut commands = Vec::new();
    let oks: Result<Vec<()>, String> = upds.into_iter().map(|(table, updates)|cmds_from_table_update(prefix.to_string() + table.as_str(), updates, &mut commands)).collect();
    oks?;
    Ok(commands)
}

/* A <table-update> is an object that maps from the row's UUID to a <row-update> object.
 */
fn cmds_from_table_update(table: String, updates: Value, cmds: &mut Vec<UpdCmd>) -> Result<(), String> {
    match updates {
        Value::Object(upds) => {
            upds.into_iter().map(|(uuid, u)| cmd_from_row_update(table.as_str(), uuid, u, cmds)).collect()
        },
        _ => Err(format!("table update is not an object: {}", updates))
    }
}

/* Convert `<row-update>` JSON object to one or more `UpdCmd`'s.  The object can have one of two
 * formats. The old ("update-1") format has the following members:
 *
 * `"old": <row>`   present for "delete" and "modify" updates
 * `"new": <row>`   present for "initial", "insert", and "modify" updates
 *
 * The new ("update-2") has one of the following members:
 * `"initial": <row>`
 * `"insert": <row>`
 * `"delete": <row>`
 * `"modify": <row>`
 */
fn cmd_from_row_update(table: &str, uuid: String, update: Value, cmds: &mut Vec<UpdCmd>) -> Result<(), String> {
    let uuid = Record::Int(parse_uuid(uuid.as_ref())?);
    match update {
        Value::Object(mut m) => {
            /* Handle update-2 format */
            match m.remove("insert").or_else(||m.remove("initial")) {
                Some(v) => {
                    let mut insert = row_from_obj(v)?;
                    insert.push((Cow::from("_uuid"), uuid));
                    cmds.push(UpdCmd::Insert(RelIdentifier::RelName(Cow::from(table.to_owned())),
                    Record::NamedStruct(Cow::from(table.to_owned()), insert)));
                    return Ok(());
                },
                None => ()
            };

            match m.remove("modify") {
                Some(v) => {
                    let mut insert = row_from_obj(v)?;
                    cmds.push(UpdCmd::Modify(RelIdentifier::RelName(Cow::from(table.to_owned())),
                                             uuid,
                                             Record::NamedStruct(Cow::from(table.to_owned()), insert)));
                    return Ok(());
                },
                None => ()
            };

            if m.contains_key("delete") {
                cmds.push(UpdCmd::DeleteKey(RelIdentifier::RelName(Cow::from(table.to_owned())), uuid.clone()));
                return Ok(());
            };

            /* None of update-2 fields found; try update-1 format */
            let old = m.contains_key("old");
            let new = m.remove("new").map(|v|row_from_obj(v));
            if new.is_none() && !old {
                return Err(format!("row is not in <row-update> format: {}", Value::Object(m)));
            };
            if old {
                // delete_key
                cmds.push(UpdCmd::DeleteKey(RelIdentifier::RelName(Cow::from(table.to_owned())), uuid.clone()))
            };
            if new.is_some() {
                let mut fields = new.unwrap()?;
                fields.push((Cow::from("_uuid"), uuid));
                // insert
                cmds.push(UpdCmd::Insert(RelIdentifier::RelName(Cow::from(table.to_owned())),
                                         Record::NamedStruct(Cow::from(table.to_owned()), fields)))
            };
            Ok(())
        },
        _ => Err(format!("row update is not an object: {}", update))
    }
}

/* ["uuid", "cf9dca71-2495-4e61-9fe2-8ea4fb2859cd"]
 */
fn record_from_array(mut src: Vec<Value>) -> Result<Record,String> {
    if src.len() != 2 {
        return Err(format!("record array is not of length 2: {:?}", src));
    };
    let val = src.remove(1);
    let field = src.remove(0);
    match field {
        Value::String(field) => {
            match (field.as_str(), val) {
                ("uuid", Value::String(uuid)) => {
                    Ok(Record::Int(parse_uuid(uuid.as_ref())?))
                },
                ("named-uuid", Value::String(uuid_name)) => {
                    Ok(Record::String(uuid_name))
                },
                ("set", Value::Array(atoms)) => {
                    let elems: Result<Vec<Record>, String> = atoms.into_iter().map(|a|record_from_val(a)).collect();
                    Ok(Record::Array(CollectionKind::Set, elems?))
                },
                ("map", Value::Array(pairs)) => {
                    let elems: Result<Vec<(Record, Record)>, String> = pairs.into_iter().map(|p|pair_from_val(p)).collect();
                    Ok(Record::Array(CollectionKind::Map, elems?.into_iter().map(|(k,v)|Record::Tuple(vec![k,v])).collect()))
                },
                _ => Err(format!("unexpected array value type \"{}\"", field))
            }
        },
        _ => Err(format!("invalid array value: {:?}", src))
    }
}

fn pair_from_val(pair: Value) -> Result<(Record, Record), String> {
    match pair {
        Value::Array(mut v) => {
            if v.len() != 2 {
                return Err(format!("key-value pair must be of length 2: {:?}", v))
            };
            let val = v.remove(1);
            let key = v.remove(0);
            Ok((record_from_val(key)?, record_from_val(val)?))
        },
        _ => Err(format!("invalid key-value pair {}", pair))
    }
}

/*
  {
    "interfaces": [ "named-uuid", "rowe82c26a4_bbf4_4e84_87fd_2107d5998a12"],
    "name": "br0"
  }
*/
fn row_from_obj(val: Value) -> Result<Vec<(Name, Record)>, String> {
    match val{
        Value::Object(m) => {
            m.into_iter().map(|(field, val)|Ok((Cow::from(field), record_from_val(val)?))).collect()
        },
        _ => Err(format!("\"row\" is not an object in {}", val))
    }
}

fn record_from_val(v: Value) -> Result<Record, String> {
    match v {
        // <string>
        Value::String(s) => Ok(Record::String(s)),
        // <number>
        Value::Number(ref n) if n.is_u64() => Ok(Record::Int(BigInt::from(n.as_u64().unwrap()))),
        // <boolean>
        Value::Bool(b) => Ok(Record::Bool(b)),
        // <uuid>, <named-uuid>, <set>, <map>
        Value::Array(v) => record_from_array(v),
        _ => Err(format!("unexpected value {}", v))
    }
}

/*
 * Functions to convert DDlog Records to JSON
 */

pub fn record_into_insert_str(rec: Record, table: &str) -> Result<String, String> {
    let opt_uuid_name = match rec {
        Record::NamedStruct(_, ref fields) => {
            fields.iter().find(|(f,_)| f == "uuid_name").map(|(_,val)|val.clone())
        },
        _ => Err(format!("Cannot convert record to insert command: {:?}", rec))?
    };
    let mut m = Map::new();
    m.insert("op".to_owned(), Value::String("insert".to_owned()));
    m.insert("table".to_owned(), Value::String(table.to_owned()));
    match opt_uuid_name {
        None => {},
        Some(Record::String(n)) => { m.insert("uuid-name".to_owned(), Value::String(n)); },
        Some(x) => Err(format!("uuid-name is not a string {:?}", x))?
    };
    m.insert("row".to_owned(), record_into_row(rec)?);
    Ok(Value::Object(m).to_string())
}

pub fn record_into_delete_str(rec: Record, table: &str) -> Result<String, String> {
    let uuid = match rec {
        Record::NamedStruct(_, fields) => {
            fields.into_iter().find(|(f,_)| f == "_uuid")
                  .ok_or_else(||format!("Record does not have _uuid field"))?.1
        },
        _ => Err(format!("Cannot convert record to delete command: {:?}", rec))?
    };
    let mut m = Map::new();
    m.insert("op".to_owned(), Value::String("delete".to_owned()));
    m.insert("table".to_owned(), Value::String(table.to_owned()));
    m.insert("where".to_owned(), uuid_condition(&uuid)?);
    Ok(Value::Object(m).to_string())
}

pub fn record_into_update_str(rec: Record, table: &str) -> Result<String, String> {
    let uuid = match rec {
        Record::NamedStruct(_, ref fields) => {
            fields.iter().find(|(f,_)| f == "_uuid")
                  .ok_or_else(||format!("Record does not have _uuid field"))?.1.clone()
        },
        _ => Err(format!("Cannot convert record to insert command: {:?}", rec))?
    };
    let mut m = Map::new();
    m.insert("op".to_owned(), Value::String("update".to_owned()));
    m.insert("table".to_owned(), Value::String(table.to_owned()));
    m.insert("where".to_owned(), uuid_condition(&uuid)?);
    m.insert("row".to_owned(), record_into_row(rec)?);
    Ok(Value::Object(m).to_string())
}

fn record_into_row(rec: Record) -> Result<Value, String> {
    match rec {
        Record::NamedStruct(_, fields) => struct_into_obj(fields),
        _ => Err(format!("Cannot convert record to <row>: {:?}", rec)),
    }
}

fn uuid_condition(uuid: &Record) -> Result<Value, String> {
    match uuid {
        Record::Int(uuid) => {
            Ok(Value::Array(vec![
                         Value::Array(vec![Value::String("_uuid".to_owned()),
                                           Value::String("==".to_owned()),
                                           Value::Array(vec![Value::String("uuid".to_owned()),
                                                             Value::String(uuid_from_int(uuid)?)])])]))
        },
        x => Err(format!("invalid uuid value {:?}", x))
    }
}

fn struct_into_obj(fields: Vec<(Name, Record)>) -> Result<Value, String> {
    let fields: Result<Map<String, Value>, String> =
        fields.into_iter()
        .filter(|(f,_)| f != "uuid_name" && f != "_uuid")
        .map(|(f,v)| record_into_field(v).map(|fld|(field_name(f), fld))).collect();
    Ok(Value::Object(fields?))
}

fn field_name(name: Name) -> String {
    if name.as_ref().starts_with("__") {
        name.as_ref()[2..].to_owned()
    } else if name.as_ref() == "uuid_name" {
        "uuid-name".to_owned()
    } else {
        name.into_owned()
    }
}

fn record_into_field(rec: Record) -> Result<Value, String> {
    match rec {
        Record::Bool(b) => Ok(Value::Bool(b)),
        Record::String(s) => Ok(Value::String(s)),
        Record::Int(i) => {
            if i.is_positive() {
                match i.to_u64() {
                    Some(v) => Ok(Value::Number(Number::from(v))),
                    None => {
                        let uuid = uuid_from_int(&i)?;
                        Ok(Value::Array(vec![Value::String("uuid".to_owned()), Value::String(uuid)]))
                    }
                }
            } else {
                i.to_i64().ok_or_else(||format!("Cannot convert BigInt {} to i64", i)).map(|x|Value::Number(Number::from(x)))
            }
        },
        Record::NamedStruct(n, mut v) => {
            if n.as_ref() == "std_Left" {
                match v.remove(0) {
                    (_, Record::Int(i)) => {
                        let uuid = uuid_from_int(&i)?;
                        Ok(Value::Array(vec![Value::String("uuid".to_owned()), Value::String(uuid)]))
                    },
                    _ => Err(format!("Unexpected uuid value: {:?}", v))
                }
            } else if n.as_ref() == "std_Right" {
                match v.remove(0) {
                    (_, Record::String(s)) => {
                        Ok(Value::Array(vec![Value::String("named-uuid".to_owned()), Value::String(s)]))
                    },
                    _ => Err(format!("Unexpected named-uuid value: {:?}", v))
                }
            } else {
                Err(format!("Cannot convert complex field {} = {:?} to JSON value", n, v))
            }
        },
        Record::Array(CollectionKind::Set, v) => {
            let elems: Result<Vec<Value>, String> = v.into_iter().map(|x| record_into_field(x)).collect();
            Ok(Value::Array(vec![Value::String("set".to_owned()), Value::Array(elems?)]))
        },
        Record::Array(CollectionKind::Map, v) => {
            let elems: Result<Vec<Value>, String> = v.into_iter().map(|x| {
                match x {
                    Record::Tuple(mut keyval) => {
                        if keyval.len() != 2 {
                            Err(format!("Map entry {:?} is not a 2-tuple",keyval))?
                        };
                        let v = record_into_field(keyval.remove(1))?;
                        let k = record_into_field(keyval.remove(0))?;
                        Ok(Value::Array(vec![k,v]))
                    },
                    _ => Err(format!("Map entry {:?} is not a tuple", x))
                }
            }).collect();
            Ok(Value::Array(vec![Value::String("map".to_owned()), Value::Array(elems?)]))
        },
        _ => Err(format!("Cannot convert record field {:?} to JSON value", rec))
    }
}

fn uuid_from_u128(i: u128) -> String {
    uuid::Uuid::from_u128(i.to_be()).to_hyphenated().to_string()
}

fn uuid_from_int(i: &BigInt) -> Result<String, String> {
    Ok(uuid_from_u128(i.to_u128().ok_or_else(||format!("uuid {} is not a u128", i))?))
}

// exported to DDlog; hence the funny name
pub fn ovsdb_uuid2str(i: &u128) -> String {
    uuid::Uuid::from_u128(i.to_be()).to_hyphenated().to_string()
}

pub fn ovsdb_uuid2name(i: &u128) -> String {
    let s = uuid::Uuid::from_u128(i.to_be()).to_simple().to_string();
    format!("u{}_{}_{}_{}_{}", &s[0..8], &s[8..12], &s[12..16], &s[16..20], &s[20..32])
}
