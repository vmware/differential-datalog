extern crate differential_datalog;
extern crate serde_json;
extern crate num;

use differential_datalog::record::*;
use serde_json::Value;
use serde_json::map::Map;
use num::BigInt;

/*
pub enum Record {
    Bool(bool),
    Int(BigInt),
    String(String),
    Tuple(Vec<Record>),
    Array(Vec<Record>),
    PosStruct(String, Vec<Record>),
    NamedStruct(String, Vec<(String, Record)>)
}

pub enum UpdCmd {
    Insert (String, Record),
    Delete (String, Record),
    DeleteKey(String, Record)
}
*/

fn complain<X>(src: &Value, category: &str) -> Result<X, String>{
    Err(format!("JSON value {} is not a valid {}", src, category))
}

fn lookup<'a>(m: &'a Map<String, Value>, field: &str) -> Result<&'a Value, String> {
    m.get(field).ok_or(format!("JSON map does not contain \"{}\" field: {:?}", field, m))
}

/*
 {
    "op": "update",
    "row": {
      "bridges": [
        "named-uuid",
        "row9b73f7bc_2a67_48df_88b0_fabc19686920"]},
    "table": "Open_vSwitch",
    "where": [
      [
        "_uuid",
        "==",
        [
          "uuid",
          "cf9dca71-2495-4e61-9fe2-8ea4fb2859cd"]]]
 }
*/
pub fn cmd_from_val(src: &Value) -> Result<UpdCmd, String> {
    match src {
        Value::Object(m) => {
            let op: String = serde_json::from_value(lookup(m, "op")?.clone()).map_err(|e|e.to_string())?;
            let table: String = serde_json::from_value(lookup(m,"table")?.clone()).map_err(|e|e.to_string())?;
            match op.as_ref() {
                "update" => { 
                    let rec = row_from_val(m, &table)?;
                    Ok(UpdCmd::Insert(table, rec))
                },
                _ => Err(format!("unsupported operation {} in {}", op, &src))
            }
        },
        _ => complain(&src, "command")
    }
}

fn row_from_val(m: &Map<String, Value>, table: &str) -> Result<Record, String> {
    // extract _uuid from "where"-condition of the form
    // "where": [[ "_uuid", "==", ["uuid", "cf9dca71-2495-4e61-9fe2-8ea4fb2859cd"]]]
    let uuid = match lookup(m, "where")? {
        Value::Array(v) => {
            match v.as_slice() {
                [Value::String(ref field),Value::String(ref op), ref uuid] if field == "_uuid" && op == "==" => {
                    uuid_from_val(uuid)
                },
                _ => Err(format!("unsupported \"where\" condition: {:?}", v))
            }
        },
        _ => Err(format!("\"where\" condition is not an array in {:?}", m))
    }?;

    // parse row
    let mut fields = match lookup(m, "row")? {
        Value::Object(obj) => row_from_obj(obj),
        _ => Err(format!("\"row\" is not an object in {:?}", m))
    }?;
    fields.push(("_uuid".to_owned(), uuid));
    Ok(Record::NamedStruct(table.to_string(), fields))
}

/*
    ["uuid", "cf9dca71-2495-4e61-9fe2-8ea4fb2859cd"]
 */
fn uuid_from_val(src: &Value) -> Result<Record,String> {
    match src {
        Value::Array(v) => {
            match v.as_slice() {
                [Value::String(ref field), Value::String(ref uuid)] if field == "uuid" => {
                    Ok(Record::Int(BigInt::parse_bytes(uuid.as_bytes(), 16).
                                   ok_or(format!("invalid uuid string \"{}\"", uuid))?))
                },
                _ => Err(format!("invalid uuid format: {:?}", v))
            }
        },
        _ => Err(format!("invalid uuid {}", src))
    }
}

/*
  {
    "interfaces": [ "named-uuid", "rowe82c26a4_bbf4_4e84_87fd_2107d5998a12"],
    "name": "br0"
  }
*/
fn row_from_obj(m: &Map<String, Value>) -> Result<Vec<(String, Record)>, String> {
    m.iter().map(|(field, val)|Ok((field.clone(), record_from_val(val)?))).collect()
}

fn record_from_val(v: &Value) -> Result<Record, String> {
    match v {
        // <string>
        Value::String(s) => Ok(Record::String(s.clone())),
        // <number>
        Value::Number(n) if n.is_u64() => Ok(Record::Int(BigInt::from(n.as_u64().unwrap()))),
        // <boolean>
        Value::Bool(b) => Ok(Record::Bool(*b)),
        // <uuid>
        // <named-uuid>
        // <set>
        // <map>
        _ => Err(format!("unexpected value {}", v))
    }
    //Err("not implemented".to_owned())
}
