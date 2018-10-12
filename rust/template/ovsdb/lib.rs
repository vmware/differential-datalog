//! Parse OVSDB database update messages and convert them into DDlog table update commands

extern crate differential_datalog;
extern crate serde_json;
extern crate num;

use differential_datalog::record::*;
use serde_json::Value;
use serde_json::map::Map;
use num::BigInt;

/*
pub enum UpdCmd {
    Insert (String, Record),
    Delete (String, Record),
    DeleteKey(String, Record)
}
*/

fn val_into_vec(v: Value) -> Vec<Value> {
    match v {
        Value::Array(a) => a,
        _ => panic!("not an array value")
    }
}

fn complain<X>(src: &Value, category: &str) -> Result<X, String>{
    Err(format!("JSON value {} is not a valid {}", src, category))
}

fn lookup<'a>(m: &'a mut Map<String, Value>, field: &str) -> Result<Value, String> {
    m.remove(field).ok_or(format!("JSON map does not contain \"{}\" field: {:?}", field, m))
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
pub fn cmd_from_val(src: Value) -> Result<UpdCmd, String> {
    match src {
        Value::Object(mut m) => {
            let op: String = serde_json::from_value(lookup(&mut m, "op")?.clone()).map_err(|e|e.to_string())?;
            let table: String = serde_json::from_value(lookup(&mut m,"table")?.clone()).map_err(|e|e.to_string())?;
            match op.as_ref() {
                "update" => { 
                    let rec = row_from_val(m, &table)?;
                    Ok(UpdCmd::Insert(table, rec))
                },
                _ => Err(format!("unsupported operation {} in {:?}", op, m))
            }
        },
        _ => complain(&src, "command")
    }
}

fn row_from_val(mut m: Map<String, Value>, table: &str) -> Result<Record, String> {
    // extract _uuid from "where"-condition of the form
    // "where": [[ "_uuid", "==", ["uuid", "cf9dca71-2495-4e61-9fe2-8ea4fb2859cd"]]]
    let uuid = match lookup(&mut m, "where")? {
        Value::Array(mut v) => {
            let valid_pattern = match v.as_slice() {
                [Value::String(field),Value::String(op), Value::Array(_)] => {
                    field == "_uuid" && op == "=="
                },
                _ => false
            };
            if valid_pattern {
                record_from_array(val_into_vec(v.remove(2)))
            } else {
                Err(format!("unsupported \"where\" condition: {:?}", v))
            }
        },
        _ => Err(format!("\"where\" condition is not an array in {:?}", m))
    }?;

    // parse row
    let mut fields = match lookup(&mut m, "row")? {
        Value::Object(obj) => row_from_obj(obj),
        _ => Err(format!("\"row\" is not an object in {:?}", m))
    }?;
    fields.push(("_uuid".to_owned(), uuid));
    Ok(Record::NamedStruct(table.to_string(), fields))
}

/*
    ["uuid", "cf9dca71-2495-4e61-9fe2-8ea4fb2859cd"]
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
                    Ok(Record::Int(BigInt::parse_bytes(uuid.as_bytes(), 16)
                                   .ok_or(format!("invalid uuid string \"{}\"", uuid))?))
                },
                ("named-uuid", Value::String(uuid_name)) => {
                    Ok(Record::String(uuid_name))
                },
                ("set", Value::Array(atoms)) => {
                    let elems: Result<Vec<Record>, String> = atoms.into_iter().map(|a|record_from_val(a)).collect();
                    Ok(Record::Array(elems?))
                },
                ("map", Value::Array(pairs)) => {
                    let elems: Result<Vec<(Record, Record)>, String> = pairs.into_iter().map(|p|pair_from_val(p)).collect();
                    Ok(Record::Array(elems?.into_iter().map(|(k,v)|Record::Tuple(vec![k,v])).collect()))
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
                return Err(format!("key-value pair must be of length 2: {:?}",v))
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
fn row_from_obj(m: Map<String, Value>) -> Result<Vec<(String, Record)>, String> {
    m.into_iter().map(|(field, val)|Ok((field, record_from_val(val)?))).collect()
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
