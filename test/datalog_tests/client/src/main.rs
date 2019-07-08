extern crate serde_json;

use ddd_ddlog::api::*;
use ddd_ddlog::*;
use differential_datalog::record;
use std::ffi::CString;
use std::net::SocketAddr;
use std::borrow::Cow;

use tokio::prelude::*;
use tokio::net::TcpStream;
use tokio::io;

fn main() -> Result<(), String>{
    let prog = HDDlog::run(1, false, None::<fn(usize, &record::Record, bool)>);

    let b = record::Record::Bool(true);

    // todo assuming the constructor for the record is the same as table name
    let rec = record::Record::PosStruct(
        Cow::from("lr.left.CLeft".to_owned()),
        [b].to_vec());

    let table_id = HDDlog::get_table_id("lr.left.CLeft").unwrap();
    let updates = &[record::UpdCmd::Insert(record::RelIdentifier::RelId(table_id as usize), rec)];

    prog.transaction_start()?;
    prog.apply_updates(updates.into_iter())?;
    prog.transaction_commit_dump_changes(Some(transmit))?;
    prog.stop()?;

    Ok(())
}

fn transmit(table: libc::size_t,
            rec: &record::Record,
            _polarity: bool) {

    let addr = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
    let stream = TcpStream::connect(&addr);

    let data = (&*rec, table);
    let s = serde_json::to_string(&data).unwrap();

    let client = stream.and_then(|stream| {
        io::write_all(stream, s).then( |_result| {
            Ok(())
        })
    }).map_err(|err| {
        println!("connection error = {:?}", err);
    });

    tokio::run(client);
}
