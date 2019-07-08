extern crate serde_json;

use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;
use differential_datalog::record::*;
use std::net::SocketAddr;

use tokio::io;
use tokio::net::TcpStream;
use tokio::prelude::*;

fn main() -> Result<(), String> {
    let prog = HDDlog::run(1, false, None::<fn(usize, &Record, bool)>);

    let rec = Record::Bool(true);

    // let table_id = HDDlog::get_table_id("lr.left.CLeft").unwrap();
    let updates = &[UpdCmd::Insert(RelIdentifier::RelId(lr_left_CLeft as usize), rec)];

    prog.transaction_start()?;
    prog.apply_updates(updates.into_iter())?;
    prog.transaction_commit_dump_changes(Some(transmit))?;
    prog.stop()?;

    Ok(())
}

fn transmit(table: libc::size_t, rec: &Record, _polarity: bool) {
    let addr = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
    let stream = TcpStream::connect(&addr);

    let data = (&*rec, table);
    let s = serde_json::to_string(&data).unwrap();

    let client = stream
        .and_then(|stream| io::write_all(stream, s).then(|_result| Ok(())))
        .map_err(|err| {
            println!("connection error = {:?}", err);
        });

    tokio::run(client);
}
