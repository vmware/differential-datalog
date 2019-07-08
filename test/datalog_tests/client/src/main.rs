extern crate serde_json;

use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;
use differential_datalog::record::*;
use std::net::SocketAddr;

use tokio::io;
use tokio::net::TcpStream;
use tokio::prelude::*;

fn main() -> Result<(), String> {
    let rec = Record::Bool(true);
    let table_id = RelIdentifier::RelId(lr_left_Left as usize);
    let updates = &[UpdCmd::Insert(table_id, rec)];

    let prog = HDDlog::run(1, false, None::<fn(usize, &Record, bool)>);
    prog.transaction_start()?;
    prog.apply_updates(updates.into_iter())?;
    prog.transaction_commit_dump_changes(Some(transmit))?;
    prog.stop()
}

fn transmit(table: libc::size_t, rec: &Record, _polarity: bool) {
    let addr_s = "127.0.0.1:8000";
    let addr = addr_s.parse::<SocketAddr>().unwrap();
    let stream = TcpStream::connect(&addr);

    let s = serde_json::to_string(&(&*rec, table)).unwrap();

    let client = stream
        .and_then(|stream| {
            io::write_all(stream, s).then(|_| Ok(()))
        })
        .map_err(|err| {
            println!("connection error = {:?}", err);
        });

    tokio::run(client);
}
