extern crate serde_json;

use ddd_ddlog::api::*;
use ddd_ddlog::*;
use differential_datalog::record;
use std::ffi::CString;
use std::net::SocketAddr;

use tokio::prelude::*;
use tokio::net::TcpStream;
use tokio::io;

fn main() {
    let prog = ddlog_run(1, false, None, 0, None);

    unsafe {
        let b = record::ddlog_bool(true);
        let c_left = CString::new("lr.left.CLeft").unwrap();

        // todo assuming the constructor for the record is the same as table name
        let rec = record::ddlog_struct(c_left.as_ptr(), [b].as_ptr(), 1);
        let table_id = ddlog_get_table_id(c_left.as_ptr());
        let updates = &[record::ddlog_insert_cmd(table_id, rec)];

        ddlog_transaction_start(prog);
        ddlog_apply_updates(prog, updates.as_ptr(), 1);
        ddlog_transaction_commit_dump_changes(prog, Some(transmit), 0);

        ddlog_stop(prog);
    }
}

pub extern "C" fn transmit(_: libc::uintptr_t,
                           table: libc::size_t,
                           rec: *const record::Record,
                           _polarity: bool) {
    unsafe {

        let addr = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
        let stream = TcpStream::connect(&addr);

        let data = (&*rec, table);
        let s = serde_json::to_string(&data).unwrap();

        let client = stream.and_then(|stream| {
            // stream.write(s.as_bytes());
            // stream.write("dawg".as_bytes());
            io::write_all(stream, "hello").then( |result| {
                Ok(())
            })
        }).map_err(|err| {
            println!("connection error = {:?}", err);
        });


        tokio::run(client);
    }
}
