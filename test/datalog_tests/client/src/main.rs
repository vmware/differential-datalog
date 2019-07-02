extern crate serde_json;

use ddd_ddlog::api::*;
use ddd_ddlog::*;
use differential_datalog::record;
use std::ffi::CString;

use std::io::prelude::*;
use std::net::TcpStream;

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
        let mut stream = TcpStream::connect("127.0.0.1:8000").expect("connection failed");

        let data = (&*rec, table);

        let s = serde_json::to_string(&data).unwrap();

        stream.write(s.as_bytes());
    }
}
