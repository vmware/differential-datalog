extern crate serde_json;

use ddd_ddlog::api::*;
use ddd_ddlog::*;
use differential_datalog::record;
use std::ffi::CString;
use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::io;
use std::str;

fn handle_connection(mut stream: TcpStream) -> io::Result<()>{

    let prog = ddlog_run(1, false, None, 0, None);

    let mut s = String::new();
    stream.read_to_string(&mut s).and_then(|_| {println!("{:?}", s); Ok(())});

    // let (rec, table): (record::Record, usize)= serde_json::from_str(&s).unwrap();

    // unsafe {
    //     let f = record::ddlog_get_struct_field(&rec as *const record::Record, 0);
    //     let b_bool = record::ddlog_get_bool(f);
    //     let b = record::ddlog_bool(b_bool);

    //     let table_name = relid2name(table).unwrap();
    //     let constr = table_name.split('.').last().unwrap();
    //     let constr_r = String::from("lr.right.") + constr;

    //     let bin = CString::new(constr_r).unwrap();
    //     let rec = record::ddlog_struct(bin.as_ptr(), [b].as_ptr(), 1);

    //     let table_id = ddlog_get_table_id(bin.as_ptr());
    //     let updates = &[record::ddlog_insert_cmd(table_id, rec)];

    //     ddlog_transaction_start(prog);
    //     ddlog_apply_updates(prog, updates.as_ptr(), 1);
    //     ddlog_transaction_commit_dump_changes(prog, Some(show_out), 0);
    // }

    Ok(())
}

pub extern "C" fn show_out(arg: libc::uintptr_t,
                           table: libc::size_t,
                           rec: *const record::Record,
                           polarity: bool) {
    unsafe {
        println!("output is {:?}, {:?}, {:?}, {:?}", arg, table, *rec, polarity);
    }
}

fn main() {

    let addr = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
    let listener = TcpListener::bind(&addr).unwrap();

    let server = listener.incoming().for_each(|socket| {
        println!("got socket");

        let buf = Vec::new();

        let res = io::read_to_end(socket, buf);

        let work = res.then(|result| {
            match result {
                Ok((_socket, buf)) => println!("{:?}", String::from_utf8(buf).unwrap()),
                Err(e) => println!("{:?}", e),
            }

            Ok(())
        });

        tokio::spawn(work);
        Ok(())
    }).map_err(|err| {
        // Handle error by printing to STDOUT.
        println!("accept error = {:?}", err);
    });

    tokio::run(server);

    // accept connections and get a TcpStream
    // tokio::run(
    //     listener.incoming()
    //         .map_err(|e| eprintln!("failed to accept stream; error = {:?}", e))
    //         .for_each(|mut stream| {
    //             println!("got stuff");
    //             let mut buf = String::new();
    //             stream.read_to_string(&mut buf);
    //             println!("{:?}", buf);
    //             Ok(())
    //         }).map_err(|e| eprintln!("Error occured: {:?}", e))
    // );
}
