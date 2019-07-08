extern crate serde_json;

use ddd_ddlog::api::*;
use ddd_ddlog::*;
use differential_datalog::record;
use std::ffi::CString;
use std::net::SocketAddr;
use std::borrow::Cow;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::io;
use std::str;

fn handle_connection(stream: TcpStream, prog: Arc<HDDlog>) -> impl Future<Item = (), Error = ()>
{
    let buf = Vec::new();
    let res = io::read_to_end(stream, buf);

    let work = res.then(move |result| {
        match result {
            Ok((_socket, buf)) => {
                let s = String::from_utf8(buf).unwrap();
                // println!("{:?}", &s);
                let (rec, table): (record::Record, usize)= serde_json::from_str(&s).unwrap();

                // let f = record::ddlog_get_struct_field(&rec as *const record::Record, 0);

                if let record::Record::NamedStruct(_, fields) = rec {
                    // let (_, b) = fields.get(0).unwrap();

                    let table_name = relid2name(table).unwrap();
                    let constr = table_name.split('.').last().unwrap();
                    let constr_r = String::from("lr.right.") + constr;
                    // println!("{:?}", constr_r);

                    //  let bin = CString::new(constr_r).unwrap();
                    //  let rec = record::ddlog_struct(bin.as_ptr(), [b].as_ptr(), 1);

                    let rec = record::Record::NamedStruct(
                        Cow::from(constr_r.to_owned()), fields);

                    // println!("{:?}", rec);

                    let table_id = HDDlog::get_table_id(&constr_r).unwrap();
                    let updates = &[record::UpdCmd::Insert(record::RelIdentifier::RelId(table_id as usize), rec)];
                    prog.transaction_start();
                    prog.apply_updates(updates.iter());
                    prog.transaction_commit_dump_changes(Some(show_out));

                    // println!("{:?}", updates);
                };
            }
            Err(e) => println!("{:?}", e),
        }

        Ok(())
    });

    work
}

fn show_out(table: usize, rec: &record::Record, polarity: bool) {
    println!("output is {:?}, {:?}, {:?}", table, rec, polarity);
}

fn main() {

    let addr = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
    let listener = TcpListener::bind(&addr).unwrap();

    let prog = HDDlog::run(1, false, None::<fn(usize, &record::Record, bool)>);

    let pm = Arc::new(prog);

    let server = listener.incoming().for_each(move |socket| {

        // prog.transaction_start();

        let work = handle_connection(socket, pm.clone());

        tokio::spawn(work);
        Ok(())
    }).map_err(|err| {
        // Handle error by printing to STDOUT.
        println!("accept error = {:?}", err);
    });

    tokio::run(server);
}
