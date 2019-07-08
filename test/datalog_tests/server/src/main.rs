extern crate serde_json;

use ddd_ddlog::api::*;
use ddd_ddlog::Relations::*;
use differential_datalog::record::*;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

fn handle_connection(stream: TcpStream, prog: Arc<HDDlog>) -> impl Future<Item = (), Error = ()> {
    let buf = Vec::new();
    let res = io::read_to_end(stream, buf);

    let work = res.then(move |result| {
        match result {
            Ok((_socket, buf)) => {
                let s = String::from_utf8(buf).unwrap();
                let (rec, _table): (Record, usize) = serde_json::from_str(&s).unwrap();

                let updates = &[UpdCmd::Insert(
                    RelIdentifier::RelId(lr_right_CMiddle as usize),
                    rec)];
                prog.transaction_start().unwrap();
                prog.apply_updates(updates.iter()).unwrap();
                prog.transaction_commit_dump_changes(Some(show_out)).unwrap();
            }
            Err(e) => println!("{:?}", e),
        }

        Ok(())
    });

    work
}

fn show_out(table: usize, rec: &Record, polarity: bool) {
    println!("output is {:?}, {:?}, {:?}", table, rec, polarity);
}

fn main() {
    let addr = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
    let listener = TcpListener::bind(&addr).unwrap();

    let prog = HDDlog::run(1, false, None::<fn(usize, &Record, bool)>);
    let prog_p = Arc::new(prog);

    let server = listener
        .incoming()
        .for_each(move |socket| {
            tokio::spawn(handle_connection(socket, prog_p.clone()));
            Ok(())
        })
        .map_err(|err| {
            println!("accept error = {:?}", err);
        });

    tokio::run(server);
}
