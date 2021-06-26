// temporary main - this should be the entry point for the d3log runtime

mod broadcast;
mod d3;
mod d3main;
mod datfile;
mod ddvalue_batch;
mod dispatch;
mod display;
mod error;
mod forwarder;
mod json_framer;
mod process;
mod record_batch;
mod tcp_network;

use crate::{
    broadcast::Broadcast,
    d3::{start_instance, Batch, Evaluator, Port, Transport},
    d3main::{Null, Print, D3},
    process::{FileDescriptorPort, MANAGEMENT_OUTPUT_FD},
};
use rand::Rng;
//use rustop::opts;
use std::sync::Arc;

fn main() {
    //    let (_args, _) = opts! {
    //        synopsis "D3log multiprocess test harness.";
    //        auto_shorts false;
    //        // --nodes or -n
    //        opt nodes:usize=1, short:'n', desc:"The number of worker processes. Default is 1.";
    // --input or -i
    // is there a way to get an Option<String> out of this?
    //  opt input_file:Option<String>, short:'i', desc:"Read input from file.";
    //    }
    //    .parse_or_exit();

    let management = Arc::new(Print(Arc::new(Null {})));

    let (_management_port, uuid) = if let Some(f) = std::env::var_os("uuid") {
        if let Some(f2) = f.to_str() {
            let m = FileDescriptorPort {
                management: management.clone(),
                fd: MANAGEMENT_OUTPUT_FD,
            };
            let uuid = f2.parse::<u128>().unwrap();
            (Arc::new(m) as Port, uuid)
        } else {
            panic!("bad uuid");
        }
    } else {
        // use uuid crate
        (
            Arc::new(Broadcast::new()) as Port,
            u128::from_be_bytes(rand::thread_rng().gen::<[u8; 16]>()),
        )
    };

    start_instance(
        D3::new(uuid, management.clone()).expect("D3"),
        uuid,
        management.clone(),
    )
    .expect("instance");
}
