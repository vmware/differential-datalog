use std::time::{SystemTime,UNIX_EPOCH};
use std::collections::HashMap;
use crate::{tcp_network::TcpNetwork,
            batch::Batch,
            Node};
use mm_ddlog::api::HDDlog;
use std::sync::Arc;
//use std::sync::Mutex as SyncMutex;
use async_std::sync::Mutex;
use tokio::task;
use differential_datalog::{DeltaMap, D3log, program::Update, DDlog, DDlogDynamic};

pub type Timestamp = u64;


pub struct TransactionManager {
    // svcc needs the membership, so we are going to assign nids
    progress : Vec<Timestamp>,
    
    // would like tcp network to be a trait object
    n : TcpNetwork,
    
    // need access to some hddlog to call d3log_localize_val
    h : HDDlog,
}
// ok, Instant is monotonic, but SystemTime is not..we need something
// which is going to track across restarts and as monotonic, so we
// will burtally coerce SytemTime into monotonicity

fn current_time() ->Timestamp {
    let now = SystemTime::now();
    let delta = now
        .duration_since(UNIX_EPOCH)
        // fix this with a little sequence number
        .expect("Monotonicity violation");
    let ms = delta.subsec_millis();
    return ms as u64;
}

impl TransactionManager {
    fn start(){
    }
    
    // pass network
    pub fn new() -> TransactionManager {
        match HDDlog::run(1, false) {
            Ok((hddlog, _init_output)) => {
                TransactionManager{
                    n:TcpNetwork::new(),
                    h:hddlog, // arc?
                    progress:Vec::new(),
                }
            }
            Err(err) => {
                // this is supposed to be management protocol
                panic!("Failed to run differential datalog: {}", err);
            }
        }
    }
}

    
// no asynch trait
// maybe this belongs in network?

pub async fn forward(tm : Arc::<Mutex::<TransactionManager>>,  input: Batch)  -> Result<(), std::io::Error> {
    let mut output = HashMap::<Node, Arc::<Mutex::<Batch>>>::new();
    for (rel, v, weight) in input {
        let tma = &*tm.lock().await;
        
        // we dont really need an instance here, do we? - i guess the state comes from
        // prog. we certainly dont need a lock on tm
        match tma.h.d3log_localize_val(rel, v.clone()) {
            Ok((loc_id, in_rel, inner_val)) => {
                let node_batch =  match output.get(&loc_id.unwrap()) {
                    Some(n) => n,
                    None => {
                        let loc = loc_id.unwrap();
                        println!("add node bnatch {}", loc as u64);
                        let n = Arc::new(Mutex::new(Batch::new(DeltaMap::new())));
                        output.insert(loc, n);
                        &output[&loc_id.expect("not actually")]
                    }
                };
                let x = &mut *(*node_batch).lock().await;
                x.insert(in_rel, inner_val, 1);
            }
            Err(val) => println!("{} {:+}", val, weight)
        }
    }



    let mut tasks = Vec::new();
    // xxx - rust has issues getting a mut reference through this tuple unification..    
    for (nid, _) in &output {
        let upd = output.get(&nid);// get_mut?
        // at minimum we should fork/join here
        let b = upd.expect("cant happen");
        // xxx - lock held across blocking operation?
        // we should let these sends complete async - needs some sort of collection bucket that
        // calls poll?
        let n = {&(&(*tm.lock().await)).n};
        let t = match n.send(*nid,b.clone()) {
            Ok(x) => x, Err(x) => return Err(x)
        };
        tasks.push(task::spawn(t));
    }
    Ok(())
}


pub async fn eval(t : Arc::<Mutex::<TransactionManager>>, input: Batch) -> Result<Batch, String> {
    let tm = t.lock().await;
    let h = &tm.h;

    // kinda harsh that we feed ddlog updates and get out a deltamap
    let mut upd = Vec::new();
    for (relid, v, _) in input {
        upd.push(Update::Insert {relid, v});
    }
    h.transaction_start()?;
    match h.apply_updates(&mut upd.clone().drain(..)) {
        Ok(()) =>  Ok(Batch::new(h.transaction_commit_dump_changes()?)),
        Err(err) => {
            println!("Failed to update differential datalog: {}", err);
            return Err(err);
        }
    }
}
