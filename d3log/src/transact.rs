use crate::tcp_network::ArcTcpNetwork;
use crate::{batch::Batch, Node};
//use async_std::sync::Mutex;
use differential_datalog::{program::Update, D3log, DDlog, DDlogDynamic, DeltaMap};
use mm_ddlog::api::HDDlog;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as SyncMutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Timestamp = u64;

pub struct TransactionManager {
    // svcc needs the membership, so we are going to assign nids
    // progress: Vec<Timestamp>,

    // would like network to be a trait object - since sync returns
    // a future this is now
    n: ArcTcpNetwork,

    // need access to some hddlog to call d3log_localize_val
    h: HDDlog,
}
// ok, Instant is monotonic, but SystemTime is not..we need something
// which is going to track across restarts and as monotonic, so we
// will burtally coerce SytemTime into monotonicity

fn current_time() -> Timestamp {
    let now = SystemTime::now();
    let delta = now
        .duration_since(UNIX_EPOCH)
        // fix this with a little sequence number
        .expect("Monotonicity violation");
    let ms = delta.subsec_millis();
    ms as u64
}

#[derive(Clone)]
pub struct ArcTransactionManager {
    t: Arc<SyncMutex<TransactionManager>>,
}

impl ArcTransactionManager {
    pub fn new() -> ArcTransactionManager {
        ArcTransactionManager {
            t: Arc::new(SyncMutex::new(TransactionManager::new())),
        }
    }
}

impl TransactionManager {
    fn start() {}

    // pass network
    pub fn new() -> TransactionManager {
        match HDDlog::run(1, false) {
            Ok((hddlog, _init_output)) => {
                TransactionManager {
                    n: ArcTcpNetwork::new(),
                    h: hddlog, // arc?
                    progress: Vec::new(),
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
pub async fn forward<'a>(tm: &ArcTransactionManager, input: Batch) -> Result<(), std::io::Error> {
    let mut output = HashMap::<Node, Box<Batch>>::new();
    for (rel, v, weight) in input {
        let tma = &*tm.t.lock().expect("lock");

        // we dont really need an instance here, do we? - i guess the state comes from
        // prog. we certainly dont need a lock on tm
        match tma.h.d3log_localize_val(rel, v.clone()) {
            Ok((loc_id, in_rel, inner_val)) => output
                .entry(loc_id.unwrap())
                .or_insert(Box::new(Batch::new(DeltaMap::new())))
                .insert(in_rel, inner_val, weight as u32),
            Err(val) => println!("{} {:+}", val, weight),
        }
    }

    // xxx - rust has issues getting a mut reference through this tuple unification..
    for (nid, b) in output.drain() {
        tm.t.lock().expect("lock").n.clone().send(nid, &b)?
    }
    Ok(())
}

pub async fn eval(t: ArcTransactionManager, input: Batch) -> Result<Batch, String> {
    let tm = t.t.lock().expect("lock");
    let h = &(*tm).h;

    // kinda harsh that we feed ddlog updates and get out a deltamap
    let mut upd = Vec::new();
    for (relid, v, _) in input {
        upd.push(Update::Insert { relid, v });
    }
    h.transaction_start()?;
    match h.apply_updates(&mut upd.clone().drain(..)) {
        Ok(()) => Ok(Batch::new(h.transaction_commit_dump_changes()?)),
        Err(err) => {
            println!("Failed to update differential datalog: {}", err);
            Err(err)
        }
    }
}
