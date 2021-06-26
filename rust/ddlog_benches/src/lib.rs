pub mod live_journal;
pub mod twitter;
pub mod utils;

use differential_datalog::{
    api::HDDlog,
    ddval::DDValue,
    program::{Config, Update},
    DDlog, DDlogDynamic,
};

pub fn init(workers: usize) -> HDDlog {
    let (ddlog, _) = benchmarks_ddlog::run(workers, false).expect("failed to start ddlog instance");

    ddlog
}

pub fn run(ddlog: HDDlog, dataset: Vec<Update<DDValue>>) -> HDDlog {
    ddlog
        .transaction_start()
        .expect("failed to start transaction");
    ddlog
        .apply_updates(&mut dataset.into_iter())
        .expect("failed to give transaction input");
    ddlog
        .transaction_commit()
        .expect("failed to commit transaction");

    ddlog
}
