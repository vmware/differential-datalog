use benchmarks_ddlog::{api::HDDlog, typedefs::twitter::Edge, Relations};
use benchmarks_differential_datalog::{
    DDlog, DDlogDynamic,
    ddval::{DDValConvert, DDValue},
    program::{RelId, Update}
};
use csv::Reader;
use flate2::bufread::GzDecoder;
use std::{fs::File, io::BufReader};

pub fn dataset(samples: Option<usize>) -> Vec<Update<DDValue>> {
    // The indices of the csv rows
    const NODE_ID: usize = 0;
    const TWITTER_ID: usize = 1;

    // Grab the file and wrap it in a buffered reader so it doesn't take forever
    // to read the entire thing in one go
    let reader = Reader::from_reader(GzDecoder::new(BufReader::new(
        File::open(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/data/twitter-2010-ids.csv.gz",
        ))
        .expect("could not open data file"),
    )));

    let mut edges = Vec::with_capacity(samples.unwrap_or(40_000_000));

    for record in reader
        .into_records()
        .flat_map(|record| record.ok())
        .take(samples.unwrap_or_else(usize::max_value))
    {
        let edge = Edge {
            from: record.get(NODE_ID).unwrap().parse().unwrap(),
            to: record.get(TWITTER_ID).unwrap().parse().unwrap(),
        };

        edges.push(Update::Insert {
            relid: Relations::twitter_Edge as RelId,
            v: edge.into_ddvalue(),
        });
    }

    edges
}

pub fn init(workers: usize) -> HDDlog {
    let (ddlog, _) = HDDlog::run(workers, false).expect("failed to create DDlog instance");
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
