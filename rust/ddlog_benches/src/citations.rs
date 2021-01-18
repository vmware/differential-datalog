use citations_ddlog::{
    api::HDDlog as DDlog,
    typedefs::{internment::Intern, Citation},
    Relations,
};
use citations_differential_datalog::{
    ddval::{DDValConvert, DDValue},
    program::{RelId, Update},
    DDlog as _,
};
use serde_json::Value as JsonValue;
use std::{collections::HashMap, fs::File, io::BufReader};
use zstd::stream::read::Decoder;

/// Load the citations dataset with the requested number of samples
pub fn dataset(samples: usize) -> Vec<Update<DDValue>> {
    // Grab the file and wrap it in a buffered reader so it doesn't take forever
    // to read the entire thing in one go
    let file = Decoder::with_buffer(BufReader::new(
        File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/data/citations.zst"))
            .expect("could not open data file"),
    ))
    .expect("failed to create zstd decoder");

    // Parse the dataset
    let dataset: HashMap<String, Vec<JsonValue>> =
        serde_json::from_reader(file).expect("could not parse data file");

    // Load the specified number of citations into a vector
    let mut citations = Vec::with_capacity(dataset.len());
    for (id, entries) in dataset.into_iter().take(samples) {
        for entry in entries {
            if let JsonValue::String(entry) = entry {
                citations.push(Update::Insert {
                    relid: Relations::Citation as RelId,
                    v: Citation {
                        c1: Intern::new(id.clone()),
                        c2: Intern::new(entry),
                    }
                    .into_ddvalue(),
                });
            } else {
                panic!("reference list contained non-string entry");
            }
        }
    }

    citations
}

/// Creates a new ddlog instance with the specified number of workers
pub fn init(workers: usize) -> DDlog {
    let (ddlog, _) = DDlog::run(workers, false).expect("failed to create DDlog instance");
    ddlog
}

/// Runs the test case, returning the ddlog instance so that the dropping of the ddlog
/// instance isn't recorded in the benchmark
pub fn run(ddlog: DDlog, dataset: Vec<Update<DDValue>>) -> DDlog {
    ddlog
        .transaction_start()
        .expect("failed to start transaction");
    ddlog
        .apply_valupdates(dataset.into_iter())
        .expect("failed to give transaction input");
    ddlog
        .transaction_commit()
        .expect("failed to commit transaction");

    ddlog
}
