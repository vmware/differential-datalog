// The main entry point to ../api.dl
use api_ddlog::*;
use api_ddlog::api::*;
use differential_datalog::record::*;
use differential_datalog::program::Update;
use std::borrow::Cow;

fn main() -> Result<(), String>{

    // start the DDlog program
    let prog = HDDlog::run(1, false, |_, _: &Record, _| {});

    // the update consists of inserting a single bool
    let table = HDDlog::get_table_id("Rin").unwrap();
    let b = Record::Bool(false);
    let rec = Record::PosStruct(Cow::from("Rin"), [b].to_vec());
    let mut updates = Vec::new();
    updates.push(Update::Insert{
        relid: 0,
        v: relval_from_record(table, &rec).unwrap()
    });

    // an entire transaction. changes execute on commit
    prog.transaction_start()?;
    prog.apply_valupdates(updates.into_iter())?;
    prog.transaction_commit_dump_changes()?;

    // stop the DDlog program
    prog.stop()
}
