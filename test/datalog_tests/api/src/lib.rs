#[cfg(test)]
mod tests {
    use api_ddlog::api::*;
    use api_ddlog::*;
    use differential_datalog::program::Update;
    use differential_datalog::record::*;
    use std::borrow::Cow;

    #[test]
    fn start_stop() -> Result<(), String> {
        let (prog, _) = HDDlog::run(1, false);

        // the update consists of inserting a single bool
        let table = HDDlog::get_table_id("Rin").unwrap();
        let b = Record::Bool(false);
        let rec = Record::PosStruct(Cow::from("Rin"), [b].to_vec());
        let mut updates = Vec::new();
        updates.push(Update::Insert {
            relid: 0,
            v: relval_from_record(table, &rec).unwrap(),
        });

        // an entire transaction. changes execute on commit
        prog.transaction_start()?;
        prog.apply_valupdates(updates.into_iter())?;
        prog.transaction_commit_dump_changes()?;

        prog.stop()
    }
}
