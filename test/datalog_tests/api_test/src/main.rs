// The main entry point to ../copy.dl

use copy_ddlog::api::*;
use differential_datalog::record::*;

use std::ffi::CString;

fn main() {
    // start the DDlog program
    let prog = ddlog_run(1, false, None, 0, None);

    unsafe {
        // construct a singleton record
        let b = ddlog_bool(false);
        let table_name = CString::new("Rin").unwrap();
        let record = ddlog_struct(table_name.as_ptr(), [b].as_ptr(), 1);

        // prepare the updates, which contains an insertion
        let table_id = ddlog_get_table_id(table_name.as_ptr());
        let updates = &[ddlog_insert_cmd(table_id, record)];

        // an entire transaction. changes execute on commit
        ddlog_transaction_start(prog);
        ddlog_apply_updates(prog, updates.as_ptr(), 1);
        ddlog_transaction_commit_dump_changes(prog, Some(show_output), 0);

        // stop the DDlog program
        ddlog_stop(prog);
    }
}

// callback on commit
pub extern "C" fn show_output(
    arg: libc::uintptr_t,
    table: libc::size_t,
    rec: *const Record,
    polarity: bool,
) {
    unsafe {
        println!(
            "output is {:?}, {:?}, {:?}, {:?}",
            arg, table, *rec, polarity
        );
    }
}
