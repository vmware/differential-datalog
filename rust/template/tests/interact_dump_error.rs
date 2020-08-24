#![cfg(feature = "command-line")]

// Note that this file should only contain a single test. The test in
// here invokes itself and having multiple test running in parallel
// while that is happening is probably a bad idea.

use cmd_parser::Command;

mod common;

fn handler(command: Command, interactive: bool) -> (Result<(), String>, bool) {
    match command {
        // We return `true` ("continue") on this path and verify that we
        // actually hit the exit command (i.e., we don't exit when
        // returning an error.
        Command::Dump(_) => (Err("unexpected dump command".to_string()), true),
        Command::Exit => (Ok(()), false),
        _ => (Err("unexpected command".to_string()), interactive),
    }
}

#[test]
fn interact_dump_error() {
    let input = b"\
dump NonExistantRelation;
exit;";
    assert_eq!(common::run_interact_test(input, handler), Ok(()))
}
