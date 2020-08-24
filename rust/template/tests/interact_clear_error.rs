#![cfg(feature = "command-line")]

// Note that this file should only contain a single test. The test in
// here invokes itself and having multiple test running in parallel
// while that is happening is probably a bad idea.

use cmd_parser::Command;

mod common;

fn handler(command: Command, interactive: bool) -> (Result<(), String>, bool) {
    match command {
        Command::Clear(_) => (Err("unexpected clear command".to_string()), true),
        Command::Exit => (Ok(()), false),
        _ => (Err("unexpected command".to_string()), interactive),
    }
}

#[test]
fn interact_clear_error() {
    let input = b"\
clear NonExistantRelation;
exit;";
    assert_eq!(common::run_interact_test(input, handler), Ok(()))
}
