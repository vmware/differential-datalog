// Note that this file should only contain a single test. The test in
// here invokes itself and having multiple test running in parallel
// while that is happening is probably a bad idea.

use cmd_parser::Command;

mod common;

fn handler(command: Command, interactive: bool) -> (i32, bool) {
    match command {
        Command::Exit => (42, false),
        _ => (0, interactive),
    }
}

#[test]
fn interact_exit() {
    assert_eq!(common::run_interact_test(br#"exit;"#, handler), 42)
}
