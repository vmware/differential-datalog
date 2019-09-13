extern crate cmd_parser;

use std::env::current_exe;
use std::env::var_os;
use std::io::Write;
use std::process::exit;
use std::process::Command as Process;
use std::process::Stdio;

use cmd_parser::interact;
use cmd_parser::Command;

const CHILD_MARKER: &str = "CHILD";

/// Run a test of the `interact` function. The function reads from the
/// processes stdin and so we spin up a dedicated process for it (and
/// require the caller to effectively be a dedicated integration test).
pub fn run_interact_test<F>(input: &[u8], callback: F) -> i32
where
    F: Fn(Command, bool) -> (i32, bool),
{
    if var_os(CHILD_MARKER).is_none() {
        let mut child = Process::new(current_exe().unwrap())
            .arg("--nocapture")
            .env_clear()
            .env(CHILD_MARKER, "true")
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();

        child.stdin.as_mut().unwrap().write_all(input).unwrap();

        let status = child.wait().unwrap();
        status.code().unwrap()
    } else {
        exit(interact(callback))
    }
}
