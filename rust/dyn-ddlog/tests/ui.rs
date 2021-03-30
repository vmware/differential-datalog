use std::process::Command;

use goldentests::{TestConfig, TestResult};

#[test]
fn goldentests() -> TestResult<()> {
    let _ = Command::new("cargo")
        .args(&["build", "--bin", "ddlog", "--features", "structopt"])
        .spawn()
        .and_then(|mut child| child.wait());

    let mut config = TestConfig::new("../../target/debug/ddlog", "tests/corpus", "// ")?;
    config.verbose = true;

    config.run_tests()
}
