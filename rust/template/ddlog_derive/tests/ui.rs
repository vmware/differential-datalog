#[test]
fn derive_ui_tests() {
    let test_cases = trybuild::TestCases::new();
    test_cases.compile_fail("tests/ui/fail/*.rs");
    test_cases.pass("tests/ui/pass/*.rs");
}
