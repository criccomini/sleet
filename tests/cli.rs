//! CLI behavior snapshots. Each case in tests/cmd/ pins a command line
//! to its stdout/stderr and exit status. Update snapshots with:
//! TRYCMD=overwrite cargo test --test cli
#[test]
fn cli() {
    trycmd::TestCases::new().case("tests/cmd/*.toml");
}
