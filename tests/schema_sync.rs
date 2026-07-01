//! The checked-in JSON Schemas must match what the structs generate.
//! Regenerate a stale file with:
//! cargo run -- schema <kind> > schema/<file>

use sleet::{response, spec};

#[track_caller]
fn assert_current(checked_in: &str, generated: String, kind: &str, file: &str) {
    assert_eq!(
        checked_in,
        format!("{generated}\n"),
        "schema/{file} is stale; regenerate with \
         `cargo run -- schema {kind} > schema/{file}`"
    );
}

#[test]
fn fleet_spec_schema_is_current() {
    assert_current(
        include_str!("../schema/config.schema.json"),
        spec::schema_json(),
        "fleet-spec",
        "config.schema.json",
    );
}

#[test]
fn response_schema_is_current() {
    assert_current(
        include_str!("../schema/cli.schema.json"),
        response::response_schema_json(),
        "response",
        "cli.schema.json",
    );
}
