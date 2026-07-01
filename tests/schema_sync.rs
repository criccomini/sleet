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
        include_str!("../schema/fleet.schema.json"),
        spec::schema_json(),
        "fleet-spec",
        "fleet.schema.json",
    );
}

#[test]
fn validate_response_schema_is_current() {
    assert_current(
        include_str!("../schema/validate.schema.json"),
        response::validate_schema_json(),
        "validate",
        "validate.schema.json",
    );
}

#[test]
fn status_response_schema_is_current() {
    assert_current(
        include_str!("../schema/status.schema.json"),
        response::status_schema_json(),
        "status",
        "status.schema.json",
    );
}

#[test]
fn db_list_response_schema_is_current() {
    assert_current(
        include_str!("../schema/db-list.schema.json"),
        response::db_list_schema_json(),
        "db-list",
        "db-list.schema.json",
    );
}

#[test]
fn db_edit_response_schema_is_current() {
    assert_current(
        include_str!("../schema/db-edit.schema.json"),
        response::db_edit_schema_json(),
        "db-edit",
        "db-edit.schema.json",
    );
}
