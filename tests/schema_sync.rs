//! The checked-in JSON Schemas must match what the structs generate.
//! Regenerate stale files with:
//! UPDATE_SCHEMAS=1 cargo test --test schema_sync

use std::path::Path;

use sleet::{heartbeat, response, spec};

#[track_caller]
fn assert_current(file: &str, generated: String) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("schema")
        .join(file);
    let generated = format!("{generated}\n");
    if std::env::var_os("UPDATE_SCHEMAS").is_some() {
        std::fs::write(&path, generated).expect("write schema");
        return;
    }
    let checked_in = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        checked_in, generated,
        "schema/{file} is stale; regenerate with \
         `UPDATE_SCHEMAS=1 cargo test --test schema_sync`"
    );
}

#[test]
fn config_schema_is_current() {
    assert_current("config.schema.json", spec::schema_json());
}

#[test]
fn cli_schema_is_current() {
    assert_current("cli.schema.json", response::schema_json());
}

#[test]
fn heartbeat_schema_is_current() {
    assert_current("heartbeat.schema.json", heartbeat::schema_json());
}
