//! The checked-in JSON Schemas must match what the structs generate.

/// Regenerate with: cargo run -- schema > schema/fleet.schema.json
#[test]
fn fleet_spec_schema_is_current() {
    let generated = format!("{}\n", sleet::spec::schema_json());
    let checked_in = include_str!("../schema/fleet.schema.json");
    assert_eq!(
        checked_in, generated,
        "schema/fleet.schema.json is stale; regenerate with \
         `cargo run -- schema > schema/fleet.schema.json`"
    );
}

/// Regenerate with: cargo run -- schema validate > schema/validate.schema.json
#[test]
fn validate_response_schema_is_current() {
    let generated = format!("{}\n", sleet::response::validate_schema_json());
    let checked_in = include_str!("../schema/validate.schema.json");
    assert_eq!(
        checked_in, generated,
        "schema/validate.schema.json is stale; regenerate with \
         `cargo run -- schema validate > schema/validate.schema.json`"
    );
}
