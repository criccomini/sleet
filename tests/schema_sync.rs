/// The checked-in JSON Schema must match what the spec structs generate.
/// Regenerate with: cargo run -- schema > schema/fleet.schema.json
#[test]
fn schema_file_is_current() {
    let generated = format!("{}\n", sleet::spec::schema_json());
    let checked_in = include_str!("../schema/fleet.schema.json");
    assert_eq!(
        checked_in, generated,
        "schema/fleet.schema.json is stale; regenerate with \
         `cargo run -- schema > schema/fleet.schema.json`"
    );
}
