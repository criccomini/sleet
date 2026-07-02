pub mod heartbeat;
pub mod render;
pub mod response;
pub mod spec;

/// A type's JSON Schema, pretty-printed.
pub(crate) fn schema_pretty<T: schemars::JsonSchema>() -> String {
    let schema = schemars::schema_for!(T);
    serde_json::to_string_pretty(&schema).expect("schema serializes")
}
