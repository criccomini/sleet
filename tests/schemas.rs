//! The checked-in JSON Schemas must accept real documents. The drift
//! test only proves generation is stable, not that documents validate.

use jsonschema::Validator;
use sleet::config::Service;
use sleet::config::{DatabaseConfig, SleetConfig};
use sleet::heartbeat::{Heartbeat, ServiceSummary};

fn schema(file: &str) -> serde_json::Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("schema")
        .join(file);
    serde_json::from_str(&std::fs::read_to_string(path).expect("schema reads"))
        .expect("schema parses")
}

fn validator(file: &str) -> Validator {
    jsonschema::validator_for(&schema(file)).expect("schema compiles")
}

#[track_caller]
fn assert_valid(validator: &Validator, doc: &serde_json::Value) {
    let errors: Vec<String> = validator
        .iter_errors(doc)
        .map(|e| format!("{e} at {}", e.instance_path()))
        .collect();
    assert!(errors.is_empty(), "{errors:?}\n{doc:#}");
}

fn example(file: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(file);
    std::fs::read_to_string(path).expect("example reads")
}

#[test]
fn example_sleet_toml_validates_against_the_config_schema() {
    let config: SleetConfig = toml::from_str(&example("sleet.toml")).unwrap();
    let doc = serde_json::to_value(&config).unwrap();
    assert_valid(&validator("config.schema.json"), &doc);
}

#[test]
fn example_db_toml_validates_against_the_database_defs_entry() {
    // A registry file is exactly a [database] table: validate against
    // the DatabaseConfig definition the config schema embeds.
    let mut root = schema("config.schema.json");
    root.as_object_mut()
        .unwrap()
        .insert("$ref".into(), "#/$defs/DatabaseConfig".into());
    for key in ["properties", "additionalProperties", "type", "required"] {
        root.as_object_mut().unwrap().remove(key);
    }
    let validator = jsonschema::validator_for(&root).expect("schema compiles");
    let db: DatabaseConfig = toml::from_str(&example("db.toml")).unwrap();
    assert_valid(&validator, &serde_json::to_value(&db).unwrap());
}

#[test]
fn heartbeat_bodies_validate_against_the_heartbeat_schema() {
    let heartbeat = Heartbeat::new(
        "sleet-1",
        "0.14.1",
        vec![ServiceSummary {
            service: Service::Gc,
            running: 3,
            backoff: 1,
        }],
    );
    let doc = serde_json::to_value(&heartbeat).unwrap();
    assert_valid(&validator("heartbeat.schema.json"), &doc);
}

#[test]
fn cli_responses_validate_against_the_cli_schema() {
    // The pinned trycmd JSON snapshots are real command output; they
    // must satisfy the response schema.
    let validator = validator("cli.schema.json");
    for snapshot in ["status_json.stdout", "register_json.stdout"] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/cmd")
            .join(snapshot);
        let doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).expect("snapshot reads"))
                .expect("snapshot parses");
        assert_valid(&validator, &doc);
    }
}
