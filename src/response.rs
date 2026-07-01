//! Response types for one-shot subcommands run with `--format json`.
//!
//! These structs are the source of truth for the response schemas under
//! `schema/` (`sleet schema <kind>`); `tests/schema_sync.rs` fails if
//! the two drift.

use std::path::Path;

use schemars::JsonSchema;
use serde::Serialize;

use crate::spec::{FleetSpec, LoadError};

/// The `sleet validate` response.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet validate response")]
pub struct ValidateResponse {
    /// Path of the spec that was checked.
    pub spec: String,
    /// Whether the spec parsed and validated.
    pub valid: bool,
    /// Problems found; empty when valid.
    pub errors: Vec<String>,
}

impl ValidateResponse {
    pub fn new(spec: &Path, result: &Result<FleetSpec, LoadError>) -> Self {
        let errors = match result {
            Ok(_) => Vec::new(),
            Err(LoadError::Invalid(e)) => e.0.clone(),
            Err(e) => vec![e.to_string()],
        };
        Self {
            spec: spec.display().to_string(),
            valid: errors.is_empty(),
            errors,
        }
    }
}

/// The `sleet validate` response JSON Schema, pretty-printed.
pub fn validate_schema_json() -> String {
    let schema = schemars::schema_for!(ValidateResponse);
    serde_json::to_string_pretty(&schema).expect("schema serializes")
}
