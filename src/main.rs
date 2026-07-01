use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use sleet::response::ValidateResponse;

#[derive(Parser)]
#[command(name = "sleet", about = "SlateDB fleet manager", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print a JSON Schema.
    Schema {
        /// Which schema to print.
        #[arg(value_enum, default_value = "fleet-spec")]
        kind: SchemaKind,
    },
    /// Parse and validate a fleet spec.
    Validate {
        /// Path to the fleet spec TOML file.
        #[arg(long)]
        spec: PathBuf,
        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: Format,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum SchemaKind {
    /// The fleet spec TOML format.
    FleetSpec,
    /// The `validate --format json` response.
    Validate,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    /// Human-readable text.
    Text,
    /// JSON matching the subcommand's response schema.
    Json,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Schema { kind } => {
            let json = match kind {
                SchemaKind::FleetSpec => sleet::spec::schema_json(),
                SchemaKind::Validate => sleet::response::validate_schema_json(),
            };
            println!("{json}");
            ExitCode::SUCCESS
        }
        Command::Validate { spec, format } => validate(&spec, format),
    }
}

fn validate(spec: &Path, format: Format) -> ExitCode {
    let result = sleet::spec::load(spec);
    match format {
        Format::Text => match result {
            Ok(_) => {
                println!("{}: ok", spec.display());
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        },
        Format::Json => {
            let response = ValidateResponse::new(spec, &result);
            println!(
                "{}",
                serde_json::to_string_pretty(&response).expect("response serializes")
            );
            if response.valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
    }
}
