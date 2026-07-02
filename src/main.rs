use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sleet::render::Render;
use sleet::response::{StatusResponse, ValidateResponse};
use sleet::spec::LoadError;

#[derive(Parser)]
#[command(name = "sleet", about = "SlateDB fleet manager", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the fleet daemon.
    Run {
        /// Path to the fleet spec TOML file.
        #[arg(long)]
        spec: PathBuf,
    },
    /// Show fleet nodes, service assignments, and service health,
    /// derived from object storage.
    Status {
        /// Path to the fleet spec TOML file.
        #[arg(long)]
        spec: PathBuf,
        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: Format,
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
    /// Print a JSON Schema.
    Schema {
        /// Which schema to print.
        #[arg(value_enum, default_value = "config")]
        kind: SchemaKind,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum SchemaKind {
    /// The fleet spec TOML format (schema/config.schema.json).
    Config,
    /// Subcommand `--format json` responses, one `$defs` entry per
    /// command (schema/cli.schema.json).
    Cli,
    /// The heartbeat object body (schema/heartbeat.schema.json).
    Heartbeat,
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
        Command::Run { spec } => run(&spec),
        Command::Status { spec, format } => status(&spec, format),
        Command::Validate { spec, format } => validate(&spec, format),
        Command::Schema { kind } => {
            let json = match kind {
                SchemaKind::Config => sleet::spec::schema_json(),
                SchemaKind::Cli => sleet::response::schema_json(),
                SchemaKind::Heartbeat => sleet::heartbeat::schema_json(),
            };
            println!("{json}");
            ExitCode::SUCCESS
        }
    }
}

fn run(spec: &Path) -> ExitCode {
    // TODO: heartbeat loop, discovery, rendezvous assignment, and
    // per-(database, service) supervised tasks.
    match sleet::spec::load(spec) {
        Ok(_) => {
            eprintln!("error: `sleet run` is not implemented");
            ExitCode::FAILURE
        }
        Err(e) => fail(e),
    }
}

fn status(spec: &Path, format: Format) -> ExitCode {
    // TODO: LIST heartbeat objects under `fleet.heartbeats`, read the
    // assignments and service states each carries, and take compaction
    // queue depth from `.compactions`.
    match sleet::spec::load(spec) {
        Ok(_) => {
            eprintln!("note: stub response; status from object storage is not implemented");
            emit(&StatusResponse::stub(), format)
        }
        Err(e) => fail(e),
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
            Err(e) => fail(e),
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

fn emit<T: Serialize + Render>(response: &T, format: Format) -> ExitCode {
    match format {
        Format::Text => response
            .render(&mut io::stdout().lock())
            .expect("stdout write"),
        Format::Json => println!(
            "{}",
            serde_json::to_string_pretty(response).expect("response serializes")
        ),
    }
    ExitCode::SUCCESS
}

fn fail(e: LoadError) -> ExitCode {
    eprintln!("{e}");
    ExitCode::FAILURE
}
