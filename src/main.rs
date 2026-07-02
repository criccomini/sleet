use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sleet::render::Render;
use sleet::response::StatusResponse;
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
