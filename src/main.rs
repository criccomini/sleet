use std::io;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sleet::render::Render;
use sleet::response::StatusResponse;
use sleet::spec::Service;

#[derive(Parser)]
#[command(name = "sleet", about = "SlateDB fleet manager", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a fleet node.
    Run {
        /// Fleet root URL, e.g. s3://ops/sleet/.
        root: String,

        /// Node identity; must be unique within the fleet.
        #[arg(long)]
        node_id: String,

        /// Services this node offers.
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "gc,compactor-coordinator,compaction-workers"
        )]
        services: Vec<Service>,

        /// Maximum concurrent compaction jobs. Default: derived from the
        /// machine.
        #[arg(long)]
        max_compaction_jobs: Option<u32>,
    },
    /// Show fleet nodes, registered databases, and service placement,
    /// derived from the fleet root.
    Status {
        /// Fleet root URL, e.g. s3://ops/sleet/.
        root: String,

        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: Format,
    },
    /// Register a database: write its dbs/<db>.toml registry file.
    Register {
        /// Fleet root URL, e.g. s3://ops/sleet/.
        root: String,

        /// Database URL, e.g. s3://bucket/db.
        url: String,

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
        // TODO: heartbeat loop, config/registry polling, rendezvous
        // placement, and per-(database, service) supervised tasks.
        Command::Run { .. } => {
            eprintln!("error: `sleet run` is not implemented");
            ExitCode::FAILURE
        }
        // TODO: LIST nodes/ for liveness and roles, LIST dbs/ for the
        // registry, compute placement with the same rendezvous ranking,
        // and take compaction queue depth from `.compactions`.
        Command::Status { format, .. } => {
            eprintln!("note: stub response; status from object storage is not implemented");
            emit(&StatusResponse::stub(), format)
        }
        // TODO: canonicalize the URL and create-only PUT the registry
        // file.
        Command::Register { .. } => {
            eprintln!("error: `sleet register` is not implemented");
            ExitCode::FAILURE
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
