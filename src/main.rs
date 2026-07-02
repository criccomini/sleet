use std::io;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sleet::config::Service;
use sleet::daemon::{self, NodeOptions};
use sleet::render::Render;
use sleet::root::FleetRoot;
use sleet::{heartbeat, ops};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    name = "sleet",
    about = "A fleet manager for SlateDB databases",
    version
)]
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
        #[arg(long, value_parser = heartbeat::validate_node_id)]
        node_id: String,

        /// Services this node offers.
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "gc,compactor-coordinator,compaction-workers"
        )]
        services: Vec<Service>,

        /// Maximum databases compacting on this node at once. Default:
        /// the machine's available parallelism.
        #[arg(long)]
        max_compaction_jobs: Option<usize>,
    },
    /// Show fleet nodes, registered databases, and service placement,
    /// derived from the fleet root.
    Status {
        /// Fleet root URL, e.g. s3://ops/sleet/.
        root: String,

        /// Also read each database's compaction queue depth from
        /// `.compactions` (one read per database).
        #[arg(long)]
        compactions: bool,

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

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sleet=info,warn".into()),
        )
        .init();
    match Cli::parse().command {
        Command::Run {
            root,
            node_id,
            services,
            max_compaction_jobs,
        } => {
            // Reject duplicates loudly, like the config path does for
            // a registry file's services list.
            let mut seen = std::collections::HashSet::new();
            if let Some(dup) = services.iter().find(|s| !seen.insert(**s)) {
                return fail(format!(
                    "--services lists \"{}\" more than once",
                    dup.as_str()
                ));
            }
            let options = NodeOptions {
                node_id,
                services,
                max_compaction_jobs: max_compaction_jobs
                    .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |p| p.get())),
            };
            let root = match FleetRoot::open(&root) {
                Ok(root) => root,
                Err(e) => return fail(e),
            };
            let shutdown = CancellationToken::new();
            let trigger = shutdown.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    trigger.cancel();
                }
            });
            match daemon::run(root, options, shutdown).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => fail(e),
            }
        }
        Command::Status {
            root,
            compactions,
            format,
        } => {
            let root = match FleetRoot::open(&root) {
                Ok(root) => root,
                Err(e) => return fail(e),
            };
            match ops::status(&root, compactions).await {
                Ok(response) => emit(&response, format),
                Err(e) => fail(e),
            }
        }
        Command::Register { root, url, format } => {
            let root = match FleetRoot::open(&root) {
                Ok(root) => root,
                Err(e) => return fail(e),
            };
            match ops::register(&root, &url).await {
                Ok(response) => emit(&response, format),
                Err(e) => fail(e),
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

fn fail(e: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: {e}");
    ExitCode::FAILURE
}
