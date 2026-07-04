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
            default_value = "gc,compactor-coordinator,compaction-workers,mirror"
        )]
        services: Vec<Service>,

        /// Maximum databases compacting on this node at once. Default:
        /// the machine's available parallelism.
        #[arg(long)]
        max_compaction_jobs: Option<usize>,

        /// Maximum (database, target) mirror jobs copying or pruning on
        /// this node at once. Default: the machine's available
        /// parallelism.
        #[arg(long)]
        max_mirror_jobs: Option<usize>,

        /// Path to the rclone binary, for mirror targets with
        /// copier = "rclone".
        #[arg(long)]
        rclone: Option<String>,
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

        /// Also read each (database, target) mirror's source and
        /// destination heads and report lag (several reads per pair).
        #[arg(long)]
        mirrors: bool,

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
    /// Mirror operations: sync, restore, prefixes.
    Mirror {
        #[command(subcommand)]
        command: MirrorCommand,
    },
}

#[derive(Subcommand)]
enum MirrorCommand {
    /// Run a single sync pass for one (database, target), regardless
    /// of the target's mode; prunes afterward when retention is set.
    Sync {
        /// Fleet root URL, e.g. s3://ops/sleet/.
        root: String,

        /// Registered database URL.
        db: String,

        /// Mirror target name.
        target: String,

        /// Path to the rclone binary, for targets with
        /// copier = "rclone".
        #[arg(long)]
        rclone: Option<String>,

        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: Format,
    },
    /// Copy one restore point's closure from a backup into an empty
    /// destination root and commit it.
    Restore {
        /// Fleet root URL, e.g. s3://ops/sleet/.
        root: String,

        /// Backup root URL (a mirror destination).
        backup: String,

        /// Empty destination root URL.
        dest: String,

        /// The restore point: a manifest id or an RFC 3339 timestamp.
        /// Default: the backup's latest manifest.
        #[arg(long)]
        at: Option<String>,

        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: Format,
    },
    /// Emit the anchored key-prefix filter lists an external
    /// replication service needs for one (database, target).
    Prefixes {
        /// Fleet root URL, e.g. s3://ops/sleet/.
        root: String,

        /// Registered database URL.
        db: String,

        /// Mirror target name.
        target: String,

        /// Which service's configuration shape to emit.
        #[arg(long, value_enum)]
        format: sleet::response::PrefixFormat,
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
            max_mirror_jobs,
            rclone,
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
            let parallelism = || std::thread::available_parallelism().map_or(4, |p| p.get());
            let options = NodeOptions {
                node_id,
                services,
                max_compaction_jobs: max_compaction_jobs.unwrap_or_else(parallelism),
                max_mirror_jobs: max_mirror_jobs.unwrap_or_else(parallelism),
                rclone,
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
            mirrors,
            format,
        } => {
            let root = match FleetRoot::open(&root) {
                Ok(root) => root,
                Err(e) => return fail(e),
            };
            match ops::status(&root, compactions, mirrors).await {
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
        Command::Mirror { command } => match command {
            MirrorCommand::Sync {
                root,
                db,
                target,
                rclone,
                format,
            } => {
                let root = match FleetRoot::open(&root) {
                    Ok(root) => root,
                    Err(e) => return fail(e),
                };
                match ops::mirror_sync(&root, &db, &target, rclone.as_deref()).await {
                    Ok(response) => emit(&response, format),
                    Err(e) => fail(e),
                }
            }
            MirrorCommand::Restore {
                root,
                backup,
                dest,
                at,
                format,
            } => {
                if let Err(e) = FleetRoot::open(&root) {
                    return fail(e);
                }
                let at = match at
                    .as_deref()
                    .map(sleet::mirror::RestorePoint::parse)
                    .transpose()
                {
                    Ok(at) => at.unwrap_or(sleet::mirror::RestorePoint::Latest),
                    Err(e) => return fail(e),
                };
                match ops::mirror_restore(&backup, &dest, at).await {
                    Ok(response) => emit(&response, format),
                    Err(e) => fail(e),
                }
            }
            MirrorCommand::Prefixes {
                root,
                db,
                target,
                format,
            } => {
                let root = match FleetRoot::open(&root) {
                    Ok(root) => root,
                    Err(e) => return fail(e),
                };
                match ops::mirror_prefixes(&root, &db, &target, format).await {
                    Ok(response) => emit(&response, Format::Text),
                    Err(e) => fail(e),
                }
            }
        },
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
