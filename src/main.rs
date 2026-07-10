use std::io;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sleet::render::Render;
use sleet::{
    CancellationToken, Fleet, MirrorSyncOptions, NodeOptions, RestorePoint, Service, StatusOptions,
    mirror_restore,
};

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
        #[arg(long, value_parser = sleet::heartbeat::validate_node_id)]
        node_id: String,

        /// Services this node offers.
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "gc,compactor-coordinator,compaction-workers,mirror"
        )]
        services: Vec<Service>,

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
        db: String,

        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: Format,
    },
    /// Mirror operations: sync, restore.
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
            let mut options = NodeOptions::new(node_id).with_services(services);
            if let Some(max_mirror_jobs) = max_mirror_jobs {
                options = options.with_max_mirror_jobs(max_mirror_jobs);
            }
            if let Some(rclone) = rclone {
                options = options.with_rclone(rclone);
            }
            let fleet = match Fleet::open(&root) {
                Ok(fleet) => fleet,
                Err(e) => return fail(e),
            };
            let shutdown = CancellationToken::new();
            let trigger = shutdown.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    trigger.cancel();
                }
            });
            match fleet.run_node(options, shutdown).await {
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
            let fleet = match Fleet::open(&root) {
                Ok(fleet) => fleet,
                Err(e) => return fail(e),
            };
            let options = StatusOptions::default()
                .with_compactions(compactions)
                .with_mirrors(mirrors);
            match fleet.status(options).await {
                Ok(response) => emit(&response, format),
                Err(e) => fail(e),
            }
        }
        Command::Register { root, db, format } => {
            let fleet = match Fleet::open(&root) {
                Ok(fleet) => fleet,
                Err(e) => return fail(e),
            };
            match fleet.register(&db).await {
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
                let fleet = match Fleet::open(&root) {
                    Ok(fleet) => fleet,
                    Err(e) => return fail(e),
                };
                let mut options = MirrorSyncOptions::default();
                if let Some(rclone) = rclone {
                    options = options.with_rclone(rclone);
                }
                match fleet.sync_mirror(&db, &target, options).await {
                    Ok(response) => emit(&response, format),
                    Err(e) => fail(e),
                }
            }
            MirrorCommand::Restore {
                backup,
                dest,
                at,
                format,
            } => {
                let at = match at.as_deref().map(RestorePoint::parse).transpose() {
                    Ok(at) => at.unwrap_or(RestorePoint::Latest),
                    Err(e) => return fail(e),
                };
                match mirror_restore(&backup, &dest, at).await {
                    Ok(response) => emit(&response, format),
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
