use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sleet::render::Render;
use sleet::response::{
    DbEditAction, DbEditResponse, DbListResponse, StatusResponse, ValidateResponse,
};
use sleet::spec::{DEFAULT_HTTP_ADDR, LoadError, Service};

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
    /// Show fleet nodes, service assignments, and service health.
    Status {
        /// Status endpoint of a sleet node (`fleet.http_addr`).
        #[arg(long, default_value = DEFAULT_HTTP_ADDR)]
        addr: SocketAddr,
        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: Format,
    },
    /// Inspect and edit the databases in a fleet spec.
    #[command(subcommand)]
    Db(DbCommand),
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
        #[arg(value_enum, default_value = "fleet-spec")]
        kind: SchemaKind,
    },
}

#[derive(Subcommand)]
enum DbCommand {
    /// List explicit databases and discovery roots in a fleet spec.
    List {
        /// Path to the fleet spec TOML file.
        #[arg(long)]
        spec: PathBuf,
        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: Format,
    },
    /// Add an explicit database entry to a fleet spec.
    Add {
        /// Object-store URL of the database root.
        url: String,
        /// Path to the fleet spec TOML file.
        #[arg(long)]
        spec: PathBuf,
        /// Services for the entry (default: inherit `[defaults]`).
        #[arg(long, value_delimiter = ',', value_parser = parse_service)]
        services: Option<Vec<Service>>,
        /// Output format.
        #[arg(long, value_enum, default_value = "text")]
        format: Format,
    },
    /// Remove an explicit database entry from a fleet spec.
    Remove {
        /// Object-store URL of the database root.
        url: String,
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
    /// Subcommand `--format json` responses, one `$defs` entry per
    /// command.
    Response,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    /// Human-readable text.
    Text,
    /// JSON matching the subcommand's response schema.
    Json,
}

fn parse_service(s: &str) -> Result<Service, String> {
    match s {
        "gc" => Ok(Service::Gc),
        "compactor" => Ok(Service::Compactor),
        "workers" => Ok(Service::Workers),
        _ => Err(format!(
            "unknown service {s:?} (expected gc, compactor, or workers)"
        )),
    }
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Run { spec } => run(&spec),
        Command::Status { addr, format } => status(addr, format),
        Command::Db(cmd) => db(cmd),
        Command::Validate { spec, format } => validate(&spec, format),
        Command::Schema { kind } => {
            let json = match kind {
                SchemaKind::FleetSpec => sleet::spec::schema_json(),
                SchemaKind::Response => sleet::response::response_schema_json(),
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

fn status(_addr: SocketAddr, format: Format) -> ExitCode {
    // TODO: GET the status endpoint at `addr` once `sleet run` serves it.
    eprintln!("note: stub response; the node status endpoint is not implemented");
    emit(&StatusResponse::stub(), format)
}

fn db(cmd: DbCommand) -> ExitCode {
    match cmd {
        DbCommand::List { spec, format } => match sleet::spec::load(&spec) {
            Ok(s) => emit(&DbListResponse::from_spec(&s), format),
            Err(e) => fail(e),
        },
        DbCommand::Add {
            url,
            spec,
            services: _services,
            format,
        } => db_edit(&spec, url, DbEditAction::Added, format),
        DbCommand::Remove { url, spec, format } => {
            db_edit(&spec, url, DbEditAction::Removed, format)
        }
    }
}

fn db_edit(spec: &Path, url: String, action: DbEditAction, format: Format) -> ExitCode {
    match sleet::spec::load(spec) {
        Ok(s) => {
            let exists = s
                .database
                .iter()
                .any(|d| d.url.trim_end_matches('/') == url.trim_end_matches('/'));
            let changed = match action {
                DbEditAction::Added => !exists,
                DbEditAction::Removed => exists,
            };
            // TODO: apply the edit with toml_edit, preserving comments,
            // and write `--services` into added entries.
            eprintln!("note: stub response; {} was not modified", spec.display());
            emit(
                &DbEditResponse {
                    spec: spec.display().to_string(),
                    url,
                    action,
                    changed,
                },
                format,
            )
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
