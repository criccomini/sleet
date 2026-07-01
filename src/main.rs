use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "sleet", about = "SlateDB fleet manager", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the fleet spec JSON Schema.
    Schema,
    /// Parse and validate a fleet spec.
    Validate {
        /// Path to the fleet spec TOML file.
        #[arg(long)]
        spec: PathBuf,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Schema => {
            println!("{}", sleet::spec::schema_json());
            ExitCode::SUCCESS
        }
        Command::Validate { spec } => match sleet::spec::load(&spec) {
            Ok(_) => {
                println!("{}: ok", spec.display());
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        },
    }
}
