use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

mod ipws;
mod palp;
mod parquet_utils;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Ipws(IpwsArgs),
    Palp(PalpArgs),
}

#[derive(Args)]
struct PalpArgs {
    #[arg(long, value_name = "FILE")]
    palp_in: PathBuf,

    #[arg(long, value_name = "FILE")]
    parquet_out: PathBuf,
}

#[derive(Args)]
struct IpwsArgs {
    #[arg(long, value_name = "FILE")]
    ws_in: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    polytope_info_in: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    parquet_in: Vec<PathBuf>,

    #[arg(long, value_name = "FILE")]
    ws_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    polytope_info_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    parquet_non_ip_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    parquet_non_reflexive_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    parquet_reflexive_out: Option<PathBuf>,

    #[arg(short, long)]
    include_derived_quantities: bool,

    #[arg(long)]
    limit: Option<usize>,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Ipws(args) => ipws::run(args),
        Commands::Palp(args) => palp::run(args),
    }
}
