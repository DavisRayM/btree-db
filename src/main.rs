use std::path::PathBuf;

use btree_db::start_repl;
use clap::Parser;

#[derive(Parser)]
#[command(version, about,long_about = None)]
struct Cli {
    /// Optional name to operate on
    name: Option<String>,

    /// Optionally, sets a database file to use
    #[arg(short, long, value_name = "FILE")]
    file: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    let name = cli.name.unwrap_or("db".into());
    let path = cli.file.unwrap_or("/tmp/default.db".into());

    start_repl(name, path)
}
