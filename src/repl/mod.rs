pub mod commands;

pub use commands::MetaCommand;
use std::{io::Write, path::PathBuf};

/// Starts a database REPL session
pub fn start_repl(name: String, _: Option<PathBuf>) {
    loop {
        print!("{name} > ");

        let mut input: String = String::new();
        std::io::stdout()
            .flush()
            .expect("failed to print to screen");
        std::io::stdin()
            .read_line(&mut input)
            .expect("failed to read command");
        let input = input.trim();

        let result: Result<MetaCommand, _> = input.try_into();
        if let Ok(command) = result {
            command.execute().expect("failed to execute command");
            continue;
        }

        std::io::stdout()
            .flush()
            .expect("failed to print to screen");
    }
}
