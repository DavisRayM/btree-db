pub mod commands;

pub use commands::MetaCommand;
use std::{io::Write, path::PathBuf};

use crate::{storage::statement::Statement, Cursor, Table};

/// Starts a database REPL session
pub fn start_repl(name: String, path: PathBuf) {
    let mut table = Table::new(path);
    env_logger::init();

    loop {
        // TODO: This needs to be at a better place
        table.flush_contents();
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

        let result: Result<Statement, _> = input.try_into();
        match result {
            Ok(s) => {
                let mut cursor = Cursor::new(&mut table);
                s.execute(&mut cursor);
            }
            Err(e) => println!("error: {}", e),
        }

        std::io::stdout()
            .flush()
            .expect("failed to print to screen");
    }
}
