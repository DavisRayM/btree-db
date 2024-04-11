use std::error::Error;

use crate::storage::layout::*;

/// Commands that are not part of the database DSL.
///
/// These commands mostly control what the REPL session does
#[derive(Debug, Clone)]
pub enum MetaCommand {
    /// Close the current REPL session
    Exit,
    /// Prints out layout information
    Layout,
}

impl MetaCommand {
    pub fn execute(&self) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Exit => {
                // NOTE: This will not drop any objects created
                std::process::exit(0);
            }
            Self::Layout => {
                println!("=== Common info ===");
                println!("Page size: {}", PAGE_SIZE);
                println!("Common header size: {}", PAGE_HEADERS_SIZE);
                println!();

                println!("=== Internal page info ===");
                println!("Header size: {}", INTERNAL_HEADER_SIZE);
                println!("Space for keys: {}", INTERNAL_SPACE_FOR_CELLS);
                println!("Max keys: {}", INTERNAL_MAX_KEYS);
                println!("Key size: {}", INTERNAL_CELL_SIZE);
                println!();

                println!("=== Leaf page info ===");
                println!("Header size: {}", LEAF_HEADER_SIZE);
                println!("Space for cells: {}", LEAF_SPACE_FOR_DATA);
                println!("Key cell size: {}", LEAF_KEY_CELL_SIZE);

                Ok(())
            }
        }
    }
}

impl TryInto<MetaCommand> for &str {
    type Error = String;

    fn try_into(self) -> Result<MetaCommand, Self::Error> {
        match self {
            ".exit" => Ok(MetaCommand::Exit),
            ".layout" => Ok(MetaCommand::Layout),
            _ => Err(format!("unknown command `{self}`.")),
        }
    }
}
