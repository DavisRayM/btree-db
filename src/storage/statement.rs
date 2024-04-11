/// Database commands/statements
#[derive(Debug, Clone)]
pub enum Statement {
    Select,
    Insert(u64, Vec<u8>),
}
