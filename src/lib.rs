mod repl;
mod storage;

pub use repl::*;
pub use storage::{Cursor, Table};

macro_rules! calculate_offsets {
    ($start:ident, $size:ident) => {{
        let start = $start;
        let end = start + $size;
        (start, end)
    }};
}

pub(crate) use calculate_offsets;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn calculate_offset() {
        let start = 0;
        let len = 10;
        let (start, end) = calculate_offsets!(start, len);

        assert_eq!(start, 0);
        assert_eq!(start + len, end);
    }
}
