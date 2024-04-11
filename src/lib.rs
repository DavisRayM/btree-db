mod repl;
mod storage;

pub use repl::*;

macro_rules! calculate_offsets {
    ($start:ident, $size:ident) => {{
        let start = $start;
        let end = start + $size;
        (start, end)
    }};
}

pub(crate) use calculate_offsets;
