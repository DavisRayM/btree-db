#![allow(dead_code)]
use std::mem::size_of;

/// Size of the page structure on disk
pub const PAGE_SIZE: usize = 4096;

// Page headers
pub const PAGE_MAGIC: usize = 0xFEBA;
pub const PAGE_MAGIC_SIZE: usize = size_of::<usize>();
pub const PAGE_MAGIC_OFFSET: usize = 0;

pub const PAGE_TYPE_SIZE: usize = size_of::<u8>();
pub const PAGE_TYPE_OFFSET: usize = PAGE_MAGIC_OFFSET + PAGE_MAGIC_SIZE;

pub const PAGE_IS_ROOT_SIZE: usize = size_of::<u8>();
pub const PAGE_IS_ROOT_OFFSET: usize = PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE;

pub const PAGE_HEADERS_SIZE: usize = PAGE_MAGIC_SIZE + PAGE_TYPE_SIZE + PAGE_IS_ROOT_SIZE;

// Internal node headers
pub const INTERNAL_NUM_KEYS_SIZE: usize = size_of::<u64>();
pub const INTERNAL_NUM_KEYS_OFFSET: usize = PAGE_HEADERS_SIZE;

pub const INTERNAL_RIGHT_MOST_CHILD_SIZE: usize = size_of::<u64>();
pub const INTERNAL_RIGHT_MOST_CHILD_OFFSET: usize =
    INTERNAL_NUM_KEYS_OFFSET + INTERNAL_NUM_KEYS_SIZE;

pub const INTERNAL_HEADER_SIZE: usize =
    PAGE_HEADERS_SIZE + INTERNAL_NUM_KEYS_SIZE + INTERNAL_RIGHT_MOST_CHILD_SIZE;

// Internal node body
pub const INTERNAL_KEY_SIZE: usize = size_of::<usize>();
pub const INTERNAL_KEY_OFFSET: usize = 0;
pub const INTERNAL_KEY_POINTER_SIZE: usize = size_of::<usize>();
pub const INTERNAL_KEY_POINTER_OFFSET: usize = INTERNAL_KEY_OFFSET + INTERNAL_KEY_SIZE;

pub const INTERNAL_CELL_SIZE: usize = INTERNAL_NUM_KEYS_SIZE + INTERNAL_KEY_POINTER_SIZE;

pub const INTERNAL_SPACE_FOR_CELLS: usize = PAGE_SIZE - INTERNAL_HEADER_SIZE;
pub const INTERNAL_MAX_KEYS: usize = INTERNAL_SPACE_FOR_CELLS / INTERNAL_CELL_SIZE;

// Leaf node headers
pub const LEAF_OVERFLOW_POINTER_SIZE: usize = size_of::<u64>();
pub const LEAF_OVERFLOW_POINTER_OFFSET: usize = PAGE_HEADERS_SIZE;
pub const LEAF_OVERFLOW_POINTER_DEFAULT: u64 = u64::MAX;

pub const LEAF_NEXT_SIBLING_POINTER_SIZE: usize = size_of::<u64>();
pub const LEAF_NEXT_SIBLING_POINTER_OFFSET: usize =
    LEAF_OVERFLOW_POINTER_OFFSET + LEAF_OVERFLOW_POINTER_SIZE;

pub const LEAF_NUM_KEYS_SIZE: usize = size_of::<u64>();
pub const LEAF_NUM_KEYS_OFFSET: usize =
    LEAF_NEXT_SIBLING_POINTER_OFFSET + LEAF_NEXT_SIBLING_POINTER_SIZE;

pub const LEAF_FREE_SPACE_START_SIZE: usize = size_of::<u64>();
pub const LEAF_FREE_SPACE_START_OFFSET: usize = LEAF_NUM_KEYS_SIZE + LEAF_NUM_KEYS_OFFSET;

pub const LEAF_FREE_SPACE_END_SIZE: usize = size_of::<u64>();
pub const LEAF_FREE_SPACE_END_OFFSET: usize =
    LEAF_FREE_SPACE_START_OFFSET + LEAF_FREE_SPACE_START_SIZE;

pub const LEAF_HEADER_SIZE: usize = PAGE_HEADERS_SIZE
    + LEAF_OVERFLOW_POINTER_SIZE
    + LEAF_NEXT_SIBLING_POINTER_SIZE
    + LEAF_NUM_KEYS_SIZE
    + LEAF_FREE_SPACE_START_SIZE
    + LEAF_FREE_SPACE_END_SIZE;

// Leaf node body
pub const LEAF_NEXT_SIBLING_POINTER_DEFAULT: u64 = u64::MAX;

pub const LEAF_CELL_HAS_OVERFLOW_FLAG_SIZE: usize = size_of::<u8>();
pub const LEAF_CELL_HAS_OVERFLOW_FLAG_OFFSET: usize = 0;
pub const LEAF_KEY_IDENTIFIER_SIZE: usize = size_of::<u64>();
pub const LEAF_KEY_INDENTIFIER_OFFSET: usize =
    LEAF_CELL_HAS_OVERFLOW_FLAG_OFFSET + LEAF_CELL_HAS_OVERFLOW_FLAG_SIZE;
pub const LEAF_KEY_POINTER_SIZE: usize = size_of::<u64>();
pub const LEAF_KEY_POINTER_OFFSET: usize = LEAF_KEY_INDENTIFIER_OFFSET + LEAF_KEY_IDENTIFIER_SIZE;

pub const LEAF_KEY_CELL_SIZE: usize =
    LEAF_CELL_HAS_OVERFLOW_FLAG_SIZE + LEAF_KEY_IDENTIFIER_SIZE + LEAF_KEY_POINTER_SIZE;

pub const LEAF_CONTENT_LEN_SIZE: usize = size_of::<usize>();
pub const LEAF_CONTENT_LEN_OFFSET: usize = 0;
pub const LEAF_CONTENT_START_OFFSET: usize = LEAF_CONTENT_LEN_OFFSET + LEAF_CONTENT_LEN_SIZE;

pub const LEAF_SPACE_FOR_DATA: usize = PAGE_SIZE - LEAF_HEADER_SIZE;
