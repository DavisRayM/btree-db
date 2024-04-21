use crate::calculate_offsets;

use super::{
    layout::{
        INTERNAL_CELL_SIZE, INTERNAL_KEY_POINTER_SIZE, INTERNAL_KEY_SIZE,
        LEAF_CELL_HAS_OVERFLOW_FLAG_OFFSET, LEAF_CELL_HAS_OVERFLOW_FLAG_SIZE,
        LEAF_KEY_IDENTIFIER_SIZE, LEAF_KEY_INDENTIFIER_OFFSET,
    },
    page::bool_to_u8,
};

#[derive(Debug, Clone)]
pub struct LeafCell {
    overflow: bool,
    identifier: u64,
    content: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct InternalCell {
    key: u64,
    pointer: [u8; 8],
}

pub trait Cell {
    fn get_key(&self) -> u64;

    fn get_key_bytes(&self) -> Vec<u8>;

    fn get_content(&self) -> Vec<u8>;

    fn set_content(&mut self, c: Vec<u8>);
}

impl InternalCell {
    pub fn new(key: u64, pointer: [u8; 8]) -> Self {
        Self { key, pointer }
    }

    pub fn key(&self) -> u64 {
        self.key
    }

    pub fn pointer(&self) -> u64 {
        u64::from_be_bytes(self.pointer)
    }
}

impl LeafCell {
    pub fn new(id: u64, content: Vec<u8>, overflow: bool) -> Self {
        Self {
            identifier: id,
            content,
            overflow,
        }
    }

    /// Returns the size of the cells contents; excluding the flags and identifier
    pub fn content_size(&self) -> usize {
        self.content.len()
    }

    /// Returns whether the cell has an overflow
    pub fn has_overflow(&self) -> bool {
        self.overflow
    }

    /// Returns the indentifier of a leaf cell
    pub fn identifier(&self) -> u64 {
        self.identifier
    }
}

impl Cell for InternalCell {
    fn get_key(&self) -> u64 {
        self.key()
    }

    fn get_key_bytes(&self) -> Vec<u8> {
        unimplemented!("probably need to use the key() function")
    }

    fn get_content(&self) -> Vec<u8> {
        let mut out = [0x00; INTERNAL_CELL_SIZE];

        out[0..INTERNAL_KEY_SIZE].clone_from_slice(self.key.to_be_bytes().as_ref());
        out[INTERNAL_KEY_SIZE..INTERNAL_KEY_SIZE + INTERNAL_KEY_POINTER_SIZE]
            .clone_from_slice(self.pointer.as_ref());

        out.to_vec()
    }

    fn set_content(&mut self, c: Vec<u8>) {
        self.key = u64::from_be_bytes(
            c[0..INTERNAL_KEY_SIZE]
                .try_into()
                .expect("failed to read internal cell key data"),
        );
        self.pointer = c[INTERNAL_KEY_SIZE..INTERNAL_KEY_SIZE + INTERNAL_KEY_POINTER_SIZE]
            .try_into()
            .expect("failed to read internal cell key pointer data");
    }
}

impl Cell for LeafCell {
    fn get_key(&self) -> u64 {
        self.identifier()
    }

    fn get_key_bytes(&self) -> Vec<u8> {
        let mut out = [0x00; LEAF_CELL_HAS_OVERFLOW_FLAG_SIZE + LEAF_KEY_IDENTIFIER_SIZE];

        let (start, end) = calculate_offsets!(
            LEAF_CELL_HAS_OVERFLOW_FLAG_OFFSET,
            LEAF_CELL_HAS_OVERFLOW_FLAG_SIZE
        );
        out[start..end].clone_from_slice(&[bool_to_u8(self.overflow)]);

        let (start, end) =
            calculate_offsets!(LEAF_KEY_INDENTIFIER_OFFSET, LEAF_KEY_IDENTIFIER_SIZE);
        out[start..end].clone_from_slice(self.identifier.to_be_bytes().as_ref());

        out.to_vec()
    }

    fn get_content(&self) -> Vec<u8> {
        self.content.clone()
    }

    fn set_content(&mut self, c: Vec<u8>) {
        self.content = c;
    }
}

impl Default for InternalCell {
    fn default() -> Self {
        Self {
            key: u64::MAX,
            pointer: u64::MAX.to_be_bytes(),
        }
    }
}

impl Default for LeafCell {
    fn default() -> Self {
        Self {
            overflow: false,
            identifier: u64::MAX,
            content: Vec::with_capacity(0),
        }
    }
}
