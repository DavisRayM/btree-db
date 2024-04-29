use core::panic;
use std::{fmt::Display, mem::size_of, sync::Arc};

use log::debug;

use crate::{
    calculate_offsets,
    storage::layout::{
        INTERNAL_CELL_SIZE, INTERNAL_KEY_POINTER_SIZE, INTERNAL_MAX_KEYS, INTERNAL_NUM_KEYS_OFFSET,
        INTERNAL_RIGHT_MOST_CHILD_OFFSET, INTERNAL_RIGHT_MOST_CHILD_SIZE,
        LEAF_FREE_SPACE_END_OFFSET, LEAF_FREE_SPACE_START_OFFSET, LEAF_KEY_INDENTIFIER_OFFSET,
        LEAF_NEXT_SIBLING_POINTER_DEFAULT, LEAF_NEXT_SIBLING_POINTER_OFFSET, LEAF_NUM_KEYS_OFFSET,
        PAGE_SIZE,
    },
};

use super::{
    cell::{Cell, LeafCell},
    layout::{
        INTERNAL_HEADER_SIZE, INTERNAL_KEY_OFFSET, INTERNAL_KEY_POINTER_OFFSET,
        LEAF_CONTENT_LEN_SIZE, LEAF_HEADER_SIZE, LEAF_KEY_CELL_SIZE, LEAF_KEY_POINTER_OFFSET,
        LEAF_OVERFLOW_POINTER_DEFAULT, LEAF_OVERFLOW_POINTER_OFFSET, PAGE_IS_ROOT_OFFSET,
        PAGE_IS_ROOT_SIZE, PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE,
    },
    page::{bool_to_u8, u8_to_bool, CachedPage, Page, PageType},
};

type Result<T> = std::result::Result<T, NodeResult>;

/// Possible result types that can be returned by [Node](Node) operations
#[derive(Debug, Clone)]
pub enum NodeResult {
    /// Returned when a node is full and requires a split action to be performed
    IsFull,
    /// Returned when a node has an overflow.
    ///
    /// Returns the remaining content that needs to be written.
    HasOverflow(Vec<u8>),
    /// Returned when trying to read a node with invalid page content
    InvalidPage { desc: String },
    /// Returned when trying to insert a duplicate key
    DuplicateKey,
    /// Returned when the identifier given for an operation does not exist
    KeyDoesNotExist,
}

impl Display for NodeResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            Self::IsFull => "node is currently full".to_string(),
            Self::HasOverflow(_) => "node has overflow".to_string(),
            Self::InvalidPage { desc } => format!("invalid page; {desc}"),
            Self::DuplicateKey => "duplicate key".to_string(),
            Self::KeyDoesNotExist => "key does not exist".to_string(),
        };

        write!(f, "{}", msg)
    }
}

// In-memory representation of a page.
//
// This structure is used to manipulate page contents in memory
pub struct Node {
    page: CachedPage,
    keys: u64,
    _type: PageType,
    buffer: Option<Page>,
}

impl Node {
    /// Creates a new [Node](Node) wrapper around a [CachedPage](CachedPage).
    ///
    pub fn load(page: CachedPage) -> Result<Self> {
        let mut obj = Self {
            page,
            keys: 0,
            _type: PageType::Leaf,
            buffer: None,
        };

        obj._type = obj.read_variable_data(PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE, false)[0]
            .try_into()
            .map_err(|e| NodeResult::InvalidPage {
                desc: format!("error while reading page type; {}", e),
            })?;
        obj.keys = obj.num_cells();

        Ok(obj)
    }

    pub fn find_cell_num(&self, key: u64) -> u64 {
        let num_cells = self.num_cells();
        let mut min_idx = 0;
        let mut max_idx = self.num_cells();

        match self._type {
            PageType::Leaf => {
                while min_idx != max_idx {
                    let index = (min_idx + max_idx) / 2;
                    let key_at_index = self.get_cell_key(self.calculate_cell_position(index), true);

                    if key == key_at_index {
                        return index;
                    } else if key < key_at_index {
                        max_idx = index;
                    } else {
                        min_idx = index + 1;
                    }
                }

                min_idx
            }
            PageType::Internal => {
                while min_idx != max_idx {
                    let index = (min_idx + max_idx) / 2;
                    let key_at_right = self.get_cell_key(self.calculate_cell_position(index), true);

                    if key_at_right >= key {
                        max_idx = index
                    } else {
                        min_idx = index + 1;
                    }
                }

                if min_idx >= num_cells {
                    num_cells
                } else {
                    min_idx
                }
            }
        }
    }

    pub fn node_high_key(&self) -> u64 {
        let cell_num = self.num_cells() - 1;
        self.get_cell_key(self.calculate_cell_position(cell_num), false)
    }

    pub fn node_type(&self) -> PageType {
        self.read_variable_data(PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE, false)[0]
            .try_into()
            .expect("failed to retrieve page type")
    }

    pub fn is_root(&self) -> bool {
        u8_to_bool(self.read_variable_data(PAGE_IS_ROOT_OFFSET, PAGE_IS_ROOT_SIZE, true)[0])
            .unwrap()
    }

    pub fn set_is_root(&mut self, val: bool) {
        self.write_all_bytes(vec![bool_to_u8(self.is_root())], PAGE_IS_ROOT_OFFSET);
    }

    pub fn overflow_pointer(&self) -> Option<u64> {
        if self._type == PageType::Internal {
            panic!("internal pages do not support overflows");
        } else {
            match self.read_u64_data(LEAF_OVERFLOW_POINTER_OFFSET, true) {
                LEAF_OVERFLOW_POINTER_DEFAULT => None,
                v => Some(v),
            }
        }
    }

    pub fn next_sibling(&self) -> Option<u64> {
        if self._type == PageType::Internal {
            None
        } else {
            match self.read_u64_data(LEAF_NEXT_SIBLING_POINTER_OFFSET, true) {
                LEAF_NEXT_SIBLING_POINTER_DEFAULT => None,
                v => Some(v),
            }
        }
    }

    pub fn set_next_sibling(&mut self, pointer: u64) {
        self.write_all_bytes(
            pointer.to_be_bytes().to_vec(),
            LEAF_NEXT_SIBLING_POINTER_OFFSET,
        );
    }

    pub fn num_cells(&self) -> u64 {
        match self._type {
            PageType::Leaf => self.read_u64_data(LEAF_NUM_KEYS_OFFSET, true),
            PageType::Internal => self.read_u64_data(INTERNAL_NUM_KEYS_OFFSET, true),
        }
    }

    pub fn insert_cell<T: Cell>(&mut self, cell: T) -> Result<()> {
        if self.check_key_exists(cell.get_key()) {
            return Err(NodeResult::DuplicateKey);
        }

        self.check_has_space(cell.get_key())?;

        debug!("inserting new cell");
        match self._type {
            PageType::Internal => self.insert_internal_cell(cell),
            PageType::Leaf => self.insert_leaf_cell(cell),
        }
    }

    pub fn right_child(&self) -> Option<u64> {
        match self._type {
            PageType::Leaf => None,
            PageType::Internal => Some(self.read_u64_data(INTERNAL_RIGHT_MOST_CHILD_OFFSET, true)),
        }
    }

    pub fn read_cell_bytes(&self, num: u64) -> Vec<u8> {
        let cell_pos = self.calculate_cell_position(num) as usize;

        match self._type {
            PageType::Internal => {
                if num < self.num_cells() {
                    self.read_variable_data(cell_pos, INTERNAL_CELL_SIZE, true)
                } else {
                    let mut vec = self.node_high_key().to_be_bytes().to_vec();
                    vec.append(&mut self.read_variable_data(
                        INTERNAL_RIGHT_MOST_CHILD_OFFSET,
                        INTERNAL_RIGHT_MOST_CHILD_SIZE,
                        true,
                    ));
                    vec
                }
            }
            PageType::Leaf => {
                let mut pointer = self.get_cell_key_pointer(cell_pos as u64, false) as usize;
                let content_size = self.read_u64_data(pointer, true);
                pointer += LEAF_CONTENT_LEN_SIZE;

                self.read_variable_data(pointer, content_size as usize, true)
            }
        }
    }

    /// Splits the contents of the current node and inserts the split content into the passed in
    /// Node.
    pub fn split<T: Cell>(&mut self, node: &mut Node, cell: T) -> Result<()> {
        // Splits are a bit iffy; This enables us to recover from any errors that occur during
        // them. All writes during this operation are sent to the buffer which is then flushed
        // after a successful split
        self.set_buffer();
        node.set_buffer();

        let res = match self.node_type() {
            PageType::Internal => self.split_internal_node(node, cell),
            PageType::Leaf => self.split_leaf_node(node, cell),
        };

        if let Err(e) = res {
            self.buffer = None;
            node.buffer = None;
            Err(e)
        } else {
            self.flush_buffer();
            node.flush_buffer();

            if let Some(sibling) = self.next_sibling() {
                node.set_next_sibling(sibling);
            }

            Ok(())
        }
    }

    pub fn update<T: Cell>(&mut self, identifier: u64, cell: T) -> Result<()> {
        if !self.check_key_exists(identifier) {
            return Err(NodeResult::KeyDoesNotExist);
        }

        let cell_num = self.find_cell_num(identifier);
        match self._type {
            PageType::Internal => {
                let pointer_bytes = cell.get_content()[INTERNAL_KEY_POINTER_OFFSET
                    ..INTERNAL_KEY_POINTER_SIZE + INTERNAL_KEY_POINTER_OFFSET]
                    .to_vec();

                if cell_num >= self.num_cells() {
                    self.write_all_bytes(pointer_bytes, INTERNAL_RIGHT_MOST_CHILD_OFFSET);
                } else {
                    let pos = self.calculate_cell_position(cell_num) as usize;
                    self.write_all_bytes(
                        cell.get_key().to_be_bytes().to_vec(),
                        pos + INTERNAL_KEY_OFFSET,
                    );
                    self.write_all_bytes(pointer_bytes, pos + INTERNAL_KEY_POINTER_OFFSET);
                }
            }
            PageType::Leaf => {
                todo!()
            }
        }

        Ok(())
    }

    /// Retrieve the cell position for an Internal node key or Leaf node key
    fn calculate_cell_position(&self, num: u64) -> u64 {
        match self._type {
            PageType::Leaf => LEAF_HEADER_SIZE as u64 + (num * LEAF_KEY_CELL_SIZE as u64),
            PageType::Internal => INTERNAL_HEADER_SIZE as u64 + (num * INTERNAL_CELL_SIZE as u64),
        }
    }

    fn check_key_exists(&self, key: u64) -> bool {
        let pos = self.calculate_cell_position(self.find_cell_num(key));

        self.get_cell_key(pos, false) == key
    }

    /// Checks if the particular node has space
    ///
    /// - Internal nodes: are checked against the maximum allowed number of keys. Ensuring the node
    /// only stores N+1 key; The +1 being the right-most pointer.
    /// - Leaf nodes: are checked to ensure the node can store one more key entry and have left
    /// over space; If only one key can be stored without it's data or part of it's data it has
    /// filled up
    fn check_has_space(&self, key: u64) -> Result<()> {
        match self._type {
            PageType::Leaf => {
                let free_space = self.read_u64_data(LEAF_FREE_SPACE_END_OFFSET, true)
                    - self.read_u64_data(LEAF_FREE_SPACE_START_OFFSET, true);

                match free_space as u64 {
                    v if v <= LEAF_KEY_CELL_SIZE as u64
                        || v - LEAF_KEY_CELL_SIZE as u64 <= LEAF_KEY_CELL_SIZE as u64 =>
                    {
                        return Err(NodeResult::IsFull)
                    }
                    _ => (),
                }
            }
            PageType::Internal => {
                if self.num_cells() + 1 > INTERNAL_MAX_KEYS as u64 {
                    return Err(NodeResult::IsFull);
                }
            }
        };

        Ok(())
    }

    fn flush_buffer(&mut self) {
        if let Some(buf) = self.buffer.take() {
            self.write_all_bytes(buf[..].to_vec(), 0);
        }
    }

    fn get_cell_key(&self, pos: u64, buffered: bool) -> u64 {
        let start_pos = match self._type {
            PageType::Leaf => LEAF_KEY_INDENTIFIER_OFFSET + pos as usize,
            PageType::Internal => INTERNAL_KEY_OFFSET + pos as usize,
        };

        self.read_u64_data(start_pos, buffered)
    }

    fn get_cell_key_pointer(&self, pos: u64, buffered: bool) -> u64 {
        let start_pos = match self._type {
            PageType::Leaf => LEAF_KEY_POINTER_OFFSET + pos as usize,
            PageType::Internal => INTERNAL_KEY_POINTER_OFFSET + pos as usize,
        };

        self.read_u64_data(start_pos, buffered)
    }

    fn insert_internal_cell<T: Cell>(&mut self, cell: T) -> Result<()> {
        let key = cell.get_key();
        let cell_num = self.find_cell_num(key);
        let mut bytes: Vec<u8>;

        if cell_num >= self.num_cells() {
            bytes = Vec::new();
            let right_child = self.read_u64_data(INTERNAL_RIGHT_MOST_CHILD_OFFSET, true);
            debug!(
                "setting new internal cell right child pointer {}",
                right_child
            );

            self.write_all_bytes(
                cell.get_content()[INTERNAL_KEY_POINTER_OFFSET
                    ..INTERNAL_KEY_POINTER_SIZE + INTERNAL_KEY_POINTER_OFFSET]
                    .to_vec(),
                INTERNAL_RIGHT_MOST_CHILD_OFFSET,
            );

            if right_child == 0 {
                return Ok(());
            }

            bytes.append(&mut cell.get_key().to_be_bytes().to_vec());
            bytes.append(&mut right_child.to_be_bytes().to_vec());
        } else {
            bytes = cell.get_content();
        }

        let pos = self.calculate_cell_position(cell_num) as usize;
        debug!("inserting new internal cell at {}; key {}", pos, key);

        let free_space_start = if self.num_cells() > 0 {
            self.num_cells() as usize * INTERNAL_CELL_SIZE + INTERNAL_HEADER_SIZE
        } else {
            INTERNAL_HEADER_SIZE
        };

        if free_space_start != pos {
            // Move cells to the right
            let keys_after_pos = self.read_variable_data(pos, free_space_start - pos, true);
            self.write_all_bytes(keys_after_pos, pos + INTERNAL_CELL_SIZE);
        }
        self.write_all_bytes(bytes, pos);

        let num_cells = self.num_cells() + 1;
        self.write_all_bytes(num_cells.to_be_bytes().to_vec(), INTERNAL_NUM_KEYS_OFFSET);

        debug!("key after insert: {}", self.read_u64_data(pos, true));
        debug!("has buffer: {:?}", self.buffer);

        Ok(())
    }

    fn insert_leaf_cell<T: Cell>(&mut self, cell: T) -> Result<()> {
        let mut free_space_start = self.read_u64_data(LEAF_FREE_SPACE_START_OFFSET, true);
        let mut free_space_end = self.read_u64_data(LEAF_FREE_SPACE_END_OFFSET, true);

        let key_pos = self.calculate_cell_position(self.find_cell_num(cell.get_key()));
        let mut content = cell.get_content();
        let mut content_bytes = Vec::new();
        content_bytes.append(&mut content.len().to_be_bytes().to_vec());
        content_bytes.append(&mut content);

        free_space_end -= content_bytes.len() as u64;

        if free_space_start + LEAF_KEY_CELL_SIZE as u64 >= free_space_end {
            // TODO: Need to figure out how to handle overflow pages
            return Err(NodeResult::HasOverflow(Vec::with_capacity(0)));
        }

        debug!(
            "inserting new leaf cell at {}; identifier {}",
            free_space_end,
            cell.get_key()
        );

        let mut key_bytes = cell.get_key_bytes();
        key_bytes.append(&mut free_space_end.to_be_bytes().to_vec());

        // Move key cells
        if key_pos < free_space_start {
            let keys_after_cell = self.read_variable_data(
                key_pos as usize,
                (free_space_start - key_pos) as usize,
                true,
            );
            self.write_all_bytes(keys_after_cell, key_pos as usize + LEAF_KEY_CELL_SIZE);
        }
        free_space_start += LEAF_KEY_CELL_SIZE as u64;

        self.write_all_bytes(key_bytes, key_pos as usize);
        self.write_all_bytes(content_bytes, free_space_end as usize);

        self.write_all_bytes(
            free_space_start.to_be_bytes().to_vec(),
            LEAF_FREE_SPACE_START_OFFSET,
        );
        self.write_all_bytes(
            free_space_end.to_be_bytes().to_vec(),
            LEAF_FREE_SPACE_END_OFFSET,
        );
        debug!(
            "new start: {}, new end: {}",
            free_space_start, free_space_end,
        );
        let num_cells = self.num_cells() + 1;
        self.write_all_bytes(num_cells.to_be_bytes().to_vec(), LEAF_NUM_KEYS_OFFSET);

        Ok(())
    }

    /// Reads u64 numbers from the attached page.
    ///
    /// The `u64` number bytes are read in big-endian format
    fn read_u64_data(&self, start: usize, buffered: bool) -> u64 {
        let size = size_of::<usize>();
        let (start, end) = calculate_offsets!(start, size);

        if buffered && self.buffer.is_some() {
            let buf = self.buffer.as_ref().expect("buffer should be set");
            u64::from_be_bytes(buf[start..end].try_into().expect("failed to read u64 data"))
        } else {
            let page = Arc::clone(&self.page.0);
            debug!("Acquiring read lock on page");
            let handle = page.read().expect("failed to retrieve read lock on page");

            u64::from_be_bytes(
                handle[start..end]
                    .try_into()
                    .expect("failed to read u64 data"),
            )
        }
    }

    /// Reads variable length data from the attached page.
    ///
    fn read_variable_data(&self, start: usize, size: usize, buffered: bool) -> Vec<u8> {
        let (start, end) = calculate_offsets!(start, size);

        if buffered && self.buffer.is_some() {
            let buf = self.buffer.as_ref().expect("buffer should be set");
            buf[start..end].into()
        } else {
            let page = Arc::clone(&self.page.0);
            debug!("Acquiring read lock on page");
            let handle = page.read().expect("failed to retrieve read lock on page");

            handle[start..end].into()
        }
    }

    fn set_buffer(&mut self) {
        self.buffer = Some(Page(
            self.read_variable_data(0, PAGE_SIZE, false)[..]
                .try_into()
                .expect("failed to create temporary buffer"),
        ));
    }

    /// Splits a full internal node
    ///
    fn split_internal_node<T: Cell>(&mut self, node: &mut Node, cell: T) -> Result<()> {
        todo!()
    }

    /// Splits a full leaf node
    ///
    fn split_leaf_node<T: Cell>(&mut self, node: &mut Node, new_cell: T) -> Result<()> {
        let cells = self.num_cells() + 1;
        let new_cell_num = self.find_cell_num(new_cell.get_key());
        let right_split_count = cells / 2;
        let left_split_count = cells - right_split_count;
        self.write_all_bytes(
            LEAF_HEADER_SIZE.to_be_bytes().to_vec(),
            LEAF_FREE_SPACE_START_OFFSET,
        );
        self.write_all_bytes(PAGE_SIZE.to_be_bytes().to_vec(), LEAF_FREE_SPACE_END_OFFSET);
        self.write_all_bytes(0_u64.to_be_bytes().to_vec(), LEAF_NUM_KEYS_OFFSET);

        for i in (0..cells).rev() {
            let destination: &mut Self;
            let mut cell: LeafCell = Default::default();

            if i == new_cell_num {
                let mut content = new_cell.get_key().to_be_bytes().to_vec();
                content.append(&mut new_cell.get_content());
                cell.from_bytes(content);
            } else if i > new_cell_num {
                let pos = self.calculate_cell_position(i - 1);
                let key = self.get_cell_key(pos, false);
                let pointer = self.get_cell_key_pointer(pos, false) as usize;

                let content_size = self.read_u64_data(pointer, false) as usize;
                let mut content_bytes =
                    self.read_variable_data(pointer + LEAF_CONTENT_LEN_SIZE, content_size, false);

                let mut cell_bytes = key.to_be_bytes().to_vec();
                cell_bytes.append(&mut content_bytes);
                cell.from_bytes(cell_bytes);
            } else {
                let pos = self.calculate_cell_position(i);
                let key = self.get_cell_key(pos, false);
                let pointer = self.get_cell_key_pointer(pos, false) as usize;

                let content_size = self.read_u64_data(pointer, false) as usize;
                let mut content_bytes =
                    self.read_variable_data(pointer + LEAF_CONTENT_LEN_SIZE, content_size, false);

                let mut cell_bytes = key.to_be_bytes().to_vec();
                cell_bytes.append(&mut content_bytes);
                cell.from_bytes(cell_bytes);
            }

            if i >= left_split_count {
                destination = node;
            } else {
                destination = self;
            }

            destination.insert_leaf_cell(cell)?;
        }

        self.write_all_bytes(
            left_split_count.to_be_bytes().to_vec(),
            LEAF_NUM_KEYS_OFFSET,
        );
        node.write_all_bytes(
            right_split_count.to_be_bytes().to_vec(),
            LEAF_NUM_KEYS_OFFSET,
        );

        Ok(())
    }

    /// Writes data to the attached page
    ///
    fn write_all_bytes(&mut self, bytes: Vec<u8>, start: usize) {
        if let Some(buf) = self.buffer.as_mut() {
            let end = bytes.len() + start;
            buf[start..end].clone_from_slice(&bytes)
        } else {
            let page = Arc::clone(&self.page.0);
            let mut handle = page.write().expect("failed to retrieve write lock on page");

            let end = bytes.len() + start;
            handle[start..end].clone_from_slice(&bytes)
        }
    }
}
