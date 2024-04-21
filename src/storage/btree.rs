use core::panic;
use std::{
    fmt::Display,
    mem::size_of,
    sync::{Arc, RwLockWriteGuard},
};

use log::debug;

use crate::{
    calculate_offsets,
    storage::layout::{
        INTERNAL_CELL_SIZE, INTERNAL_KEY_SIZE, INTERNAL_MAX_KEYS, INTERNAL_NUM_KEYS_OFFSET,
        INTERNAL_NUM_KEYS_SIZE, LEAF_FREE_SPACE_END_OFFSET, LEAF_FREE_SPACE_END_SIZE,
        LEAF_FREE_SPACE_START_OFFSET, LEAF_FREE_SPACE_START_SIZE, LEAF_KEY_IDENTIFIER_SIZE,
        LEAF_KEY_INDENTIFIER_OFFSET, LEAF_NEXT_SIBLING_POINTER_DEFAULT,
        LEAF_NEXT_SIBLING_POINTER_OFFSET, LEAF_NEXT_SIBLING_POINTER_SIZE, LEAF_NUM_KEYS_OFFSET,
        LEAF_NUM_KEYS_SIZE, PAGE_SIZE,
    },
};

use super::{
    cell::Cell,
    layout::{
        INTERNAL_HEADER_SIZE, INTERNAL_KEY_OFFSET, INTERNAL_KEY_POINTER_OFFSET,
        INTERNAL_KEY_POINTER_SIZE, LEAF_CONTENT_LEN_SIZE, LEAF_HEADER_SIZE, LEAF_KEY_CELL_SIZE,
        LEAF_KEY_POINTER_OFFSET, LEAF_KEY_POINTER_SIZE, PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE,
    },
    page::{CachedPage, Page, PageType},
};

type Result<T> = std::result::Result<T, NodeResult>;

/// Possible result types that can be returned by [Node](Node) operations
#[derive(Debug, Clone)]
pub enum NodeResult {
    /// Returned when a node is full and requires a split action to be performed
    IsFull,
    /// Returned when a node has no space for content and requires an overflow page to insert data
    HasOverflowen(u64, Vec<u8>),
    /// Returned when trying to read a node with invalid page content
    InvalidPage { desc: String },
    /// Returned when trying to insert a duplicate key
    DuplicateKey,
}

impl Display for NodeResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            Self::IsFull => "node is currently full".to_string(),
            Self::HasOverflowen(_, _) => "node has overflowen".to_string(),
            Self::InvalidPage { desc } => format!("invalid page; {desc}"),
            Self::DuplicateKey => "duplicate key".to_string(),
        };

        write!(f, "{}", msg)
    }
}

// In-memory representation of a page.
//
// This structure is used to manipulate page contents in memory
pub struct Node {
    page: CachedPage,
    overflow_pages: Vec<CachedPage>,
    keys: u64,
    _type: PageType,
}

impl Node {
    pub fn load(page: CachedPage, overflow_pages: Vec<CachedPage>) -> Result<Self> {
        let mut obj = Self {
            page,
            overflow_pages,
            keys: 0,
            _type: PageType::Leaf,
        };

        obj._type = obj.read_variable_data(PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE)[0]
            .try_into()
            .map_err(|e| NodeResult::InvalidPage {
                desc: format!("error while reading page type; {}", e),
            })?;
        obj.keys = obj.num_cells();

        Ok(obj)
    }

    pub fn node_type(&self) -> &PageType {
        &self._type
    }

    pub fn next_sibling(&self) -> Option<u64> {
        if self._type == PageType::Internal {
            panic!("internal pages do not support next sibling headers");
        } else {
            match self.read_u64_data(LEAF_NEXT_SIBLING_POINTER_OFFSET) {
                LEAF_NEXT_SIBLING_POINTER_DEFAULT => None,
                v => Some(v),
            }
        }
    }

    pub fn set_next_sibling(&self, pointer: u64) {
        let (start, end) = calculate_offsets!(
            LEAF_NEXT_SIBLING_POINTER_OFFSET,
            LEAF_NEXT_SIBLING_POINTER_SIZE
        );
        let page = Arc::clone(&self.page.0);
        let mut handle = page.write().expect("failed to retrieve write lock on page");
        handle[start..end].clone_from_slice(&pointer.to_be_bytes());
    }

    pub fn num_cells(&self) -> u64 {
        match self._type {
            PageType::Leaf => self.read_u64_data(LEAF_NUM_KEYS_OFFSET),
            PageType::Internal => self.read_u64_data(INTERNAL_NUM_KEYS_OFFSET),
        }
    }

    pub fn insert_cell<T: Cell>(&mut self, cell: T) -> Result<()> {
        if self.check_key_exists(cell.get_key()) {
            return Err(NodeResult::DuplicateKey);
        }

        debug!("inserting new cell");
        let num_cells = self.num_cells() + 1;
        let page = Arc::clone(&self.page.0);
        let mut handle = page.write().expect("failed to retrieve write lock on page");
        let start;
        let end;

        match self._type {
            PageType::Internal => {
                self.insert_internal_cell(cell, &mut handle)?;
                (start, end) = calculate_offsets!(INTERNAL_NUM_KEYS_OFFSET, INTERNAL_NUM_KEYS_SIZE);
            }
            PageType::Leaf => {
                self.insert_leaf_cell(cell, &mut handle)?;
                (start, end) = calculate_offsets!(LEAF_NUM_KEYS_OFFSET, LEAF_NUM_KEYS_SIZE);
            }
        };

        handle[start..end].clone_from_slice(&num_cells.to_be_bytes());

        Ok(())
    }

    pub fn read_cell_bytes(&self, num: u64) -> Vec<u8> {
        let cell_pos = self.calculate_cell_position(num) as usize;

        match self._type {
            PageType::Internal => self.read_variable_data(cell_pos, INTERNAL_CELL_SIZE),
            PageType::Leaf => {
                let mut pointer = self.get_cell_key_pointer(cell_pos as u64) as usize;
                let content_size = self.read_u64_data(pointer);
                pointer += LEAF_CONTENT_LEN_SIZE;

                self.read_variable_data(pointer, content_size as usize)
            }
        }
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

        self.get_cell_key(pos) == key
    }

    fn get_cell_key(&self, pos: u64) -> u64 {
        let start_pos = match self._type {
            PageType::Leaf => LEAF_KEY_INDENTIFIER_OFFSET + pos as usize,
            PageType::Internal => INTERNAL_KEY_OFFSET + pos as usize,
        };

        self.read_u64_data(start_pos)
    }

    fn get_cell_key_pointer(&self, pos: u64) -> u64 {
        let start_pos = match self._type {
            PageType::Leaf => LEAF_KEY_POINTER_OFFSET + pos as usize,
            PageType::Internal => INTERNAL_KEY_POINTER_OFFSET + pos as usize,
        };

        self.read_u64_data(start_pos)
    }

    fn find_cell_num(&self, key: u64) -> u64 {
        let num_cells = self.num_cells();
        let mut min_idx = 0;
        let mut max_idx = self.num_cells();

        match self._type {
            PageType::Leaf => {
                while min_idx != max_idx {
                    let index = (min_idx + max_idx) / 2;
                    let key_at_index = self.get_cell_key(self.calculate_cell_position(index));

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
                    let key_at_right = self.get_cell_key(self.calculate_cell_position(index));

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

    fn insert_internal_cell<T: Cell>(
        &mut self,
        cell: T,
        handle: &mut RwLockWriteGuard<'_, Page>,
    ) -> Result<()> {
        if self.num_cells() > INTERNAL_MAX_KEYS as u64 {
            return Err(NodeResult::IsFull);
        }

        let key = cell.get_key();
        let bytes: [u8; INTERNAL_CELL_SIZE] =
            cell.get_content()[..]
                .try_into()
                .map_err(|_| NodeResult::InvalidPage {
                    desc: "invalid internal cell data".to_string(),
                })?;

        let pos = self.calculate_cell_position(self.find_cell_num(key));
        debug!("inserting new internal cell at {}; key {}", pos, key);
        let mut buf = handle.0.to_vec();
        let mut after_cell = buf.split_off(pos as usize);

        buf.append(&mut bytes.to_vec());
        buf.append(&mut after_cell);

        handle.0.clone_from_slice(&buf[..PAGE_SIZE]);
        Ok(())
    }

    fn insert_leaf_cell<T: Cell>(
        &mut self,
        cell: T,
        handle: &mut RwLockWriteGuard<'_, Page>,
    ) -> Result<()> {
        let free_space_start = self.read_u64_data(LEAF_FREE_SPACE_START_OFFSET);
        let mut free_space_end = self.read_u64_data(LEAF_FREE_SPACE_END_OFFSET);

        let mut content = cell.get_content();
        let mut content_bytes = Vec::new();
        content_bytes.append(&mut content.len().to_be_bytes().to_vec());
        content_bytes.append(&mut content);

        free_space_end -= content_bytes.len() as u64;

        if free_space_start + LEAF_KEY_CELL_SIZE as u64 >= free_space_end {
            // TODO: differentiate between when a leaf node has overflow or has become full
            return Err(NodeResult::IsFull);
        }

        debug!(
            "inserting new leaf cell at {}; identifier {}",
            free_space_end,
            cell.get_key()
        );

        let mut key_bytes = cell.get_key_bytes();
        key_bytes.append(&mut free_space_end.to_be_bytes().to_vec());
        let key_end = free_space_start + LEAF_KEY_CELL_SIZE as u64;

        handle[free_space_start as usize..key_end as usize].clone_from_slice(&key_bytes[..]);
        handle[free_space_end as usize..free_space_end as usize + content_bytes.len()]
            .clone_from_slice(&content_bytes[..]);

        let (start, end) =
            calculate_offsets!(LEAF_FREE_SPACE_START_OFFSET, LEAF_FREE_SPACE_START_SIZE);
        handle[start..end].clone_from_slice(&key_end.to_be_bytes());
        let (start, end) = calculate_offsets!(LEAF_FREE_SPACE_END_OFFSET, LEAF_FREE_SPACE_END_SIZE);
        handle[start..end].clone_from_slice(&free_space_end.to_be_bytes());

        Ok(())
    }

    /// Reads u64 numbers from the attached page.
    ///
    /// The `u64` number bytes are read in big-endian format
    fn read_u64_data(&self, start: usize) -> u64 {
        let size = size_of::<usize>();
        let (start, end) = calculate_offsets!(start, size);
        let page = Arc::clone(&self.page.0);
        debug!("Acquiring read lock on page");
        let handle = page.read().expect("failed to retrieve read lock on page");

        u64::from_be_bytes(
            handle[start..end]
                .try_into()
                .expect("failed to read u64 data"),
        )
    }

    /// Reads variable length data from the attached page.
    ///
    fn read_variable_data(&self, start: usize, size: usize) -> Vec<u8> {
        let (start, end) = calculate_offsets!(start, size);
        let page = Arc::clone(&self.page.0);
        debug!("Acquiring read lock on page");
        let handle = page.read().expect("failed to retrieve read lock on page");

        handle[start..end].into()
    }
}
