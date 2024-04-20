use std::sync::{Arc, RwLockWriteGuard};

use log::debug;

use crate::{
    calculate_offsets,
    storage::layout::{
        INTERNAL_CELL_SIZE, INTERNAL_KEY_SIZE, INTERNAL_MAX_KEYS, INTERNAL_NUM_KEYS_OFFSET,
        INTERNAL_NUM_KEYS_SIZE, LEAF_FREE_SPACE_END_OFFSET, LEAF_FREE_SPACE_END_SIZE,
        LEAF_FREE_SPACE_START_OFFSET, LEAF_FREE_SPACE_START_SIZE, LEAF_KEY_IDENTIFIER_SIZE,
        LEAF_KEY_INDENTIFIER_OFFSET, LEAF_NUM_KEYS_OFFSET, LEAF_NUM_KEYS_SIZE, PAGE_SIZE,
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
    pub fn load(page: CachedPage, overflow_pages: Vec<CachedPage>) -> Result<Self, String> {
        let mut obj = Self {
            page,
            overflow_pages,
            keys: 0,
            _type: PageType::Leaf,
        };

        obj._type = obj.read_page_type()?;
        obj.keys = obj.num_cells();

        Ok(obj)
    }

    fn read_page_type(&self) -> Result<PageType, String> {
        let (start, end) = calculate_offsets!(PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE);
        let page = Arc::clone(&self.page.0);
        let handle = page.read().expect("failed to retrieve read lock on page");

        handle[start..end][0].try_into()
    }

    /// Retrieve the cell position for an Internal node key or Leaf node key
    fn calculate_cell_position(&self, num: u64) -> u64 {
        match self._type {
            PageType::Leaf => LEAF_HEADER_SIZE as u64 + (num * LEAF_KEY_CELL_SIZE as u64),
            PageType::Internal => INTERNAL_HEADER_SIZE as u64 + (num * INTERNAL_CELL_SIZE as u64),
        }
    }

    fn get_cell_key(&self, pos: u64) -> u64 {
        let start;
        let end;
        match self._type {
            PageType::Leaf => {
                let start_pos = LEAF_KEY_INDENTIFIER_OFFSET + pos as usize;
                (start, end) = calculate_offsets!(start_pos, LEAF_KEY_IDENTIFIER_SIZE);
            }
            PageType::Internal => {
                let start_pos = INTERNAL_KEY_OFFSET + pos as usize;
                (start, end) = calculate_offsets!(start_pos, INTERNAL_KEY_SIZE);
            }
        }

        let page = Arc::clone(&self.page.0);
        let handle = page.read().expect("failed to retrieve read lock on page");
        u64::from_be_bytes(
            handle[start..end]
                .try_into()
                .expect("failed to read cell key"),
        )
    }

    fn get_cell_key_pointer(&self, pos: u64) -> u64 {
        let start;
        let end;
        match self._type {
            PageType::Leaf => {
                let start_pos = LEAF_KEY_POINTER_OFFSET + pos as usize;
                (start, end) = calculate_offsets!(start_pos, LEAF_KEY_POINTER_SIZE);
            }
            PageType::Internal => {
                let start_pos = INTERNAL_KEY_POINTER_OFFSET + pos as usize;
                (start, end) = calculate_offsets!(start_pos, INTERNAL_KEY_POINTER_SIZE);
            }
        }

        let page = Arc::clone(&self.page.0);
        let handle = page.read().expect("failed to retrieve read lock on page");
        u64::from_be_bytes(
            handle[start..end]
                .try_into()
                .expect("failed to read cell key"),
        )
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
    ) -> Result<(), String> {
        if self.num_cells() > INTERNAL_MAX_KEYS as u64 {
            return Err("page is full; need to implement split".to_string());
        }

        let key = cell.get_key();
        let bytes: [u8; INTERNAL_CELL_SIZE] = cell.get_content()[..]
            .try_into()
            .map_err(|_| "invalid internal cell data".to_string())?;

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
    ) -> Result<(), String> {
        let (start, end) =
            calculate_offsets!(LEAF_FREE_SPACE_START_OFFSET, LEAF_FREE_SPACE_START_SIZE);
        let free_space_start = u64::from_be_bytes(
            handle[start..end]
                .try_into()
                .expect("failed to read free space header"),
        );
        let (start, end) = calculate_offsets!(LEAF_FREE_SPACE_END_OFFSET, LEAF_FREE_SPACE_END_SIZE);
        let mut free_space_end = u64::from_be_bytes(
            handle[start..end]
                .try_into()
                .expect("failed to read free space header"),
        );

        let mut content = cell.get_content();
        let mut content_bytes = Vec::new();
        content_bytes.append(&mut content.len().to_be_bytes().to_vec());
        content_bytes.append(&mut content);

        free_space_end -= content_bytes.len() as u64;

        if free_space_start + LEAF_KEY_CELL_SIZE as u64 >= free_space_end {
            return Err(
                "page is full; need to implement overflow pages and page compaction".to_string(),
            );
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

    pub fn node_type(&self) -> &PageType {
        &self._type
    }

    pub fn num_cells(&self) -> u64 {
        let page = Arc::clone(&self.page.0);
        let handle = page.read().expect("failed to retrieve read lock on page");
        let start;
        let end;

        match self._type {
            PageType::Leaf => {
                (start, end) = calculate_offsets!(LEAF_NUM_KEYS_OFFSET, LEAF_NUM_KEYS_SIZE);
            }
            PageType::Internal => {
                (start, end) = calculate_offsets!(INTERNAL_NUM_KEYS_OFFSET, INTERNAL_NUM_KEYS_SIZE);
            }
        };

        u64::from_be_bytes(
            handle[start..end]
                .try_into()
                .expect("failed to read num keys bytes"),
        )
    }

    pub fn insert_cell<T: Cell>(&mut self, cell: T) -> Result<(), String> {
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
        let page = Arc::clone(&self.page.0);
        let handle = page.read().expect("failed to retrieve read lock on page");
        let start;
        let end;

        match self._type {
            PageType::Internal => {
                (start, end) = calculate_offsets!(cell_pos, INTERNAL_CELL_SIZE);
            }
            PageType::Leaf => {
                let pointer = self.get_cell_key_pointer(cell_pos as u64) as usize;
                let content_size_end = pointer + LEAF_CONTENT_LEN_SIZE;
                let content_size = usize::from_be_bytes(
                    handle[pointer..content_size_end]
                        .try_into()
                        .expect("failed to read content size metadata"),
                );

                start = content_size_end;
                end = start + content_size;
            }
        };

        debug!("reading cell at {} - {}", start, end);
        handle[start..end].to_vec()
    }
}
