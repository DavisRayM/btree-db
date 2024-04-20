use log::debug;

use super::{btree::Node, cell::LeafCell, page::PageType, table::Table};

#[derive(Debug, Clone, PartialEq)]
pub enum CursorState {
    AtEnd,
    AtStart,
    InProgress,
}

/// Traversal mechanism for a tree structure.
///
/// This type provides the functionality to retrieve, add and remove data from a Table.
pub struct Cursor<'a> {
    // TODO: Support multi-threading
    table: &'a mut Table,
    cell_num: u64,
    node: Node,
    _state: CursorState,
}

impl<'a> Cursor<'a> {
    /// Create a new cursor object for a Table
    pub fn new(table: &'a mut Table) -> Self {
        let node =
            Node::load(table.root_page(), Vec::with_capacity(0)).expect("failed to load root node");

        let _state = match node.num_cells() {
            0 => CursorState::AtEnd,
            _ => CursorState::AtStart,
        };

        Self {
            table,
            cell_num: 0,
            node,
            _state,
        }
    }

    fn advance(&mut self) {
        self.cell_num += 1;
        if self.node.num_cells() <= self.cell_num {
            if let Some(sibling) = self.node.next_sibling() {
                debug!("cursor moving to new sibling node: {}", sibling);

                self.node = Node::load(
                    self.table
                        .get_page(sibling)
                        .expect("sibling does not exist"),
                    Vec::with_capacity(0),
                )
                .expect("failed to load next sibling");
                self.cell_num = 0;
            } else {
                self._state = CursorState::AtEnd;
            }
        }
    }

    /// Inserts a new record into the table
    ///
    pub fn insert(&mut self, identifier: u64, content: &String) -> Result<(), String> {
        match self.node.node_type() {
            PageType::Leaf => {
                let cell = LeafCell::new(identifier, content.as_bytes().to_vec(), false);
                self.node.insert_cell(cell).map_err(|e| e.to_string())
            }
            PageType::Internal => {
                debug!("locating leaf node to insert identifier: {}", identifier);
                todo!()
            }
        }
    }

    /// Selects all records from the linked table.
    ///
    pub fn select(&mut self) -> Vec<String> {
        let mut data = Vec::new();

        while self._state != CursorState::AtEnd {
            if self._state != CursorState::InProgress {
                self._state = CursorState::InProgress;
            }

            data.push(String::from_utf8(self.node.read_cell_bytes(self.cell_num)).unwrap());
            self.advance();
        }

        data
    }
}
