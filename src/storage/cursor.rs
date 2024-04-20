use super::{btree::Node, cell::LeafCell, table::Table};

#[derive(Debug, Clone, PartialEq)]
pub enum CursorState {
    AtEnd,
    AtStart,
    InProgress,
}

pub struct Cursor<'a> {
    table: &'a mut Table,
    cell_num: u64,
    node: Node,
    _state: CursorState,
}

impl<'a> Cursor<'a> {
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

    pub fn advance(&mut self) {
        self.cell_num += 1;
        if self.node.num_cells() <= self.cell_num {
            self._state = CursorState::AtEnd;
        }
    }

    /// TODO: This only handles leaf cell inserts; needs to handle internal node key inserts too
    pub fn insert(&mut self, identifier: u64, content: &String) -> Result<(), String> {
        let cell = LeafCell::new(identifier, content.as_bytes().to_vec(), false);
        self.node.insert_cell(cell)
    }

    /// TODO: At the moment we only store string data
    pub fn select(&mut self) -> Vec<String> {
        let mut data = Vec::new();

        while self._state == CursorState::AtStart || self._state == CursorState::InProgress {
            self._state = CursorState::InProgress;
            data.push(String::from_utf8(self.node.read_cell_bytes(self.cell_num)).unwrap());
            self.advance();
        }

        data
    }
}
