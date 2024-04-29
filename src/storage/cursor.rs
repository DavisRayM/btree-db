use log::debug;

use super::{
    btree::{Node, NodeResult},
    cell::{Cell, InternalCell, LeafCell},
    layout::LEAF_KEY_POINTER_SIZE,
    page::PageType,
    table::Table,
};

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
    table: &'a mut Table,
    cell_num: u64,
    node: Node,
    _state: CursorState,
    // Stores the parent cell number and page number for easy traversal
    page_breadcrumb: Vec<(u64, u64)>,
}

impl<'a> Cursor<'a> {
    /// Create a new cursor object for a Table
    pub fn new(table: &'a mut Table) -> Self {
        let node = Node::load(table.root_page()).expect("failed to load root node");

        let _state = match node.num_cells() {
            0 => CursorState::AtEnd,
            _ => CursorState::AtStart,
        };

        Self {
            page_breadcrumb: vec![(0, table.root)],
            table,
            cell_num: 0,
            node,
            _state,
        }
    }

    /// Inserts a new record into the table
    ///
    pub fn insert(&mut self, identifier: u64, content: Vec<u8>) -> Result<(), String> {
        match self.node.node_type() {
            PageType::Leaf => {
                let cell = LeafCell::new(identifier, content.clone(), false);
                let result = self.node.insert_cell(cell);
                match result {
                    Ok(_) => Ok(()),
                    Err(NodeResult::IsFull) => self.split(identifier, content),
                    Err(e) => Err(e.to_string()),
                }
            }
            PageType::Internal => {
                self.find_node(identifier);
                self.insert(identifier, content)
            }
        }
    }

    /// Selects all records from the linked table.
    ///
    pub fn select(&mut self) -> Vec<String> {
        let mut data = Vec::new();
        while self.node.node_type() != PageType::Leaf {
            debug!("searching for leaf node");
            self.find_node(0);
        }

        while self._state != CursorState::AtEnd {
            if self._state != CursorState::InProgress {
                self._state = CursorState::InProgress;
            }

            data.push(String::from_utf8(self.node.read_cell_bytes(self.cell_num)).unwrap());
            self.advance();
        }

        data
    }

    fn advance(&mut self) {
        self.cell_num += 1;
        if self.node.num_cells() <= self.cell_num {
            debug!("cursor at the end; sibling {:?}", self.node.next_sibling());
            if let Some(sibling) = self.node.next_sibling() {
                self.node = Node::load(
                    self.table
                        .get_page(sibling)
                        .expect("sibling does not exist"),
                )
                .expect("failed to load next sibling");
                self.cell_num = 0;
            } else {
                self._state = CursorState::AtEnd;
            }
        }
    }

    fn find_node(&mut self, identifier: u64) {
        let cell_num = self.node.find_cell_num(identifier);
        let key_data = self.node.read_cell_bytes(cell_num);
        let mut cell = InternalCell::default();
        cell.from_bytes(key_data);
        debug!("loading found page: {}", cell.pointer());
        self.page_breadcrumb.push((cell_num, cell.pointer()));
        self.node = Node::load(self.table.get_page(cell.pointer()).unwrap()).unwrap();
        debug!("current breadcrumbs: {:?}", self.page_breadcrumb);
    }

    fn split(&mut self, identifier: u64, content: Vec<u8>) -> Result<(), String> {
        debug!("splitting current node: {:?}", self.page_breadcrumb.last());
        let (new_page, page) = self.table.create_page(&self.node.node_type());
        let mut new_node =
            Node::load(page).map_err(|e| format!("failed to split node: {}", e.to_string()))?;
        let old_max = self.node.node_high_key();

        match self.node.node_type() {
            PageType::Leaf => {
                let cell = LeafCell::new(identifier, content.clone(), false);
                self.node
                    .split(&mut new_node, cell)
                    .map_err(|e| format!("failed to split leaf node; {}", e))?;
            }
            PageType::Internal => {
                let cell = InternalCell::new(
                    identifier,
                    content[..LEAF_KEY_POINTER_SIZE].try_into().unwrap(),
                );
                self.node
                    .split(&mut new_node, cell)
                    .map_err(|e| format!("failed to split internal node; {}", e))?;
            }
        };

        self.node.set_next_sibling(new_page);
        if self.node.is_root() {
            debug!("split node was root; creating new root");
            let (old_num, _) = self.table.create_new_root();
            self.node = Node::load(self.table.root_page()).unwrap();
            debug!(
                "inserting old root as cell key {} for split page {}",
                old_max, old_num
            );
            self.node
                .insert_cell(InternalCell::new(1, old_num.to_be_bytes()))
                .expect("failed to insert key into new internal node");
            debug!(
                "inserting new page as cell key {} for split page {}",
                old_max, new_page
            );
            self.node
                .insert_cell(InternalCell::new(old_max, new_page.to_be_bytes()))
                .expect("failed to insert right most key in internal node");
        } else {
            debug!("split node was child; updating page pointers");
            let (cell_num, cur_page) = self.page_breadcrumb.pop().expect("current page is unknown");
            let (_, parent_page) = self
                .page_breadcrumb
                .last()
                .expect("parent page not present");

            let max_key = self.node.node_high_key();
            let new_page_max = new_node.node_high_key();

            self.node = Node::load(
                self.table
                    .get_page(*parent_page)
                    .expect("expected parent page to exist"),
            )
            .expect("failed to retrieve parent page");

            let key_data = self.node.read_cell_bytes(cell_num);
            let mut cell = InternalCell::default();
            cell.from_bytes(key_data);

            if cur_page != self.node.right_child().unwrap() {
                debug!(
                    "updating old cell key {} to {} for page {}",
                    cell.get_key(),
                    max_key,
                    cell.pointer(),
                );
                // TODO: Handle parent node overflow
                self.node
                    .update(
                        cell.get_key(),
                        InternalCell::new(max_key, cur_page.to_be_bytes()),
                    )
                    .map_err(|e| format!("failed to update parent node pointer; {e}"))?;
            }
            debug!(
                "inserting new cell key {} for split page {}",
                new_page_max, new_page
            );

            let cell = InternalCell::new(new_page_max, new_page.to_be_bytes());
            match self.node.insert_cell(cell) {
                Ok(()) => (),
                Err(NodeResult::IsFull) => {
                    return self.split(new_page_max, new_page.to_be_bytes().to_vec());
                }
                Err(e) => return Err(format!("failed to split parent node: {}", e)),
            }
        }

        Ok(())
    }
}
