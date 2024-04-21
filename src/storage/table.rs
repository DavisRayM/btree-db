use super::{page::CachedPage, pager::Pager};
use std::path::PathBuf;

/// Table is a wrapper around B+-Trees
///
/// Table wraps a B+-Tree structure and provides functionality to retrieve specific pages in the
/// tree as well as functionality to modify the structure of the tree
pub struct Table {
    pager: Pager,
    root: u64,
}

impl Table {
    /// Creates a new Table wrapper on an existing/new B+-Tree structure on-disk
    pub fn new(file_path: PathBuf) -> Self {
        let pager = Pager::new(file_path);

        Self {
            root: pager.root_page(),
            pager,
        }
    }

    /// Retrieves a particular page in the table
    pub fn get_page(&mut self, num: u64) -> Option<CachedPage> {
        self.pager.get_page(num)
    }

    pub fn root_page(&mut self) -> CachedPage {
        self.pager
            .get_page(self.root)
            .expect("failed to retrieve root page")
    }

    pub fn flush_contents(&mut self) {
        self.pager.flush_cache();
    }
}
