use super::{page::CachedPage, pager::Pager};
use std::path::PathBuf;

pub struct Table {
    pager: Pager,
    root: u64,
}

impl Table {
    pub fn new(file_path: PathBuf) -> Self {
        let pager = Pager::new(file_path);

        Self {
            root: pager.root_page(),
            pager,
        }
    }

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
