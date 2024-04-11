use super::page::{CachedPage, PageType};

// In-memory representation of a page.
//
// This structure is used to manipulate page contents in memory
pub struct Node {
    page: CachedPage,
    overflow_pages: Vec<CachedPage>,
    keys: usize,
    _type: PageType,
}
