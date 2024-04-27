use std::sync::{Arc, RwLock};

use crate::calculate_offsets;

use super::layout::{
    LEAF_FREE_SPACE_END_OFFSET, LEAF_FREE_SPACE_END_SIZE, LEAF_FREE_SPACE_START_OFFSET,
    LEAF_FREE_SPACE_START_SIZE, LEAF_HEADER_SIZE, LEAF_NEXT_SIBLING_POINTER_DEFAULT,
    LEAF_NEXT_SIBLING_POINTER_OFFSET, LEAF_NEXT_SIBLING_POINTER_SIZE,
    LEAF_OVERFLOW_POINTER_DEFAULT, LEAF_OVERFLOW_POINTER_OFFSET, LEAF_OVERFLOW_POINTER_SIZE,
    PAGE_IS_ROOT_OFFSET, PAGE_IS_ROOT_SIZE, PAGE_MAGIC, PAGE_MAGIC_OFFSET, PAGE_MAGIC_SIZE,
    PAGE_SIZE, PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE,
};

/// On-disk structure for storing and organizing records
#[derive(Debug, Clone)]
pub struct Page(pub [u8; PAGE_SIZE]);

/// Cached in-memory page
#[derive(Debug, Clone)]
pub struct CachedPage(pub Arc<RwLock<Page>>);

impl CachedPage {
    pub fn new(page: Page) -> Self {
        Self(Arc::new(RwLock::new(page)))
    }
}

impl<Idx> std::ops::Index<Idx> for Page
where
    Idx: std::slice::SliceIndex<[u8]>,
{
    type Output = Idx::Output;

    fn index(&self, index: Idx) -> &Self::Output {
        &self.0[index]
    }
}

impl<Idx> std::ops::IndexMut<Idx> for Page
where
    Idx: std::slice::SliceIndex<[u8]>,
{
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        &mut self.0[index]
    }
}

/// Type of page.
///
/// A page can be one of two types:
///
/// - `Internal`: An internal node within the B+-Tree structure. It acts as an index for the B+-Tree
/// - `Leaf`: An external node within the B+-Tree structure. These pages store the actual data
#[derive(Debug, Clone, PartialEq)]
pub enum PageType {
    Internal,
    Leaf,
}

impl Into<u8> for &PageType {
    fn into(self) -> u8 {
        match self {
            PageType::Leaf => 0xA,
            PageType::Internal => 0xB,
        }
    }
}

impl TryFrom<u8> for PageType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0xA => Ok(PageType::Leaf),
            0xB => Ok(PageType::Internal),
            v => Err(format!("unknown type: {:#x}", v)),
        }
    }
}

/// Builder struct for a page.
pub struct PageBuilder {
    inner: [u8; PAGE_SIZE],
    _type: PageType,
    content_set: bool,
}

impl PageBuilder {
    pub fn content(mut self, c: [u8; PAGE_SIZE]) -> Result<Self, String> {
        let (start, end) = calculate_offsets!(PAGE_MAGIC_OFFSET, PAGE_MAGIC_SIZE);
        let magic = usize::from_be_bytes(
            c[start..end]
                .try_into()
                .expect("failed to read page magic data"),
        );

        if magic != PAGE_MAGIC {
            Err("content is not a valid page".to_string())
        } else {
            self.inner = c;
            self.content_set = true;
            Ok(self)
        }
    }

    pub fn kind(mut self, _type: &PageType) -> Self {
        let (start, end) = calculate_offsets!(PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE);

        self.inner[start..end].clone_from_slice(&[_type.into()]);
        self._type = _type.clone();
        self
    }

    pub fn is_root(mut self, is_root: bool) -> Self {
        let (start, end) = calculate_offsets!(PAGE_IS_ROOT_OFFSET, PAGE_IS_ROOT_SIZE);

        self.inner[start..end].clone_from_slice(&[bool_to_u8(is_root)]);
        self
    }

    pub fn build(mut self) -> Page {
        let (start, end) = calculate_offsets!(PAGE_MAGIC_OFFSET, PAGE_MAGIC_SIZE);
        self.inner[start..end].clone_from_slice(PAGE_MAGIC.to_be_bytes().as_ref());

        if self._type == PageType::Leaf && !self.content_set {
            let (start, end) =
                calculate_offsets!(LEAF_FREE_SPACE_START_OFFSET, LEAF_FREE_SPACE_START_SIZE);
            self.inner[start..end].clone_from_slice(&LEAF_HEADER_SIZE.to_be_bytes());

            let (start, end) =
                calculate_offsets!(LEAF_FREE_SPACE_END_OFFSET, LEAF_FREE_SPACE_END_SIZE);
            self.inner[start..end].clone_from_slice(&PAGE_SIZE.to_be_bytes());

            let (start, end) = calculate_offsets!(
                LEAF_NEXT_SIBLING_POINTER_OFFSET,
                LEAF_NEXT_SIBLING_POINTER_SIZE
            );
            self.inner[start..end]
                .clone_from_slice(&LEAF_NEXT_SIBLING_POINTER_DEFAULT.to_be_bytes());

            let (start, end) =
                calculate_offsets!(LEAF_OVERFLOW_POINTER_OFFSET, LEAF_OVERFLOW_POINTER_SIZE);
            self.inner[start..end].clone_from_slice(&LEAF_OVERFLOW_POINTER_DEFAULT.to_be_bytes());
        }

        Page(self.inner)
    }
}

impl Default for PageBuilder {
    fn default() -> Self {
        let builder = PageBuilder {
            inner: [0x0; PAGE_SIZE],
            _type: PageType::Leaf,
            content_set: false,
        }
        .kind(&PageType::Internal)
        .is_root(false);

        builder
    }
}

/// Converts a boolean value into a u8 value
pub fn bool_to_u8(v: bool) -> u8 {
    if v {
        0x0
    } else {
        0x1
    }
}

/// Converts a u8 value into a boolean value
pub fn u8_to_bool(v: u8) -> Result<bool, String> {
    match v {
        0x0 => Ok(true),
        0x1 => Ok(false),
        _ => Err("value is not a boolean".to_string()),
    }
}
