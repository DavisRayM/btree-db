use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};

use crate::storage::{layout::PAGE_SIZE, page::PageBuilder};

use super::page::{CachedPage, Page, PageType};

pub struct Pager {
    num_pages: u64,
    root_page: u64,
    cache: HashMap<u64, CachedPage>,
    out: File,
}

impl Pager {
    pub fn new(path: PathBuf) -> Self {
        let out = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .expect("failed to open pager on-disk file");
        let file_len = out
            .metadata()
            .expect("failed to retrieve pager on-disk metadata")
            .len();
        let num_pages = file_len / PAGE_SIZE as u64;

        let mut obj = Self {
            num_pages,
            root_page: 0,
            cache: HashMap::new(),
            out,
        };

        if num_pages == 0 {
            obj.new_page(PageType::Leaf, true);
        }

        obj
    }

    fn file_len(&self) -> u64 {
        self.out
            .metadata()
            .expect("failed to retrieve pager on-disk metadata")
            .len()
    }

    fn read_page(&self, offset: u64) -> [u8; PAGE_SIZE] {
        let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let mut reader = BufReader::new(&self.out);

        reader
            .seek(SeekFrom::Start(offset))
            .expect("failed to read at offset");

        reader
            .read_exact(&mut buf)
            .expect("failed to read page data");

        buf
    }

    fn cache_page(&mut self, index: u64, page: Page) -> CachedPage {
        let cached_page = CachedPage::new(page);
        let copy = CachedPage(Arc::clone(&cached_page.0));
        self.cache.insert(index, cached_page);
        copy
    }

    pub fn root_page(&self) -> u64 {
        self.root_page
    }

    pub fn new_page(&mut self, kind: PageType, is_root: bool) -> CachedPage {
        let builder = PageBuilder::default().kind(&kind).is_root(is_root);

        let num = self.num_pages;
        self.num_pages += 1;
        self.cache_page(num, builder.build())
    }

    pub fn get_page(&mut self, num: u64) -> Option<CachedPage> {
        let offset = num * PAGE_SIZE as u64;
        if offset > self.file_len() {
            return None;
        }

        if let Some(cached_page) = self.cache.get(&num) {
            Some(CachedPage(Arc::clone(&cached_page.0)))
        } else {
            let page = Page(self.read_page(offset));
            Some(self.cache_page(num, page))
        }
    }

    pub fn flush_cache(&mut self) {
        let mut writer = BufWriter::new(&self.out);

        for (page_num, page) in self.cache.iter() {
            let offset = page_num * PAGE_SIZE as u64;
            writer
                .seek(SeekFrom::Start(offset))
                .expect("failed to flush cached pages");

            let bytes = page
                .0
                .read()
                .expect("failed to retrieve read handle on page")
                .0;
            writer
                .write_all(&bytes)
                .expect("failed to write updated page content");
        }
    }
}
