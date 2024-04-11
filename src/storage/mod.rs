pub mod btree;
pub mod cell;
pub(crate) mod layout;
pub(crate) mod page;
pub mod statement;

use std::error::Error;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub trait StorageEngine {
    /// Inserts a new record
    ///
    /// # Params
    ///
    /// - `identifier`: Unique identifier for the record.
    /// - `value`: Byte array of data to store.
    fn insert(&mut self, identifier: u64, value: Vec<u8>) -> Result<()>;

    /// Updates an existing record
    ///
    /// # Params
    ///
    /// - `identifier`: Unique identifier for the record.
    /// - `value`: Updated byte array of data to store.
    fn update(&mut self, identifier: u64, value: Vec<u8>) -> Result<()>;

    /// Removes an existing record
    ///
    /// # Params
    ///
    /// - `identifier`: Unique identifier for the record.
    fn remove(&mut self, identifier: u64) -> Result<()>;

    /// Retrieves an existing record
    ///
    /// # Params
    ///
    /// - `identifier`: Unique identifier for the record.
    fn get(self, identifier: u64) -> Result<Vec<u8>>;
}
