mod status;
pub mod meta;
mod header;
pub mod traits;
mod with_data;
mod with_path;

pub use status::Status;
pub use header::Header;
pub use with_data::TableWithData;
pub use with_path::TableWithPath;

use anyhow::Result;
use std::io::{Read, Seek};
use traits::TableTrait;
use uuid::Uuid;

/// Table engine version.
pub const VERSION: u32 = 2;

/// Table file extension.
pub const FILE_EXTENSION: &str = "fmtable";

/// Basic table implementation
#[derive(Debug, PartialEq, Clone)]
pub struct Table {
    pub header: Header
}

impl Table {
    /// Create a new basic table
    pub fn new(name: &str, uuid: Option<Uuid>) -> Result<Self> {
        Ok(Self {
            header: Header::new(name, uuid)?
        })
    }

    pub fn load(reader: &mut (impl Read + Seek)) -> Result<Self> {
        let mut table = Self::new("",Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?;
        table.load_headers_from(reader)?;
        Ok(table)
    }
}

impl TableTrait for Table {
    fn header_ref(&self) -> &Header {
        &self.header
    }

    fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }
}

#[cfg(test)]
pub use with_path::test_helper as with_path_test_helper;

use crate::traits::ByteSized;

#[cfg(test)]
mod tests {
    use crate::db::table::traits::table_test_helper::fake_table_uuid;
    use super::*;

    #[test]
    fn header_ref() {
        // init table
        let mut table = Table::new("my_table", Some(fake_table_uuid())).unwrap();
        table.header.meta.record_count = 4;
        assert_eq!(table.header_ref().meta.record_count, 4);
        table.header.meta.record_count = 3;
        assert_eq!(table.header_ref().meta.record_count, 3);
    }

    #[test]
    fn header_mut() {
        // init table
        let mut table = Table::new("my_table", Some(fake_table_uuid())).unwrap();
        table.header_mut().meta.record_count = 4;
        assert_eq!(table.header.meta.record_count, 4);
        table.header_mut().meta.record_count = 3;
        assert_eq!(table.header.meta.record_count, 3);
    }
}