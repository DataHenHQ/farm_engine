mod status;
pub mod meta;
mod header;
mod iter_record;
pub mod traits;
mod with_data;
mod with_path;

pub use status::Status;
pub use header::Header;
pub use iter_record::IterRecord;
pub use with_data::TableWithData;
pub use with_path::TableWithPath;

use anyhow::{bail, Result};
use std::io::{Read, Seek};
use traits::TableTrait;
use uuid::Uuid;

/// Table engine version.
pub const VERSION: u32 = 2;

/// Table file extension.
pub const FILE_EXTENSION: &str = "fmtable";

/// Basic table implementation.
#[derive(Debug, PartialEq, Clone)]
pub struct Table {
    pub header: Header
}

impl Table {
    /// Create a new basic table.
    pub fn new(name: &str, uuid: Option<Uuid>) -> Result<Self> {
        Ok(Self {
            header: Header::new(name, uuid)?
        })
    }

    /// Load a table from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    pub fn load(reader: &mut (impl Read + Seek)) -> Result<Self> {
        let mut table = Self::new("",Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?;
        table.load_headers_from(reader)?;
        Ok(table)
    }

    /// Iterate over the table records.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `from_index` - Record index to start iteration.
    /// * `limit` - Number of records to iterate.
    pub fn iter<'reader, 'table>(&'table self, reader: &'reader mut (impl Read + Seek), from_index: Option<u64>, limit: Option<u64>) -> Result<IterRecord<'reader, 'table, impl Read + Seek>> {
        let min = match from_index {
            Some(v) => {
                if v > self.header_ref().meta.record_count {
                    bail!("from_index is greater than the table record count")
                }
                v
            },
            None => 0
        };
        let max = match limit {
            Some(v) => if v + min > self.header_ref().meta.record_count {
                self.header_ref().meta.record_count
            } else {
                v + min
            },
            None => self.header_ref().meta.record_count
        } - 1;
        self.seek_to_record(reader, min)?;
        Ok(IterRecord {
            reader,
            table: self,
            min,
            max
        })
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
    use std::io::Cursor;

    use crate::db::{field::Value, table::traits::table_test_helper::{self, fake_table_uuid}};
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

    #[test]
    fn load() {
        let (buf, _, _) = table_test_helper::fake_table_with_fields(true).unwrap();
        let mut reader = Cursor::new(buf.to_vec());
        let table = Table::load(&mut reader).unwrap();
        assert_eq!(table.header_ref().meta.record_count, 3);
        assert_eq!(table.header_ref().meta.get_name(), "my_table");
        assert_eq!(table.header_ref().meta.get_uuid(), &fake_table_uuid());
        let expected = vec![
            (234234234i32, "abc"),
            (345345345i32, "dfeg"),
            (857548574i32, "hi123")
        ];
        table.seek_to_record(&mut reader, 0).unwrap();
        for index in 0..3 {
            let foo = expected[index].0;
            let bar = expected[index].1;
            match table.record_from(&mut reader, index as u64).unwrap() {
                Some(record) => {
                    match record.get("foo") {
                        Some(v) => match v {
                            Value::I32(n) => assert_eq!(foo, *n),
                            _ => assert!(false, "expected foo {} but got None", foo)
                        },
                        None => assert!(false, "expected foo {} but got None", foo)
                    }
                    match record.get("bar") {
                        Some(v) => match v {
                            Value::Str(s) => assert_eq!(bar, s),
                            _ => assert!(false, "expected bar {} but got None", bar)
                        },
                        None => assert!(false, "expected bar {} but got None", bar)
                    }
                },
                None => assert!(false, "expected record but got None")
            }
        }
    }

    #[test]
    fn iter() {
        // init table
        let (buf, _, _) = table_test_helper::fake_table_with_fields(true).unwrap();
        let mut reader = Cursor::new(buf.to_vec());
        let table = Table::load(&mut reader).unwrap();
        let iter = table.iter(&mut reader, None, None).unwrap();
        let expected = vec![
            (234234234i32, "abc"),
            (345345345i32, "dfeg"),
            (857548574i32, "hi123")
        ];
        for (index, record) in iter.enumerate() {
            let foo = expected[index].0;
            let bar = expected[index].1;
            match record.get("foo") {
                Some(v) => match v {
                    Value::I32(n) => assert_eq!(foo, *n),
                    _ => assert!(false, "expected foo {} but got None", index)
                },
                None => assert!(false, "expected foo {} but got None", index)
            }
            match record.get("bar") {
                Some(v) => match v {
                    Value::Str(s) => assert_eq!(bar, s),
                    _ => assert!(false, "expected bar {} but got None", index)
                },
                None => assert!(false, "expected bar {} but got None", index)
            }
        }
    }

    #[test]
    fn iter_from_lower_range() {
        // init table
        let (buf, _, _) = table_test_helper::fake_table_with_fields(true).unwrap();
        let mut reader = Cursor::new(buf.to_vec());
        let table = Table::load(&mut reader).unwrap();
        let iter = table.iter(&mut reader, Some(1), None).unwrap();
        let expected = vec![
            (345345345i32, "dfeg"),
            (857548574i32, "hi123")
        ];
        let mut expected_index = 0;
        for (index, record) in iter.enumerate() {
            let foo = expected[expected_index].0;
            let bar = expected[expected_index].1;
            expected_index = expected_index + 1;
            match record.get("foo") {
                Some(v) => match v {
                    Value::I32(n) => assert_eq!(foo, *n),
                    _ => assert!(false, "expected foo {} but got None", index)
                },
                None => assert!(false, "expected foo {} but got None", index)
            }
            match record.get("bar") {
                Some(v) => match v {
                    Value::Str(s) => assert_eq!(bar, s),
                    _ => assert!(false, "expected bar {} but got None", index)
                },
                None => assert!(false, "expected bar {} but got None", index)
            }
        }
    }

    #[test]
    fn iter_from_lower_out_of_range() {
        // init table
        let (buf, _, _) = table_test_helper::fake_table_with_fields(true).unwrap();
        let mut reader = Cursor::new(buf.to_vec());
        let table = Table::load(&mut reader).unwrap();
        match table.iter(&mut reader, Some(10), None) {
            Ok(_) => assert!(false, "expected error but got iterator"),
            Err(e) => assert_eq!("from_index is greater than the table record count".to_string(), e.to_string())
        };
    }

    #[test]
    fn iter_from_high_range() {
        // init table
        let (buf, _, _) = table_test_helper::fake_table_with_fields(true).unwrap();
        let mut reader = Cursor::new(buf.to_vec());
        let table = Table::load(&mut reader).unwrap();
        let iter = table.iter(&mut reader, None, Some(2)).unwrap();
        let expected = vec![
            (234234234i32, "abc"),
            (345345345i32, "dfeg")
        ];
        let mut expected_index = 0;
        for (_, record) in iter.enumerate() {
            let foo = expected[expected_index].0;
            let bar = expected[expected_index].1;
            expected_index = expected_index + 1;
            match record.get("foo") {
                Some(v) => match v {
                    Value::I32(n) => assert_eq!(foo, *n),
                    _ => assert!(false, "expected foo {} but got None", expected_index)
                },
                None => assert!(false, "expected foo {} but got None", expected_index)
            }
            match record.get("bar") {
                Some(v) => match v {
                    Value::Str(s) => assert_eq!(bar, s),
                    _ => assert!(false, "expected bar {} but got None", expected_index)
                },
                None => assert!(false, "expected bar {} but got None", expected_index)
            }
        }
    }

    #[test]
    fn iter_from_range() {
        // init table
        let (buf, _, _) = table_test_helper::fake_table_with_fields(true).unwrap();
        let mut reader = Cursor::new(buf.to_vec());
        let table = Table::load(&mut reader).unwrap();
        let iter = table.iter(&mut reader, Some(1), Some(1)).unwrap();
        let expected = vec![
            (345345345i32, "dfeg")
        ];
        let mut expected_index = 0;
        for (_, record) in iter.enumerate() {
            let foo = expected[expected_index].0;
            let bar = expected[expected_index].1;
            expected_index = expected_index + 1;
            match record.get("foo") {
                Some(v) => match v {
                    Value::I32(n) => assert_eq!(foo, *n),
                    _ => assert!(false, "expected foo {} but got None", expected_index)
                },
                None => assert!(false, "expected foo {} but got None", expected_index)
            }
            match record.get("bar") {
                Some(v) => match v {
                    Value::Str(s) => assert_eq!(bar, s),
                    _ => assert!(false, "expected bar {} but got None", expected_index)
                },
                None => assert!(false, "expected bar {} but got None", expected_index)
            }
        }
    }

    #[test]
    fn iter_from_high_out_of_range() {
        // init table
        let (buf, _, _) = table_test_helper::fake_table_with_fields(true).unwrap();
        let mut reader = Cursor::new(buf.to_vec());
        let table = Table::load(&mut reader).unwrap();
        let iter = table.iter(&mut reader, None, Some(10)).unwrap();
        let expected = vec![
            (234234234i32, "abc"),
            (345345345i32, "dfeg"),
            (857548574i32, "hi123")
        ];
        let mut expected_index = 0;
        for (_, record) in iter.enumerate() {
            let foo = expected[expected_index].0;
            let bar = expected[expected_index].1;
            expected_index = expected_index + 1;
            match record.get("foo") {
                Some(v) => match v {
                    Value::I32(n) => assert_eq!(foo, *n),
                    _ => assert!(false, "expected foo {} but got None", expected_index)
                },
                None => assert!(false, "expected foo {} but got None", expected_index)
            }
            match record.get("bar") {
                Some(v) => match v {
                    Value::Str(s) => assert_eq!(bar, s),
                    _ => assert!(false, "expected bar {} but got None", expected_index)
                },
                None => assert!(false, "expected bar {} but got None", expected_index)
            }
        }
    }
}