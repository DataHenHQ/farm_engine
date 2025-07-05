use anyhow::Result;
use uuid::Uuid;
use std::io::{Seek, Read, Write};
use std::marker::PhantomData;
use crate::db::table::Header;
use crate::traits::{ByteSized, DataTrait};
use crate::db::field::Record;
use super::traits::TableTrait;
use super::Table;
use super::Status;

/// Table engine.
#[derive(Debug, PartialEq, Clone)]
pub struct TableWithData<T: Read + Write + Seek, E, S: DataTrait<T, E>> {
    _data_trait_error: PhantomData<E>,
    _data_trait: PhantomData<T>,

    /// Table data.
    pub data: S,

    /// Base table
    pub base: Table
}

impl<T: Read + Write + Seek, E, S: DataTrait<T, E>> TableWithData<T, E, S> {
    /// Returns a reference to the table header.
    pub fn header_ref(&self) -> &Header {
        &self.base.header
    }

    /// Returns a mutable reference to the table header.
    pub fn header_mut(&mut self) -> &mut Header {
        &mut self.base.header
    }

    /// Create a new table instance.
    /// 
    /// # Arguments
    /// 
    /// * `data` - Table data.
    /// * `name` - Table name.
    /// * `uuid` - Table UUID, if not provided a random UUID will be generated.
    pub fn new(data: S, name: &str, uuid: Option<Uuid>) -> Result<Self> {
        let base = Table::new(name, uuid)?;
        Ok(Self{
            _data_trait_error: PhantomData,
            _data_trait: PhantomData,
            data,
            base
        })
    }

    /// Returns the table binary size. This operation moves the reader to the end of the data.
    pub fn real_size(&mut self) -> Result<u64> {
        self.base.real_size_from(&mut self.data)
    }

    /// Loads the table's header.
    pub fn load_headers(&mut self) -> Result<()> {
        self.base.load_headers_from(&mut self.data)
    }

    /// Loads a table from a file.
    /// 
    /// # Arguments
    /// 
    /// * `data` - Table data.
    pub fn load(data: S) -> Result<Self> {
        let mut table = Self::new(data, "", Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?;
        table.base.load_from(&mut table.data)?;
        Ok(table)
    }

    /// Read the record from the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn record(&mut self, index: u64) -> Result<Option<Record>> {
        self.base.record_from(&mut self.data, index)
    }

    /// Prefill the stream with empty records.
    /// 
    /// # Arguments
    /// 
    /// * `record_count` - Number of records to prefill.
    pub fn fill_records(&mut self, record_count: u64) -> Result<()> {
        self.base.fill_records_into(&mut self.data, record_count)
    }

    /// Updates or append a record into the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Index value index.
    /// * `record` - Record to save.
    /// * `save_headers` - Headers will be saved on append when true.
    pub fn save_record(&mut self, index: u64, record: &Record) -> Result<()> {
        self.base.save_record_into(&mut self.data, index, record)
    }

    /// Appends a record into the table file.
    /// 
    /// # Arguments
    /// 
    /// * `record` - Record to append.
    /// * `save_headers` - Headers will be saved on append when true.
    pub fn append_record(&mut self, record: &Record, save_headers: bool) -> Result<()> {
        self.base.append_record_into(&mut self.data, record, save_headers)
    }

    /// Perform a healthckeck over the table.
    /// 
    /// # Returns
    /// 
    /// * `Status::Good` - Table is valid.
    /// * `Status::New` - Table is new.
    /// * `Status::NoFields` - Table has no fields.
    /// * `Status::Corrupted` - Table is corrupted.
    pub fn healthcheck(&mut self) -> Result<Status> {
        self.base.healthcheck_from(&mut self.data)
    }

    /// Saves the headers and then jump back to the last writer stream position.
    pub fn save_headers(&mut self) -> Result<()> {
        self.base.save_headers_into(&mut self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use crate::error::TableError;
    use crate::Data;
    use crate::db::field::{FieldType, Value};
    use crate::db::table::Header;
    use crate::db::table::meta::Meta;
    use crate::db::table::traits::table_test_helper::*;
    use crate::traits::{ByteSized, LoadFrom, WriteTo};

    #[test]
    fn header_ref() {
        // init table
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
        table.base.header.meta.record_count = 4;
        assert_eq!(table.header_ref().meta.record_count, 4);
        table.base.header.meta.record_count = 3;
        assert_eq!(table.header_ref().meta.record_count, 3);
    }

    #[test]
    fn header_mut() {
        // init table
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
        table.header_mut().meta.record_count = 4;
        assert_eq!(table.base.header.meta.record_count, 4);
        table.header_mut().meta.record_count = 3;
        assert_eq!(table.base.header.meta.record_count, 3);
    }

    #[test]
    fn record_with_fields() {
        // init buffer
        let (buf, record_count, meta) = match fake_table_with_fields(false) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        data.write_all(&buf).unwrap();
        let mut table = TableWithData::new(data, meta.get_name(), Some(meta.get_uuid().clone())).unwrap();

        // init table and expected records
        table.base.header.meta.record_count = record_count;
        if let Err(e) = add_fields(&mut table.base.header.record) {
            assert!(false, "{:?}", e);
        }
        let expected = match fake_records() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test first record
        let data = match table.record(0) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => {
                    assert!(false, "expected {:?} but got None", expected[0]);
                    return;
                }
            },
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[0], data);

        // test second record
        let data = match table.record(1) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => {
                    assert!(false, "expected {:?} but got None", expected[0]);
                    return;
                }
            },
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[1], data);

        // test third record
        let data = match table.record(2) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => {
                    assert!(false, "expected {:?} but got None", expected[0]);
                    return;
                }
            },
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[2], data);
    }

    #[test]
    fn record_without_fields() {
        // init table
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
        table.base.header.meta.record_count = 4;

        // test
        match table.record(0) {
            Ok(v) => assert!(false, "expected TableError::NoFields but got {:?}", v),
            Err(e) => match e.downcast::<TableError>() {
                Ok(ex) => match ex {
                    TableError::NoFields => {},
                    te => assert!(false, "expected TableError::NoFields but got TableError::{:?}", te)
                },
                Err(ex) => assert!(false, "expected TableError::NoFields but got error: {:?}", ex)
            }
        }
    }

    #[test]
    fn save_record_smaller_file() {
        // create table
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
        let mut records = write_fake_table(&mut table.data, false).unwrap();
        add_fields(&mut table.base.header.record).unwrap();

        // set record count to trigger the error
        table.base.header.meta.record_count = 1;

        // test
        let expected = "can't write or append the record, the table file is too small";
        records[2].set("foo", Value::I32(11));
        records[2].set("bar", Value::Str("hello".to_string()));
        match table.save_record(2, &records[2]) {
            Ok(v) => assert!(false, "expected error but got {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn save_record_with_fields() {
        // create table and check original value
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
        let mut records = write_fake_table(&mut table.data, false).unwrap();
        add_fields(&mut table.base.header.record).unwrap();
        table.base.header.meta.record_count = records.len() as u64;

        // read old record value
        let pos = table.base.calc_record_pos(2);
        let mut buf = [0u8; ADD_FIELDS_RECORD_BYTES];
        let mut old_bytes_before = vec!(0u8; pos as usize);
        let mut old_bytes_after = vec!(0u8; ADD_FIELDS_RECORD_BYTES);
        table.data.rewind().unwrap();
        if let Err(e) = table.data.read_exact(&mut old_bytes_before) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        if let Err(e) = table.data.read_exact(&mut buf) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        if let Err(e) = table.data.read_exact(&mut old_bytes_after) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        let expected = [
            // foo field
            0, 0, 1u8, 77u8,
            // bar field
            0, 0, 0, 3u8, 51u8, 114u8, 100u8, 0, 0
        ];
        assert_eq!(expected, buf);

        // save record and check saved record value
        let expected = [
            // foo field
            0, 0, 0, 11u8,
            // bar field
            0, 0, 0, 5u8, 104u8, 101u8, 108u8, 108u8, 111u8
        ];
        records[2].set("foo", Value::I32(11));
        records[2].set("bar", Value::Str("hello".to_string()));
        if let Err(e) = table.save_record(2, &records[2]) {
            assert!(false, "expected success but got error: {:?}", e)
        }
        if let Err(e) = table.data.rewind() {
            assert!(false, "expected to seek on reader but got error: {:?}", e);
            return;
        }
        let mut new_bytes_before = vec!(0u8; pos as usize);
        let mut new_bytes_after = vec!(0u8; ADD_FIELDS_RECORD_BYTES);
        if let Err(e) = table.data.read_exact(&mut new_bytes_before) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        if let Err(e) = table.data.read_exact(&mut buf) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        if let Err(e) = table.data.read_exact(&mut new_bytes_after) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        assert_eq!(old_bytes_before, new_bytes_before);
        assert_eq!(expected, buf);
        assert_eq!(old_bytes_after, new_bytes_after);
    }

    #[test]
    fn save_record_without_fields() {
        // create table and create expected table file contents
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
        let mut records = write_fake_table(&mut table.data, false).unwrap();
        let mut expected = Vec::new();
        table.data.rewind().unwrap();
        table.data.read_to_end(&mut expected).unwrap();

        // test
        records[2].set("foo", Value::I32(11));
        records[2].set("bar", Value::Str("hello".to_string()));
        match table.save_record(2, &records[2]) {
            Ok(()) => assert!(false, "expected TableError::NoFields but got success"),
            Err(e) => match e.downcast::<TableError>() {
                Ok(ex) => match ex {
                    TableError::NoFields => {},
                    te => assert!(false, "expected TableError::NoFields but got TableError::{:?}", te)
                },
                Err(ex) => assert!(false, "expected TableError::NoFields but got error: {:?}", ex)
            }
        }

        // check file after invalid save, it shouldn't change
        let mut buf = Vec::new();
        table.data.rewind().unwrap();
        table.data.read_to_end(&mut buf).unwrap();
        assert_eq!(expected, buf);
    }

    #[test]
    fn append_record_into() {
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
        table.header_mut().record.add("foo", FieldType::I32).unwrap();
        table.save_headers().unwrap();
        let expected_values = vec![11i32, 22i32, 33i32, 44i32];
        for index in 0..expected_values.len() {
            let mut record = table.header_ref().record.new_record().unwrap();
            record.set("foo", Value::I32(expected_values[index]));
            table.append_record(&record, false).unwrap();
            assert_eq!(table.header_ref().meta.record_count, index as u64 + 1);
        }
        assert_eq!(table.header_ref().meta.record_count, 4);
        for index in 0..expected_values.len() {
            match table.record(index as u64) {
                Ok(opt) => match opt {
                    Some(v) => assert_eq!(v.get("foo").unwrap(), &Value::I32(expected_values[index])),
                    None => assert!(false, "expected record with foo {} but got None", expected_values[index])
                },
                Err(e) => assert!(false, "expected success but got error: {:?}", e)
            }
        }
    }

    #[test]
    fn healthcheck_new_table() {
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
        
        // test healthcheck status
        let expected = Status::New;
        match table.healthcheck() {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn healthcheck_new_with_empty_file() {
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
    
        // test healthcheck status
        let expected = Status::New;
        match table.healthcheck() {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn healthcheck_corrupted_headers() {
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();

        let buf = [0u8; 5];
        table.data.write_all(&buf).unwrap();
        let expected = Status::Corrupted;
        match table.healthcheck() {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }
    
    #[test]
    fn healthcheck_corrupted() {
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();

        let mut buf = [0u8; Meta::BYTES+EMPTY_RECORD_HEADER_BYTES+5];
        let mut writer = &mut buf as &mut [u8];
        let mut header = Header::new("my_table", Some(fake_table_uuid())).unwrap();
        header.meta.record_count = 10;
        header.write_to(&mut writer).unwrap();

        table.data.rewind().unwrap();
        table.data.write_all(&buf).unwrap();
        add_fields(&mut table.base.header.record).unwrap();
        let expected = Status::Corrupted;
        match table.healthcheck() {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }
    
    #[test]
    fn healthcheck_good() {
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();

        write_fake_table(&mut table.data, false).unwrap();
        let expected = Status::Good;
        match table.healthcheck() {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }
    
    #[test]
    fn healthcheck_no_fields() {
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();

        table.save_headers().unwrap();
        let expected = Status::NoFields;
        match table.healthcheck() {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn save_headers() {
        // create table file and read table header data
        let data: Data<Cursor<Vec<u8>>> = Data::new(Cursor::new(Vec::new()), false);
        let mut table = TableWithData::new(data, "my_table", Some(fake_table_uuid())).unwrap();
        write_fake_table(&mut table.data, false).unwrap();
        let size = Meta::BYTES + 122;
        let mut expected = vec![0u8; size];
        table.data.rewind().unwrap();
        table.data.read_exact(&mut expected).unwrap();
        table.data.rewind().unwrap();
        table.base.header.load_from(&mut table.data).unwrap();

        // test save table header
        assert_eq!(4, table.base.header.meta.record_count);
        table.base.header.meta.record_count = 5;
        if let Err(e) = table.save_headers() {
            assert!(false, "expected success but got error: {:?}", e);
        };
        table.base.header.meta.record_count = 4;
        assert_eq!(4, table.base.header.meta.record_count);
        table.data.rewind().unwrap();
        table.base.header.load_from(&mut table.data).unwrap();
        assert_eq!(5, table.base.header.meta.record_count);
    }
}