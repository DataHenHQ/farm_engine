use anyhow::{bail, Result};
use std::io::{Read, Seek, SeekFrom, Write};
use crate::error::TableError;
use crate::traits::{LoadFrom, WriteTo};
use crate::db::field::Record;
use crate::db::table::{Header, Status};
use crate::db::table::meta::{MAGIC_NUMBER_SIZE, MAGIC_NUMBER_BYTES};
use crate::fill_writer;

pub trait TableTrait {
    /// Returns a reference to the table header.
    fn header_ref(&self) -> &Header;

    /// Returns a mutable reference to the table header.
    fn header_mut(&mut self) -> &mut Header;

    /// Returns the table binary size. This operation moves the reader to the end of the file.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    fn real_size_from(&self, reader: &mut (impl Read + Seek)) -> Result<u64> {
        reader.seek(SeekFrom::End(0))?;
        Ok(reader.stream_position()?)
    }

    /// Calculate the target record position at the table.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    fn calc_record_pos(&self, index: u64) -> u64 {
        let data_size = self.header_ref().record.record_byte_size() as u64;
        self.header_ref().size_as_bytes() + index * data_size
    }

    /// Loads the table's header from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    fn load_headers_from(&mut self, reader: &mut (impl Read + Seek)) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        self.header_mut().load_from(reader)?;
        Ok(())
    }

    /// Loads a table from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    fn load_from(&mut self, reader: &mut (impl Read + Seek)) -> Result<()> {
        match self.healthcheck_from(reader) {
            Ok(v) => match v {
                Status::Good => Ok(()),
                Status::NoFields => Err(TableError::NoFields.into()),
                vu => Err(TableError::Unavailable(vu).into())
            },
            Err(e) => Err(e)
        }
    }
    
    /// Move to index position and then read the record from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    fn record_from(&self, reader: &mut (impl Read + Seek), index: u64) -> Result<Option<Record>> {
        if self.header_ref().record.len() < 1 {
            bail!(TableError::NoFields)
        }

        if self.header_ref().meta.record_count > index {
            let pos = self.calc_record_pos(index);
            reader.seek(SeekFrom::Start(pos))?;
            return Ok(Some(self.header_ref().record.read_record(reader)?));
        }
        Ok(None)
    }

    /// Updates or append a record into the table file.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `index` - Index value index.
    /// * `record` - Record to save.
    /// * `save_headers` - Headers will be saved on append when true.
    fn save_record_into(&mut self, writer: &mut (impl Write + Seek), index: u64, record: &Record, save_headers: bool) -> Result<()> {
        // validate table
        if self.header_ref().record.len() < 1 {
            bail!(TableError::NoFields)
        }
        if index > self.header_ref().meta.record_count {
            bail!("can't write or append the record, the table file is too small");
        }

        // seek and write record
        let pos = self.calc_record_pos(index);
        writer.seek(SeekFrom::Start(pos))?;
        self.header_ref().record.write_record(writer, &record)?;
        
        // exit when no append
        if index < self.header_ref().meta.record_count {
            return Ok(())
        }

        // increase record count on append
        self.header_mut().meta.record_count += 1;
        if save_headers {
            writer.flush()?;
            self.save_headers_into(writer)?;
        }
        Ok(())
    }

    /// Validates the binary size.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// 
    /// # Returns
    /// 
    /// * `Status::New` - Table is new.
    /// * `Status::Good` - Binary magic number is valid.
    /// * `Status::Corrupted` - Binary magic number mismatch expected size.
    fn healthcheck_binary(&mut self, reader: &mut (impl Read + Seek)) -> Result<Status> {
        // check min size
        let size = self.real_size_from(reader)?;
        if size < 1 {
            return Ok(Status::New);
        }
        if size < MAGIC_NUMBER_SIZE as u64 {
            return Ok(Status::Corrupted);
        }

        // check on magic bytes
        reader.rewind()?;
        let mut magic_buf = [0u8; MAGIC_NUMBER_SIZE];
        reader.read_exact(&mut magic_buf)?;
        if magic_buf != MAGIC_NUMBER_BYTES {
            return Ok(Status::Corrupted);
        }
        Ok(Status::Good)
    }

    /// Validates the headers.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// 
    /// # Returns
    /// 
    /// * `Status::Good` - Headers are valid.
    /// * `Status::New` - Table is new.
    /// * `Status::Corrupted` - Headers are corrupted.
    fn healthcheck_headers(&mut self, reader: &mut (impl Read + Seek)) -> Result<Status> {
        // try to load the table headers
        reader.rewind()?;
        match self.load_headers_from(reader) {
            Ok(_) => Ok(Status::Good),
            Err(e) => match e.downcast::<std::io::Error>() {
                Ok(ex) => match ex.kind() {
                    std::io::ErrorKind::NotFound => {
                        // File not found so the table is new
                        Ok(Status::New)
                    }
                    std::io::ErrorKind::UnexpectedEof => {
                        // if the file is empty then is new
                        let real_size = self.real_size_from(reader)?;
                        if real_size < 1 {
                            return Ok(Status::New);
                        }

                        // EOF eror means the table is corrupted
                        Ok(Status::Corrupted)
                    },
                    _ => Err(ex.into())
                },
                Err(ex) => Err(ex)
            }
        }
    }

    /// Validates the table binary size.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// 
    /// # Returns
    /// 
    /// * `Status::Good` - Table binary size is valid.
    /// * `Status::Corrupted` - Table binary size mismatch expected size.
    fn healthcheck_size(&mut self, reader: &mut (impl Read + Seek)) -> Result<Status> {
        let real_size = self.real_size_from(reader)?;
        let expected_size = self.calc_record_pos(self.header_ref().meta.record_count);
        if real_size != expected_size {
            // sizes don't match, the file is corrupted
            return Ok(Status::Corrupted);
        }
        Ok(Status::Good)
    }

    /// Validates the fields.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// 
    /// # Returns
    /// 
    /// * `Status::Good` - Fields are valid.
    /// * `Status::NoFields` - Fields are invalid.
    fn healthcheck_fields(&mut self, _reader: &mut (impl Read + Seek)) -> Result<Status> {
        // validate field count
        if self.header_ref().record.len() < 1 {
            return Ok(Status::NoFields)
        }

        // all good
        Ok(Status::Good)
    }

    /// Perform a healthckeck over the table by reading
    /// the headers and checking the table binary size.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// 
    /// # Returns
    /// 
    /// * `Status::Good` - Table is valid.
    /// * `Status::New` - Table is new.
    /// * `Status::NoFields` - Table has no fields.
    /// * `Status::Corrupted` - Table is corrupted.
    fn healthcheck_from(&mut self, reader: &mut (impl Read + Seek)) -> Result<Status> {
        // check whenever table binary is ok
        match self.healthcheck_binary(reader)? {
            Status::Good =>match self.healthcheck_headers(reader)? {
                Status::Good => match self.healthcheck_size(reader)? {
                    Status::Good => match self.healthcheck_fields(reader)? {
                        Status::Good => Ok(Status::Good),
                        s => Ok(s)
                    },
                    s => Ok(s)
                },
                s => Ok(s)
            },
            s => Ok(s)
        }
    }

    /// Saves the headers and then jump back to the last writer stream position.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    fn save_headers_into(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.rewind()?;
        self.header_ref().write_to(writer)?;
        Ok(())
    }

    /// Prefill the stream with empty records.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `record_count` - Number of records to fill.
    fn fill_records_into(&mut self, writer: &mut (impl Write + Seek), record_count: u64) -> Result<()> {
        if self.header_ref().meta.record_count < record_count {
            let size = self.calc_record_pos(record_count);
            fill_writer(writer, size)?;
            writer.flush()?;
            self.header_mut().meta.record_count = record_count;
            self.save_headers_into(writer)?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;
    use crate::test_helper::*;
    use crate::db::field::{Value, FieldType, Field, Header as RecordHeader};
    use crate::db::table::meta::Meta;
    use crate::db::table::meta::test_helper::build_meta_bytes;
    use crate::traits::ByteSized;
    use uuid::Uuid;

    /// It's the size of a record header without any field.
    pub const EMPTY_RECORD_HEADER_BYTES: usize = u32::BYTES;

    /// Record header size generated by add_fields function.
    pub const ADD_FIELDS_HEADER_BYTES: usize = Field::BYTES * 2 + u32::BYTES;

    /// Record size generated by aADD_FIELDS_RECORD_BYTESdd_fields function.
    pub const ADD_FIELDS_RECORD_BYTES: usize = 13;

    /// Fake records bytes size generated by fake_records.
    pub const FAKE_RECORDS_BYTES: usize = ADD_FIELDS_RECORD_BYTES * 3;

    /// Fake table without fields byte size.
    pub const FAKE_TABLE_WITHOUT_FIELDS_BYTES: usize = Meta::BYTES + EMPTY_RECORD_HEADER_BYTES;

    /// Fake table with fields byte size.
    pub const FAKE_TABLE_BYTES: usize = Meta::BYTES + ADD_FIELDS_HEADER_BYTES + FAKE_RECORDS_BYTES;

    /// Byte slice that represents an empty record header.
    pub const EMPTY_RECORD_HEADER_BYTE_SLICE: [u8; EMPTY_RECORD_HEADER_BYTES] = [
        // field count
        0, 0, 0, 0u8
    ];

    /// Byte slice to be generated by the record header generated by add_fields_function.
    pub const ADD_FIELDS_HEADER_BYTE_SLICE: [u8; ADD_FIELDS_HEADER_BYTES] = [
        // field count
        0, 0, 0, 2u8,

        // foo field name value size
        0, 0, 0, 3u8,
        // foo field name value
        102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
        // foo field type
        4u8, 0, 0, 0, 0,

        // bar field name value size
        0, 0, 0, 3u8,
        // bar field name value
        98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
        // bar field type
        12u8, 0, 0, 0, 5u8
    ];

    pub const FAKE_RECORDS_BYTE_SLICE: [u8; FAKE_RECORDS_BYTES] = [
        // first record
        // foo field
        13u8, 246u8, 33u8, 122u8,
        // bar field
        0, 0, 0, 3u8, 97u8, 98u8, 99u8, 0, 0,

        // second record
        // foo field
        20u8, 149u8, 141u8, 65u8,
        // bar field
        0, 0, 0, 4u8, 100u8, 102u8, 101u8, 103u8, 0,

        // third record
        // foo field
        51u8, 29u8, 39u8, 30u8,
        // bar field
        0, 0, 0, 5u8, 104u8, 105u8, 49u8, 50u8, 51u8
    ];

    pub struct FakeTable {
        pub header: Header
    }

    /// Fake table struct for testing
    impl FakeTable {
        pub fn new(name: &str, uuid: Option<Uuid>) -> Self {
            Self{
                header: Header::new(name, uuid).unwrap()
            }
        }
    }

    impl TableTrait for FakeTable {
        fn header_ref(&self) -> &Header {
            &self.header
        }
    
        fn header_mut(&mut self) -> &mut Header {
            &mut self.header
        }
    }

    /// Add test fields into record header.
    /// 
    /// # Arguments
    /// 
    /// * `header` - Record header to add fields into.
    pub fn add_fields(header: &mut RecordHeader) -> Result<()> {
        header.add("foo", FieldType::I32)?;
        header.add("bar", FieldType::Str(5))?;

        Ok(())
    }

    /// Create fake records based on the fields added by add_fields.
    /// 
    /// # Arguments
    /// 
    /// * `records` - Record vector to add records into.
    pub fn fake_records() -> Result<Vec<Record>> {
        let mut header = RecordHeader::new();
        add_fields(&mut header)?;
        let mut records = Vec::new();

        // add first record
        let mut record = header.new_record()?;
        record.set_by_index(0, Value::I32(234234234i32));
        record.set_by_index(1, Value::Str("abc".to_string()));
        records.push(record);

        // add second record
        let mut record = header.new_record()?;
        record.set_by_index(0, Value::I32(345345345i32));
        record.set_by_index(1, Value::Str("dfeg".to_string()));
        records.push(record);

        // add third record
        let mut record = header.new_record()?;
        record.set_by_index(0, Value::I32(857548574i32));
        record.set_by_index(1, Value::Str("hi123".to_string()));
        records.push(record);

        Ok(records)
    }

    /// Resturn a fake table uuid.
    pub fn fake_table_uuid() -> Uuid {
        Uuid::from_bytes([0u8; Uuid::BYTES])
    }

    /// Return a fake table file without fields as byte slice.
    pub fn fake_table_without_fields(real_record_count: bool) -> Result<([u8; FAKE_TABLE_WITHOUT_FIELDS_BYTES], Meta)> {
        // init buffer
        let fake_record_count = if real_record_count { 0 } else { 1245634545244325234u64 };
        let mut buf = [0u8; FAKE_TABLE_WITHOUT_FIELDS_BYTES];
        let (meta_buf, meta) = build_meta_bytes("my_table", fake_record_count, Some(fake_table_uuid()));
        copy_bytes(&mut buf, &meta_buf, 0)?;
        copy_bytes(&mut buf, &EMPTY_RECORD_HEADER_BYTE_SLICE, Meta::BYTES)?;
        Ok((buf, meta))
    }

    /// Return a fake table file with fields as byte slice and the record count.
    pub fn fake_table_with_fields(real_record_count: bool) -> Result<([u8; FAKE_TABLE_BYTES], u64, Meta)> {
        // init buffer
        let fake_record_count = if real_record_count { 3 } else { 3245634545244324234u64 };
        let mut buf = [0u8; FAKE_TABLE_BYTES];
        let (meta_buf, meta) = build_meta_bytes("my_table", fake_record_count, Some(fake_table_uuid()));
        copy_bytes(&mut buf, &meta_buf, 0)?;
        copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, Meta::BYTES)?;
        copy_bytes(&mut buf, &FAKE_RECORDS_BYTE_SLICE, Meta::BYTES + ADD_FIELDS_HEADER_BYTES)?;
        Ok((buf, 3, meta))
    }

    /// Write a fake table bytes into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `unprocessed` - If `true` then build all records with MatchFlag::None.
    pub fn write_fake_table(writer: &mut (impl Seek + Write), unprocessed: bool) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        // write table header
        let mut header = Header::new("my_table", Some(fake_table_uuid()))?;
        header.meta.record_count = 4;
        add_fields(&mut header.record)?;
        header.write_to(writer)?;
        
        // write first record
        let mut record = header.record.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(111i32));
            record.set("bar", Value::Str("first".to_string()));
        }
        header.record.write_record(writer, &record)?;
        records.push(record);
        
        // write second record date
        let mut record = header.record.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(222i32));
            record.set("bar", Value::Str("2th".to_string()));
        }
        header.record.write_record(writer, &record)?;
        records.push(record);
        
        // write third record date
        let mut record = header.record.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(333i32));
            record.set("bar", Value::Str("3rd".to_string()));
        }
        header.record.write_record(writer, &record)?;
        records.push(record);

        // write fourth record date
        let mut record = header.record.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(444i32));
            record.set("bar", Value::Str("4th".to_string()));
        }
        header.record.write_record(writer, &record)?;
        records.push(record);

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helper::*;
    use std::io::Cursor;
    use crate::test_helper::*;
    use crate::db::field::Value;
    use crate::db::table::meta::test_helper::build_meta_bytes;
    use crate::db::table::meta::Meta;
    use crate::traits::ByteSized;

    #[test]
    fn calc_record_pos_with_fields() {
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));

        // add fields
        if let Err(e) = add_fields(&mut table.header.record) {
            assert!(false, "expected to add fields, but got error: {:?}", e);
        }
        assert_eq!(241, table.calc_record_pos(2));
        assert_eq!(254, table.calc_record_pos(3));
    }

    #[test]
    fn calc_record_pos_without_fields() {
        let table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let pos = Meta::BYTES as u64 + table.header.record.size_as_bytes();
        assert_eq!(pos, table.calc_record_pos(1));
        assert_eq!(pos, table.calc_record_pos(2));
        assert_eq!(pos, table.calc_record_pos(3));
    }

    #[test]
    fn load_headers_from() {
        // create buffer
        let mut buf = [0u8; Meta::BYTES + ADD_FIELDS_HEADER_BYTES];
        let (header_buf, _) = build_meta_bytes("my_table", 3245634545244324234u64, Some(fake_table_uuid()));
        if let Err(e) = copy_bytes(&mut buf, &header_buf, 0) {
            assert!(false, "{:?}", e);
        }
        if let Err(e) = copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, Meta::BYTES) {
            assert!(false, "{:?}", e);
        }
        let mut reader = Cursor::new(buf.to_vec());

        // test load_headers
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        if let Err(e) = table.load_headers_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
        }

        // check expected table header
        let mut expected = Header::new("my_table", Some(fake_table_uuid())).unwrap();
        expected.meta.record_count = 3245634545244324234u64;
        if let Err(e) = add_fields(&mut expected.record) {
            assert!(false, "expected to add fields, but got error: {:?}", e);
        }
        assert_eq!(expected, table.header);
    }

    #[test]
    fn record_from_with_fields() {
        // init buffer
        let (buf, record_count, _) = match fake_table_with_fields(false) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init table and expected records
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        table.header.meta.record_count = record_count;
        if let Err(e) = add_fields(&mut table.header.record) {
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
        let record = match table.record_from(&mut reader, 0) {
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
        assert_eq!(expected[0], record);

        // test second record
        let record = match table.record_from(&mut reader, 1) {
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
        assert_eq!(expected[1], record);

        // test third record
        let record = match table.record_from(&mut reader, 2) {
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
        assert_eq!(expected[2], record);
    }

    #[test]
    fn record_from_without_fields() {
        // init buffer
        let buf = [0u8];
        let mut reader = Cursor::new(buf.to_vec());

        // init table
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        table.header.meta.record_count = 4;

        // test
        match table.record_from(&mut reader, 0) {
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
    fn save_record_from_smaller_file() {
        // create table
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut writer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let mut records = write_fake_table(&mut writer, false).unwrap();
        add_fields(&mut table.header.record).unwrap();

        // set record count to trigger the error
        table.header.meta.record_count = 1;

        // test
        let expected = "can't write or append the record, the table file is too small";
        records[2].set("foo", Value::I32(11));
        records[2].set("bar", Value::Str("hello".to_string()));
        match table.save_record_into(&mut writer, 2, &records[2], true) {
            Ok(v) => assert!(false, "expected error but got {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn save_record_into_with_fields() {
        // create table and check original value
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut data: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let mut records = write_fake_table(&mut data, false).unwrap();
        data.rewind().unwrap();
        add_fields(&mut table.header.record).unwrap();
        table.header.meta.record_count = records.len() as u64;

        // read old record value
        let pos = table.calc_record_pos(2);
        let mut buf = [0u8; ADD_FIELDS_RECORD_BYTES];
        let mut old_bytes_before = vec!(0u8; pos as usize);
        let mut old_bytes_after = vec!(0u8; ADD_FIELDS_RECORD_BYTES);
        if let Err(e) = data.read_exact(&mut old_bytes_before) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        if let Err(e) = data.read_exact(&mut buf) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        if let Err(e) = data.read_exact(&mut old_bytes_after) {
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
        if let Err(e) = table.save_record_into(&mut data, 2, &records[2], true) {
            assert!(false, "expected success but got error: {:?}", e)
        }
        if let Err(e) = data.seek(SeekFrom::Start(0)) {
            assert!(false, "expected to seek on reader but got error: {:?}", e);
            return;
        }
        let mut new_bytes_before = vec!(0u8; pos as usize);
        let mut new_bytes_after = vec!(0u8; ADD_FIELDS_RECORD_BYTES);
        if let Err(e) = data.read_exact(&mut new_bytes_before) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        if let Err(e) = data.read_exact(&mut buf) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        if let Err(e) = data.read_exact(&mut new_bytes_after) {
            assert!(false, "expected to read bytes but got error: {:?}", e);
            return;
        }
        assert_eq!(old_bytes_before, new_bytes_before);
        assert_eq!(expected, buf);
        assert_eq!(old_bytes_after, new_bytes_after);
    }

    #[test]
    fn healthcheck_new_table() {
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut reader: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        
        // test healthcheck status
        let expected = Status::New;
        match table.healthcheck_from(&mut reader) {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn healthcheck_new_with_empty_file() {
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut reader: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    
        // test healthcheck status
        let expected = Status::New;
        match table.healthcheck_from(&mut reader) {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn healthcheck_corrupted_headers() {
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut reader: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        let buf = [0u8; 5];
        reader.write_all(&buf).unwrap();
        let expected = Status::Corrupted;
        match table.healthcheck_from(&mut reader) {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }
    
    #[test]
    fn healthcheck_corrupted() {
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut data: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let mut header = Header::new("my_table", Some(fake_table_uuid())).unwrap();
        header.meta.record_count = 10;
        add_fields(&mut header.record).unwrap();
        header.write_to(&mut data).unwrap();

        data.rewind().unwrap();
        table.header.load_from(&mut data).unwrap();
        let expected = Status::Corrupted;
        match table.healthcheck_from(&mut data) {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }
    
    #[test]
    fn healthcheck_good() {
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut reader: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        write_fake_table(&mut reader, false).unwrap();
        reader.rewind().unwrap();
        let expected = Status::Good;
        match table.healthcheck_from(&mut reader) {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }
    
    #[test]
    fn healthcheck_no_fields() {
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut reader: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        table.save_headers_into(&mut reader).unwrap();
        let expected = Status::NoFields;
        match table.healthcheck_from(&mut reader) {
            Ok(status) => assert_eq!(expected , status),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn save_record_into_without_fields() {
        // create table and create expected table file contents
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut data: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let mut records = write_fake_table(&mut data, false).unwrap();
        let mut expected = Vec::new();
        data.rewind().unwrap();
        data.read_to_end(&mut expected).unwrap();

        // test
        records[2].set("foo", Value::I32(11));
        records[2].set("bar", Value::Str("hello".to_string()));
        match table.save_record_into(&mut data, 2, &records[2], true) {
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
        data.rewind().unwrap();
        data.read_to_end(&mut buf).unwrap();
        assert_eq!(expected, buf);
    }

    #[test]
    fn save_headers_into() {
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut data: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // create table file and read table header data
        write_fake_table(&mut data, false).unwrap();
        data.rewind().unwrap();
        let size = Meta::BYTES + 122;
        let mut expected = vec![0u8; size];
        data.read_exact(&mut expected).unwrap();
        data.rewind().unwrap();
        table.header.load_from(&mut data).unwrap();

        // test save table header
        let mut buf = vec![0u8; size];
        let wrt = &mut buf as &mut [u8];
        let mut writer = Cursor::new(wrt);
        if let Err(e) = table.save_headers_into(&mut writer) {
            assert!(false, "expected success but got error: {:?}", e);
        };
        assert_eq!(expected, buf);
    }

    #[test]
    fn fill_records_into() {
        let mut table = FakeTable::new("my_table", Some(fake_table_uuid()));
        let mut data: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        // create table file and read table header data
        let mut records = write_fake_table(&mut data, false).unwrap();
        assert_eq!(records.len(), 4);
        table.load_from(&mut data).unwrap();
        assert_eq!(table.header.meta.record_count, 4);

        // fill and validate header record count
        table.fill_records_into(&mut data, 6).unwrap();
        assert_eq!(table.header.meta.record_count, 6);

        // validate size
        let size = table.real_size_from(&mut data).unwrap();
        let expected_size = table.calc_record_pos(6);
        assert_eq!(expected_size, size);

        // validate records
        records.push(table.header.record.new_record().unwrap());
        records.push(table.header.record.new_record().unwrap());
        assert_eq!(records.len(), 6);
        for i in 0..6 {
            let record = match table.record_from(&mut data, i as u64) {
                Ok(opt) => match opt {
                    Some(v) => v,
                    None => return assert!(false, "expected record but got None")
                },
                Err(e) => return assert!(false, "expected success but got error: {:?}", e)
            };
            assert_eq!(*records[i].get("foo").unwrap(), *record.get("foo").unwrap());
            assert_eq!(*records[i].get("bar").unwrap(), *record.get("bar").unwrap());
        }
    }
}