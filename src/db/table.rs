pub mod header;
pub mod record;

use anyhow::{bail, Result};
use uuid::Uuid;
use regex::Regex;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
use crate::{file_size, fill_file};
use crate::error::TableError;
use crate::traits::{ByteSized, LoadFrom, WriteTo};
use header::Header;
use record::header::{Header as RecordHeader};
use record::Record;

/// Table engine version.
pub const VERSION: u32 = 2;

/// Table file extension.
pub const FILE_EXTENSION: &str = "fmtable";

/// Table healthcheck status.
#[derive(Debug, PartialEq)]
pub enum Status {
    New,
    Good,
    NoFields,
    Corrupted
}

impl Display for Status{
    fn fmt(&self, f: &mut Formatter) -> FmtResult { 
        write!(f, "{}", match self {
            Self::New => "new",
            Self::Good => "good",
            Self::NoFields => "no fields",
            Self::Corrupted => "corrupted"
        })
    }
}

/// Table engine.
#[derive(Debug, PartialEq, Clone)]
pub struct Table {
    /// Table file path.
    pub path: PathBuf,

    /// Table header.
    pub header: Header,

    // Record header. It contains information about the fields.
    pub record_header: RecordHeader
}

impl Table {
    /// Generates a regex expression to validate the index file extension.
    pub fn file_extension_regex() -> Regex {
        let expression = format!(r"(?i)\.{}$", FILE_EXTENSION);
        Regex::new(&expression).unwrap()
    }

    /// Create a new table instance.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Table file path.
    /// * `name` - Table name.
    pub fn new(path: PathBuf, name: &str, uuid: Option<Uuid>) -> Result<Self> {
        Ok(Self{
            path,
            header: Header::new(name, uuid)?,
            record_header: RecordHeader::new()
        })
    }

    /// Loads a table from a file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Table file path.
    pub fn from_file(path: PathBuf) -> Result<Self> {
        let mut table = Self::new(path, "", Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?;
        match table.healthcheck() {
            Ok(v) => match v {
                Status::Good => Ok(table),
                vu => bail!(TableError::Unavailable(vu))
            },
            Err(e) => Err(e)
        }
    }

    /// Returns a table file buffered reader.
    pub fn new_reader(&self) -> Result<BufReader<File>> {
        let file = File::open(&self.path)?;
        Ok(BufReader::new(file))
    }

    /// Returns a table file buffered writer.
    /// 
    /// # Arguments
    /// 
    /// * `create` - Set to `true` when the file should be created.
    pub fn new_writer(&self, create: bool) -> Result<BufWriter<File>> {
        let mut options = OpenOptions::new();
        options.write(true);
        if create {
            options.create(true);
        }
        let file = options.open(&self.path)?;
        Ok(BufWriter::new(file))
    }

    /// Calculate the target record position at the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn calc_record_pos(&self, index: u64) -> u64 {
        let data_size = self.record_header.record_byte_size() as u64;
        Header::BYTES as u64 + self.record_header.size_as_bytes() + index * data_size
    }

    /// Get the record's headers.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    pub fn load_headers_from(&mut self, reader: &mut (impl Read + Seek)) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        self.header.load_from(reader)?;
        self.record_header.load_from(reader)?;
        Ok(())
    }
    
    /// Move to index position and then read the record from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    pub fn seek_record_from(&self, reader: &mut (impl Read + Seek), index: u64) -> Result<Option<Record>> {
        if self.record_header.len() < 1 {
            bail!(TableError::NoFields)
        }

        if self.header.record_count > index {
            let pos = self.calc_record_pos(index);
            reader.seek(SeekFrom::Start(pos))?;
            return Ok(Some(self.record_header.read_record(reader)?));
        }
        Ok(None)
    }

    /// Read the record from the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn record(&self, index: u64) -> Result<Option<Record>> {
        let mut reader = self.new_reader()?;
        self.seek_record_from(&mut reader, index)
    }

    /// Updates or append a record into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - File writer to save data into.
    /// * `index` - Record index.
    /// * `record` - Record to save.
    /// * `save_headers` - Headers will be saved on append when true.
    pub fn save_record_into(&mut self, writer: &mut (impl Write + Seek), index: u64, record: &Record, save_headers: bool) -> Result<()> {
        // validate table
        if self.record_header.len() < 1 {
            bail!(TableError::NoFields)
        }
        if index > self.header.record_count {
            bail!("can't write or append the record, the table file is too small");
        }

        // seek and write record
        let pos = self.calc_record_pos(index);
        writer.seek(SeekFrom::Start(pos))?;
        self.record_header.write_record(writer, &record)?;
        
        // exit when no append
        if index < self.header.record_count {
            return Ok(())
        }

        // increase record count on append
        self.header.record_count += 1;
        if save_headers {
            self.save_headers_into(writer)?;
        }
        Ok(())
    }

    /// Updates or append a record into the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Index value index.
    /// * `record` - Record to save.
    /// * `save_headers` - Headers will be saved on append when true.
    pub fn save_record(&mut self, index: u64, record: &Record, save_headers: bool) -> Result<()> {
        let mut writer = self.new_writer(false)?;        
        self.save_record_into(&mut writer, index, record, save_headers)?;
        writer.flush()?;
        Ok(())
    }

    /// Perform a healthckeck over the table file by reading
    /// the headers and checking the file size.
    pub fn healthcheck(&mut self) -> Result<Status> {
        // check whenever table file exists
        match self.new_reader() {
            // try to load the table headers
            Ok(mut reader) => if let Err(e) = self.load_headers_from(&mut reader) {
                match e.downcast::<std::io::Error>() {
                    Ok(ex) => match ex.kind() {
                        std::io::ErrorKind::NotFound => {
                            // File not found so the table is new
                            return Ok(Status::New);
                        }
                        std::io::ErrorKind::UnexpectedEof => {
                            // if the file is empty then is new
                            let real_size = file_size(&self.path)?;
                            if real_size < 1 {
                                return Ok(Status::New);
                            }

                            // EOF eror means the table is corrupted
                            return Ok(Status::Corrupted);
                        },
                        _ => bail!(ex)
                    },
                    Err(ex) => return Err(ex)
                }
            },
            Err(e) => match e.downcast::<std::io::Error>() {
                Ok(ex) => match ex.kind() {
                    std::io::ErrorKind::NotFound => {
                        return Ok(Status::New)
                    },
                    _ => bail!(ex)
                },
                Err(ex) => bail!(ex)
            }
        };

        // validate corrupted table
        let real_size = file_size(&self.path)?;
        let expected_size = self.calc_record_pos(self.header.record_count);
        if real_size != expected_size {
            // sizes don't match, the file is corrupted
            return Ok(Status::Corrupted);
        }
        
        // validate field count
        if self.record_header.len() < 1 {
            return Ok(Status::NoFields)
        }

        // all good
        Ok(Status::Good)
    }

    /// Saves the headers and then jump back to the last writer stream position.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    pub fn save_headers_into(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.flush()?;
        let old_pos = writer.stream_position()?;
        writer.rewind()?;
        self.header.write_to(writer)?;
        self.record_header.write_to(writer)?;
        writer.flush()?;
        writer.seek(SeekFrom::Start(old_pos))?;
        Ok(())
    }

    /// Saves the headers and then jump back to the last writer stream position.
    pub fn save_headers(&self) -> Result<()> {
        let mut writer = self.new_writer(false)?;
        self.save_headers_into(&mut writer)
    }

    /// Loads or creates the table file.
    /// 
    /// # Arguments
    /// 
    /// * `override_on_error` - Overrides the table file if corrupted instead of error.
    /// * `force_override` - Always creates a new table file with the current headers.
    pub fn load_or_create(&mut self, override_on_error: bool, force_override: bool) -> Result<()> {
        let mut should_create = force_override;

        // perform index healthcheck
        if !force_override {
            match self.healthcheck() {
                Ok(v) => match v {
                    Status::Good => return Ok(()),
                    Status::New => should_create = true,
                    Status::NoFields => bail!(TableError::NoFields),
                    vu => if !override_on_error {
                        bail!(TableError::Unavailable(vu))
                    }
                },
                Err(e) => return Err(e)
            }
        }

        // create table file when required
        if should_create {
            let mut writer = self.new_writer(true)?;
            let size = self.calc_record_pos(self.header.record_count);
            fill_file(&self.path, size, true)?;
            self.save_headers_into(&mut writer)?;
            writer.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;
    use crate::test_helper::*;
    use crate::db::field::{field_type::FieldType, Field};
    use crate::db::field::value::Value;
    use crate::db::table::header::test_helper::build_header_bytes;
    use tempfile::TempDir;

    /// It's the size of a record header without any field.
    pub const EMPTY_RECORD_HEADER_BYTES: usize = u32::BYTES;

    /// Record header size generated by add_fields function.
    pub const ADD_FIELDS_HEADER_BYTES: usize = Field::BYTES * 2 + u32::BYTES;

    /// Record size generated by aADD_FIELDS_RECORD_BYTESdd_fields function.
    pub const ADD_FIELDS_RECORD_BYTES: usize = 13;

    /// Fake records bytes size generated by fake_records.
    pub const FAKE_RECORDS_BYTES: usize = ADD_FIELDS_RECORD_BYTES * 3;

    /// Fake index with fields byte size.
    pub const FAKE_INDEX_BYTES: usize = Header::BYTES + ADD_FIELDS_HEADER_BYTES + FAKE_RECORDS_BYTES;

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

    /// Return a fake table file with fields as byte slice and the record count.
    pub fn fake_table_with_fields() -> Result<([u8; FAKE_INDEX_BYTES], u64)> {
        // init buffer
        let mut buf = [0u8; FAKE_INDEX_BYTES];
        let header_buf = build_header_bytes("my_table", 3245634545244324234u64, Some(fake_table_uuid()));
        copy_bytes(&mut buf, &header_buf, 0)?;
        copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, Header::BYTES)?;
        copy_bytes(&mut buf, &FAKE_RECORDS_BYTE_SLICE, Header::BYTES + ADD_FIELDS_HEADER_BYTES)?;
        Ok((buf, 3))
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
        header.record_count = 4;
        header.write_to(writer)?;

        // write record header
        let mut record_header = RecordHeader::new();
        add_fields(&mut record_header)?;
        record_header.write_to(writer)?;
        
        // write first record
        let mut record = record_header.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(111i32));
            record.set("bar", Value::Str("first".to_string()));
        }
        record_header.write_record(writer, &record)?;
        records.push(record);
        
        // write second record date
        let mut record = record_header.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(222i32));
            record.set("bar", Value::Str("2th".to_string()));
        }
        record_header.write_record(writer, &record)?;
        records.push(record);
        
        // write third record date
        let mut record = record_header.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(333i32));
            record.set("bar", Value::Str("3rd".to_string()));
        }
        record_header.write_record(writer, &record)?;
        records.push(record);

        // write fourth record date
        let mut record = record_header.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(444i32));
            record.set("bar", Value::Str("4th".to_string()));
        }
        record_header.write_record(writer, &record)?;
        records.push(record);

        Ok(records)
    }

    /// Create a fake table file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Table file path.
    /// * `empty` - If `true` then build all records as empty records.
    pub fn create_fake_table(path: &PathBuf, unprocessed: bool) -> Result<Vec<Record>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        let records = write_fake_table(&mut writer, unprocessed)?;
        writer.flush()?;

        Ok(records)
    }

    /// Execute a function with both a temp directory and a new table.
    /// 
    /// # Arguments
    /// 
    /// * `f` - Function to execute.
    pub fn with_tmpdir_and_table(f: &impl Fn(&TempDir, &mut Table) -> Result<()>) {
        let sub = |dir: &TempDir| -> Result<()> {
            // create Table and execute
            let mut table = Table::new(
                dir.path().join("t.fmtable"),
                "my_table",
                Some(fake_table_uuid())
            )?;

            // execute function
            match f(&dir, &mut table) {
                Ok(_) => Ok(()),
                Err(e) => bail!(e)
            }
        };
        with_tmpdir(&sub)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helper::*;
    use std::io::Cursor;
    use crate::test_helper::*;
    use crate::db::field::value::Value;
    use crate::db::table::header::test_helper::build_header_bytes;

    #[test]
    fn file_extension_regex() {
        let rx = Table::file_extension_regex();
        assert!(rx.is_match("hello.fmtable"), "expected to match \"hello.fmtable\" but got false");
        assert!(rx.is_match("/path/to/hello.fmtable"), "expected to match \"/path/to/hello.fmtable\" but got false");
        assert!(!rx.is_match("hello.table"), "expected to not match \"hello.table\" but got true");
    }

    #[test]
    fn new() {
        let header = Header::new("my_table", Some(fake_table_uuid())).unwrap();
        let expected = Table{
            path: "my_table.fmtable".into(),
            header,
            record_header: RecordHeader::new()
        };
        match Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn calc_record_pos_with_fields() {
        let mut table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();

        // add fields
        if let Err(e) = add_fields(&mut table.record_header) {
            assert!(false, "expected to add fields, but got error: {:?}", e);
        }
        assert_eq!(241, table.calc_record_pos(2));
        assert_eq!(254, table.calc_record_pos(3));
    }

    #[test]
    fn calc_record_pos_without_fields() {
        let table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();
        let pos = Header::BYTES as u64 + table.record_header.size_as_bytes();
        assert_eq!(pos, table.calc_record_pos(1));
        assert_eq!(pos, table.calc_record_pos(2));
        assert_eq!(pos, table.calc_record_pos(3));
    }

    #[test]
    fn load_headers_from() {
        // create buffer
        let mut buf = [0u8; Header::BYTES + ADD_FIELDS_HEADER_BYTES];
        let index_header_buf = build_header_bytes("my_table", 3245634545244324234u64, Some(fake_table_uuid()));
        if let Err(e) = copy_bytes(&mut buf, &index_header_buf, 0) {
            assert!(false, "{:?}", e);
        }
        if let Err(e) = copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, Header::BYTES) {
            assert!(false, "{:?}", e);
        }
        let mut reader = Cursor::new(buf.to_vec());

        // test load_headers
        let mut table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();
        if let Err(e) = table.load_headers_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
        }

        // check expected index header
        let mut expected = Header::new("my_table", Some(fake_table_uuid())).unwrap();
        expected.record_count = 3245634545244324234u64;
        assert_eq!(expected, table.header);

        // check expected record header
        let mut expected = RecordHeader::new();
        if let Err(e) = add_fields(&mut expected) {
            assert!(false, "expected to add fields, but got error: {:?}", e);
        }
        assert_eq!(expected, table.record_header);
    }

    #[test]
    fn seek_record_from_with_fields() {
        // init buffer
        let (buf, record_count) = match fake_table_with_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init table and expected records
        let mut table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();
        table.header.record_count = record_count;
        if let Err(e) = add_fields(&mut table.record_header) {
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
        let record = match table.seek_record_from(&mut reader, 0) {
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
        let record = match table.seek_record_from(&mut reader, 1) {
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
        let record = match table.seek_record_from(&mut reader, 2) {
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
    fn seek_record_from_without_fields() {
        // init buffer
        let buf = [0u8];
        let mut reader = Cursor::new(buf.to_vec());

        // init table
        let mut table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();
        table.header.record_count = 4;

        // test
        match table.seek_record_from(&mut reader, 0) {
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
    fn record_with_fields() {
        with_tmpdir_and_table(&|_, table| {
            // init buffer
            let (buf, record_count) = match fake_table_with_fields() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            create_file_with_bytes(&table.path, &buf)?;

            // init table and expected records
            table.header.record_count = record_count;
            if let Err(e) = add_fields(&mut table.record_header) {
                assert!(false, "{:?}", e);
            }
            let expected = match fake_records() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };

            // test first record
            let data = match table.record(0) {
                Ok(opt) => match opt {
                    Some(v) => v,
                    None => {
                        assert!(false, "expected {:?} but got None", expected[0]);
                        bail!("");
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e);
                }
            };
            assert_eq!(expected[0], data);

            // test second record
            let data = match table.record(1) {
                Ok(opt) => match opt {
                    Some(v) => v,
                    None => {
                        assert!(false, "expected {:?} but got None", expected[0]);
                        bail!("")
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            assert_eq!(expected[1], data);

            // test third record
            let data = match table.record(2) {
                Ok(opt) => match opt {
                    Some(v) => v,
                    None => {
                        assert!(false, "expected {:?} but got None", expected[0]);
                        bail!("")
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            assert_eq!(expected[2], data);
            Ok(())
        });
    }

    #[test]
    fn record_without_fields() {
        with_tmpdir_and_table(&|_, table| {
            // init buffer
            let buf = [0u8];
            create_file_with_bytes(&table.path, &buf)?;

            // init table
            table.header.record_count = 4;

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

            Ok(())
        });
    }

    #[test]
    fn save_record_into_smaller_file() {
        with_tmpdir_and_table(&|_, table| {
            // create table
            let mut records = create_fake_table(&table.path, false)?;
            add_fields(&mut table.record_header)?;

            // set record count to trigger the error
            table.header.record_count = 1;

            // test
            let expected = "can't write or append the record, the table file is too small";
            records[2].set("foo", Value::I32(11));
            records[2].set("bar", Value::Str("hello".to_string()));
            match table.save_record(2, &records[2], true) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }
            
            Ok(())
        });
    }

    #[test]
    fn save_record_into_with_fields() {
        with_tmpdir_and_table(&|_, table| {
            // create table and check original value
            let mut records = create_fake_table(&table.path, false)?;
            add_fields(&mut table.record_header)?;
            table.header.record_count = records.len() as u64;

            // read old record value
            let pos = table.calc_record_pos(2);
            let mut buf = [0u8; ADD_FIELDS_RECORD_BYTES];
            let file = File::open(&table.path)?;
            let mut reader = BufReader::new(file);
            let mut old_bytes_before = vec!(0u8; pos as usize);
            let mut old_bytes_after = vec!(0u8; ADD_FIELDS_RECORD_BYTES);
            reader.read_exact(&mut old_bytes_before)?;
            reader.read_exact(&mut buf)?;
            reader.read_exact(&mut old_bytes_after)?;
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
            if let Err(e) = table.save_record(2, &records[2], true) {
                assert!(false, "expected success but got error: {:?}", e)
            }
            reader.seek(SeekFrom::Start(0))?;
            let mut new_bytes_before = vec!(0u8; pos as usize);
            let mut new_bytes_after = vec!(0u8; ADD_FIELDS_RECORD_BYTES);
            reader.read_exact(&mut new_bytes_before)?;
            reader.read_exact(&mut buf)?;
            reader.read_exact(&mut new_bytes_after)?;
            assert_eq!(old_bytes_before, new_bytes_before);
            assert_eq!(expected, buf);
            assert_eq!(old_bytes_after, new_bytes_after);

            Ok(())
        });
    }

    #[test]
    fn save_record_into_without_fields() {
        with_tmpdir_and_table(&|_, table| {
            // create table and create expected table file contents
            let mut records = create_fake_table(&table.path, true)?;
            let mut expected = Vec::new();
            let file = File::open(&table.path)?;
            let mut reader = BufReader::new(file);
            reader.read_to_end(&mut expected)?;

            // test
            records[2].set("foo", Value::I32(11));
            records[2].set("bar", Value::Str("hello".to_string()));
            match table.save_record(2, &records[2], true) {
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
            let file = File::open(&table.path)?;
            let mut reader = BufReader::new(file);
            reader.read_to_end(&mut buf)?;
            assert_eq!(expected, buf);

            Ok(())
        });
    }

    #[test]
    fn healthcheck_new_table() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            // test healthcheck status
            let expected = Status::New;
            match table.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_new_index_with_empty_file() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            // test healthcheck status
            table.new_writer(true)?;
            let expected = Status::New;
            match table.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_corrupted_headers() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            let buf = [0u8; 5];
            create_file_with_bytes(&table.path, &buf)?;
            let expected = Status::Corrupted;
            match table.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_corrupted() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            let mut buf = [0u8; Header::BYTES+EMPTY_RECORD_HEADER_BYTES+5];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new("my_table", Some(fake_table_uuid()))?;
            header.record_count = 10;
            header.write_to(&mut writer)?;

            create_file_with_bytes(&table.path, &buf)?;
            add_fields(&mut table.record_header)?;
            assert_eq!(Status::Corrupted, table.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_good() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            create_fake_table(&table.path, false)?;
            assert_eq!(Status::Good, table.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_no_fields() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            let mut writer = table.new_writer(true)?;
            table.save_headers_into(&mut writer)?;
            assert_eq!(Status::NoFields, table.healthcheck()?);
            Ok(())
        });
    }

    #[test]
    fn save_headers_into() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            // create table file and read table header data
            create_fake_table(&table.path, false)?;
            let mut reader = table.new_reader()?;
            let size = Header::BYTES + 122;
            let mut expected = vec![0u8; size];
            reader.read_exact(&mut expected)?;
            reader.rewind()?;
            table.header.load_from(&mut reader)?;
            table.record_header.load_from(&mut reader)?;

            // test save table header
            let mut buf = vec![0u8; size];
            let wrt = &mut buf as &mut [u8];
            let mut writer = Cursor::new(wrt);
            if let Err(e) = table.save_headers_into(&mut writer) {
                assert!(false, "expected success but got error: {:?}", e);
            };
            assert_eq!(expected, buf);
            
            Ok(())
        });
    }

    #[test]
    fn save_headers() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            // create table file and read table header data
            create_fake_table(&table.path, false)?;
            let mut reader = table.new_reader()?;
            let size = Header::BYTES + 122;
            let mut expected = vec![0u8; size];
            reader.read_exact(&mut expected)?;
            reader.rewind()?;
            table.header.load_from(&mut reader)?;
            table.record_header.load_from(&mut reader)?;

            // test save table header
            assert_eq!(4, table.header.record_count);
            table.header.record_count = 5;
            if let Err(e) = table.save_headers() {
                assert!(false, "expected success but got error: {:?}", e);
            };
            table.header.record_count = 4;
            assert_eq!(4, table.header.record_count);
            reader.rewind()?;
            table.header.load_from(&mut reader)?;
            assert_eq!(5, table.header.record_count);
            
            Ok(())
        });
    }
}