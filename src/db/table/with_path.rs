use anyhow::{bail, Result};
use uuid::Uuid;
use regex::Regex;
use buf_read_write::BufStream;
use std::fs::File;
use std::path::PathBuf;
use crate::db::field::Record;
use crate::db::table::Header;
use crate::error::TableError;
use crate::Data;
use crate::traits::DataTrait;
use super::{Status, TableWithData, FILE_EXTENSION};

/// Table engine.
#[derive(Debug)]
pub struct TableWithPath {
    /// Cached table path
    pub path: PathBuf,

    /// Inner table
    pub inner: TableWithData<BufStream<File>, (), Data<BufStream<File>>>
}

impl<'table> TableWithPath {
    /// Returns a reference to the table header.
    pub fn header_ref(&self) -> &Header {
        self.inner.header_ref()
    }

    /// Returns a mutable reference to the table header.
    pub fn header_mut(&'table mut self) -> &'table mut Header {
        self.inner.header_mut()
    }

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
    /// * `truncate` - If `true` then truncate the file.
    /// * `name` - Table name.
    /// * `uuid` - Table UUID, if not provided a random UUID will be generated.
    pub fn create(path: PathBuf, truncate: bool, name: &str, uuid: Option<Uuid>) -> Result<Self> {
        let mut options = File::options();
        options.read(true)
            .write(true)
            .append(false);
        if truncate {
            options.truncate(true);
            options.create(true);
        } else {
            options.create_new(true);
        }
        let file = options.open(&path)?;
        Ok(Self{
            path,
            inner: TableWithData::new(Data::new(BufStream::new(file), false), name, uuid)?
        })
    }

    /// Loads a table from a file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Table file path.
    pub fn load(path: PathBuf) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .append(false)
            .truncate(false)
            .create(false)
            .open(&path)?;
        let mut table = Self{
            path,
            inner: TableWithData::load(Data::new(BufStream::new(file), false))?
        };
        match table.healthcheck() {
            Ok(v) => match v {
                Status::Good => Ok(table),
                Status::NoFields => Err(TableError::NoFields.into()),
                vu => Err(TableError::Unavailable(vu).into())
            },
            Err(e) => Err(e)
        }
    }

    /// Loads or creates the table.
    /// 
    /// # Arguments
    /// 
    /// * `override_on_error` - Overrides the table if corrupted instead of error.
    /// * `force_override` - Always creates a new table with the current headers.
    pub fn load_or_create(path: PathBuf, name: &str, uuid: Option<Uuid>, override_on_error: bool, force_override: bool) -> Result<Self> {
        if !force_override {
            match Self::healthcheck_exists(&path)? {
                Status::Good => return Self::load(path),
                Status::New => {},
                Status::NoFields => if !override_on_error {
                    bail!(TableError::NoFields)
                },
                vu => if !override_on_error {
                    bail!(TableError::Unavailable(vu))
                }
            }
        }
        Self::create(path, true, name, uuid)
    }

    /// Validates the table file.
    /// 
    /// # Returns
    /// 
    /// * `Status::Good` - Table is valid.
    /// * `Status::New` - Table is new.
    fn healthcheck_exists(path: &PathBuf) -> Result<Status> {
        match File::open(&path) {
            Ok(_) => Ok(Status::Good),
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    return Ok(Status::New)
                },
                _ => Err(e.into())
            }
        }
    }

    /// Perform a healthckeck over the table file.
    /// 
    /// # Returns
    /// 
    /// * `Status::Good` - Table is valid.
    /// * `Status::New` - Table is new.
    /// * `Status::NotFound` - Table file is not found.
    /// * `Status::NoFields` - Table has no fields.
    /// * `Status::Corrupted` - Table is corrupted.
    pub fn healthcheck(&mut self) -> Result<Status> {
        match Self::healthcheck_exists(&self.path)? {
            Status::Good => self.inner.healthcheck(),
            s => Ok(s)
        }
    }

    /// Returns the table binary size. This operation moves the reader to the end of the data.
    pub fn real_size(&mut self) -> Result<u64> {
        self.inner.real_size()
    }

    /// Loads the table's header.
    pub fn load_headers(&mut self) -> Result<()> {
        self.inner.load_headers()
    }

    /// Read the record from the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn record(&mut self, index: u64) -> Result<Option<Record>> {
        self.inner.record(index)
    }

    /// Prefill the stream with empty records.
    /// 
    /// # Arguments
    /// 
    /// * `record_count` - Number of records to prefill.
    pub fn fill_records(&mut self, record_count: u64) -> Result<()> {
        self.inner.fill_records(record_count)
    }

    /// Updates or append a record into the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Index value index.
    /// * `record` - Record to save.
    /// * `save_headers` - Headers will be saved on append when true.
    pub fn save_record(&mut self, index: u64, record: &Record, save_headers: bool) -> Result<()> {
        self.inner.save_record(index, record, save_headers)
    }

    /// Saves the headers and then jump back to the last writer stream position.
    pub fn save_headers(&mut self) -> Result<()> {
        self.inner.save_headers()
    }
}

#[cfg(test)]
pub mod test_helper {
    use std::fs::OpenOptions;
    use std::io::{BufWriter, Write};

    use super::*;
    use crate::test_helper::*;
    use crate::db::table::traits::table_test_helper::*;
    use crate::db::field::Record;
    use tempfile::TempDir;

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
    pub fn with_tmpdir_and_table(f: &impl Fn(&TempDir, &mut TableWithPath) -> Result<()>) {
        let sub = |dir: &TempDir| -> Result<()> {
            // create Table and execute
            let mut table = TableWithPath::create(
                dir.path().join("t.fmtable"),
                false,
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
    use std::io::{BufReader, Read, Seek, Write};
    use super::*;
    use crate::db::table::traits::table_test_helper::*;
    use crate::db::table::traits::table_test_helper::{fake_table_with_fields, fake_table_without_fields, FAKE_TABLE_BYTES};
    use crate::test_helper::with_tmpdir;

    #[test]
    fn header_ref() {
        with_tmpdir(&|dir| -> Result<()> {
            let path = dir.path().join("my_table.fmtable");
            let mut table = TableWithPath::create(path.clone(), false, "my_table", Some(fake_table_uuid()))?;
            table.inner.base.header.meta.record_count = 4;
            assert_eq!(table.header_ref().meta.record_count, 4);
            table.inner.base.header.meta.record_count = 3;
            assert_eq!(table.header_ref().meta.record_count, 3);
            Ok(())
        })
    }

    #[test]
    fn header_mut() {
        with_tmpdir(&|dir| -> Result<()> {
            let path = dir.path().join("my_table.fmtable");
            let mut table = TableWithPath::create(path.clone(), false, "my_table", Some(fake_table_uuid()))?;
            table.header_mut().meta.record_count = 4;
            assert_eq!(table.inner.base.header.meta.record_count, 4);
            table.header_mut().meta.record_count = 3;
            assert_eq!(table.inner.base.header.meta.record_count, 3);
            Ok(())
        })
    }

    #[test]
    fn file_extension_regex() {
        let rx = TableWithPath::file_extension_regex();
        assert!(rx.is_match("hello.fmtable"), "expected to match \"hello.fmtable\" but got false");
        assert!(rx.is_match("/path/to/hello.fmtable"), "expected to match \"/path/to/hello.fmtable\" but got false");
        assert!(!rx.is_match("hello.table"), "expected to not match \"hello.table\" but got true");
    }

    #[test]
    fn create() {
        with_tmpdir(&|dir| -> Result<()> {
            let expected_path = dir.path().join("my_table.fmtable");
            match TableWithPath::create(expected_path.clone(), false, "my_table_a", Some(fake_table_uuid())) {
                Ok(mut table) => {
                    // check path
                    assert_eq!(expected_path, table.path);

                    // check name
                    assert_eq!("my_table_a", table.inner.base.header.meta.get_name());

                    // check that is new
                    table.inner.data.seek(std::io::SeekFrom::End(0))?;
                    assert_eq!(0, table.inner.data.stream_position()?);

                    // check file and data
                    let expected: [u8; 5] = [
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX)
                    ];
                    table.inner.data.rewind()?;
                    table.inner.data.write_all(&expected)?;
                    table.inner.data.flush()?;
                    let file = File::open(&expected_path)?;
                    let mut reader = BufReader::new(file);
                    let mut buf = [0u8; 5];
                    reader.read_exact(&mut buf)?;
                    assert_eq!(expected, buf);
                },
                Err(e) => assert!(false, "expected table to be created but got error: {:?}", e)
            }
            Ok(())
        })
    }

    #[test]
    fn healthcheck_exists_new() {
        with_tmpdir(&|dir| -> Result<()> {
            let path = dir.path().join("my_table.fmtable");
            match TableWithPath::healthcheck_exists(&path) {
                Ok(Status::New) => assert!(true),
                Ok(v) => assert!(false, "expected Status::New but got {:?}", v),
                Err(e) => assert!(false, "expected Ok(Status::New) but got error: {:?} | {:?}", e.is::<std::io::Error>(), e)
            }
            Ok(())
        });
    }

    #[test]
    fn healthcheck_exists_exists() {
        with_tmpdir(&|dir| -> Result<()> {
            let path = dir.path().join("my_table.fmtable");
            File::create(path.clone())?;
            match TableWithPath::healthcheck_exists(&path) {
                Ok(Status::Good) => assert!(true),
                Ok(v) => assert!(false, "expected Status::Good but got {:?}", v),
                Err(e) => assert!(false, "expected Ok(Status::Good) but got error: {:?}", e)
            }
            Ok(())
        });
    }

    #[test]
    fn load_or_create_create_no_override_no_truncate() {
        with_tmpdir(&|dir| -> Result<()> {
            let expected_path = dir.path().join("my_table.fmtable");
            match TableWithPath::load_or_create(expected_path.clone(), "my_table", Some(fake_table_uuid()), false, false) {
                Ok(mut table) => {
                    // check path
                    assert_eq!(expected_path, table.path);

                    // check name
                    assert_eq!("my_table", table.inner.base.header.meta.get_name());

                    // check that is new
                    table.inner.data.seek(std::io::SeekFrom::End(0))?;
                    assert_eq!(0, table.inner.data.stream_position()?);

                    // check file and data are the same
                    let expected: [u8; 5] = [
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX)
                    ];
                    table.inner.data.rewind()?;
                    table.inner.data.write_all(&expected)?;
                    table.inner.data.flush()?;
                    let file = File::open(&expected_path)?;
                    let mut reader = BufReader::new(file);
                    let mut buf = [0u8; 5];
                    reader.read_exact(&mut buf)?;
                    assert_eq!(expected, buf);
                },
                Err(e) => assert!(false, "expected Ok and recreate as true but got error: {:?}", e) 
            };
            Ok(())
        });
    }

    #[test]
    fn load_or_create_exists_no_override_no_truncate() {
        with_tmpdir(&|dir| -> Result<()> {
            let expected_path = dir.path().join("my_table.fmtable");

            // create file with default table data
            let (expected_table_bytes, _, meta) = fake_table_with_fields(true)?;
            let mut file = File::create(expected_path.clone())?;
            file.write_all(&expected_table_bytes)?;
            file.flush()?;

            match TableWithPath::load_or_create(expected_path.clone(), "my_table", Some(fake_table_uuid()), false, false) {
                Ok(mut table) => {
                    // check path
                    assert_eq!(expected_path, table.path);

                    // check name
                    assert_eq!(meta.get_name(), table.inner.base.header.meta.get_name());

                    // check record_count
                    assert_eq!(meta.record_count, table.inner.base.header.meta.record_count);

                    // check file and data are the same
                    let mut buf = [0u8; FAKE_TABLE_BYTES];
                    table.inner.data.rewind()?;
                    table.inner.data.read_exact(&mut buf)?;
                    assert_eq!(expected_table_bytes, buf);
                },
                Err(e) => assert!(false, "expected Ok and recreate as true but got error: {:?}", e) 
            };
            Ok(())
        });
    }

    #[test]
    fn load_or_create_exists_no_fields_no_override_no_truncate() {
        with_tmpdir(&|dir| -> Result<()> {
            let expected_path = dir.path().join("my_table.fmtable");

            // create file with default table data
            let (expected_table_bytes, _meta) = fake_table_without_fields(true)?;
            let mut file = File::create(expected_path.clone())?;
            file.write_all(&expected_table_bytes)?;
            file.flush()?;

            match TableWithPath::load_or_create(expected_path.clone(), "my_table", Some(fake_table_uuid()), false, false) {
                Ok(table) => assert!(false, "expected Err(TableError::NoFields) but got Ok: {:?}", table),
                Err(e) =>  match e.downcast::<TableError>() {
                    Ok(TableError::NoFields) => assert!(true),
                    Ok(te) => assert!(false, "expected TableError::NoFields but got error: {:?}", te),
                    Err(ex) => assert!(false, "expected TableError::NoFields but got error: {:?}", ex)
                }
            };
            Ok(())
        });
    }

    #[test]
    fn load_or_create_exists_with_truncate_no_override() {
        with_tmpdir(&|dir| -> Result<()> {
            let expected_path = dir.path().join("my_table.fmtable");

            // create file to truncate
            let mut file = File::create(expected_path.clone())?;
            file.write_all(&[
                rand::random_range(0u8..u8::MAX),
                rand::random_range(0u8..u8::MAX),
                rand::random_range(0u8..u8::MAX),
                rand::random_range(0u8..u8::MAX),
                rand::random_range(0u8..u8::MAX)
            ])?;
            file.flush()?;

            match TableWithPath::load_or_create(expected_path.clone(), "my_table", Some(fake_table_uuid()), false, true) {
                Ok(mut table) => {
                    // check path
                    assert_eq!(expected_path, table.path);

                    // check name
                    assert_eq!("my_table", table.inner.base.header.meta.get_name());

                    // check that is new
                    table.inner.data.seek(std::io::SeekFrom::End(0))?;
                    assert_eq!(0, table.inner.data.stream_position()?);

                    // check file and data are the same
                    let expected: [u8; 5] = [
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX),
                        rand::random_range(0u8..u8::MAX)
                    ];
                    table.inner.data.rewind()?;
                    table.inner.data.write_all(&expected)?;
                    table.inner.data.flush()?;
                    let file = File::open(&expected_path)?;
                    let mut reader = BufReader::new(file);
                    let mut buf = [0u8; 5];
                    reader.read_exact(&mut buf)?;
                    assert_eq!(expected, buf);
                },
                Err(e) => assert!(false, "expected Ok and recreate as true but got error: {:?}", e) 
            };
            Ok(())
        });
    }
}