use anyhow::{bail, Result};
use uuid::Uuid;
use regex::Regex;
use buf_read_write::BufStream;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use crate::{file_size, fill_file};
use crate::error::TableError;
use crate::traits::{ByteSized, WriteTo};
use super::{Header, Status, Table};

/// Table engine version.
pub const VERSION: u32 = 2;

/// Table file extension.
pub const FILE_EXTENSION: &str = "fmtable";

/// Table engine.
#[derive(Debug)]
pub struct TableFile {
    /// Cached table path
    pub path: PathBuf,

    data: Table<File, TableSource<BufStream<File>>>
}

impl TableFile {
    /// Generates a regex expression to validate the index file extension.
    fn file_extension_regex() -> Regex {
        let expression = format!(r"(?i)\.{}$", FILE_EXTENSION);
        Regex::new(&expression).unwrap()
    }

    /// Create a new table instance.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Table file path.
    /// * `name` - Table name.
    pub fn create(path: PathBuf, truncate: bool, name: &str, uuid: Option<Uuid>) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .append(false)
            .truncate(truncate)
            .create(true)
            .open(&path)?;
        Ok(Self{
            path,
            file: BufStream::new(file),
            header: Header::new(name, uuid)?
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
            file: BufStream::new(file),
            header: Header::new("", Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?
        };
        match table.healthcheck() {
            Ok(v) => match v {
                Status::Good => Ok(table),
                vu => bail!(TableError::Unavailable(vu))
            },
            Err(e) => Err(e)
        }
    }
}

impl<'table> TableTrait<BufStream<File>> for TableFile {
    fn data_ref(&self) -> &BufStream<File> {
        &self.file
    }

    fn data_mut(&mut self) -> &mut BufStream<File> {
        &mut self.file
    }

    fn header_ref(&self) -> &Header {
        &self.header
    }

    fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }

    fn real_size(&self) -> Result<u64> {
        Ok(file_size(&self.path)?)
    }

    /// Validates the table file.
    /// 
    /// # Returns
    /// 
    /// * `Status::Good` - Table is valid.
    /// * `Status::New` - Table is new.
    fn healthcheck_binary(&mut self) -> Result<Status> {
        match File::open(&self.path) {
            Ok(_) => Ok(Status::Good),
            Err(e) => match e.downcast::<std::io::Error>() {
                Ok(ex) => match ex.kind() {
                    std::io::ErrorKind::NotFound => {
                        return Ok(Status::New)
                    },
                    _ => Err(ex.into())
                },
                Err(ex) => Err(ex.into())
            }
        }
    }

    fn recreate(&mut self) -> Result<()> {
        let size = self.calc_record_pos(self.header_ref().meta.record_count);
        self.file.
        fill_file(&self.path, size, true)?;
        let mut writer = self.file;
        self.save_headers_into(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    fn_inner_write_header!()
}

#[cfg(test)]
mod test_helper {
    use super::*;
    use crate::test_helper::*;
    use crate::db::table::traits::test_helper::*;
    use crate::db::field::Record;
    use tempfile::TempDir;

    /// Create a fake table file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Table file path.
    /// * `empty` - If `true` then build all records as empty records.
    fn create_fake_table(path: &PathBuf, unprocessed: bool) -> Result<Vec<Record>> {
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
    pub fn with_tmpdir_and_table(f: &impl Fn(&TempDir, &mut TableFile) -> Result<()>) {
        let sub = |dir: &TempDir| -> Result<()> {
            // create Table and execute
            let mut table = TableFile::new(
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
    use crate::db::table::traits::test_helper::*;
    use super::test_helper::*;

    #[test]
    fn file_extension_regex() {
        let rx = TableFile::file_extension_regex();
        assert!(rx.is_match("hello.fmtable"), "expected to match \"hello.fmtable\" but got false");
        assert!(rx.is_match("/path/to/hello.fmtable"), "expected to match \"/path/to/hello.fmtable\" but got false");
        assert!(!rx.is_match("hello.table"), "expected to not match \"hello.table\" but got true");
    }

    #[test]
    fn new() {
        let header = Header::new("my_table", Some(fake_table_uuid())).unwrap();
        let expected = TableFile{
            path: "my_table.fmtable".into(),
            header,
        };
        match TableFile::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }
}