use anyhow::{bail, Result};
use serde::{Serialize};
use serde_json::{Map as JSMap, Value as JSValue};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
use crate::error::{ParseError, IndexError};
use crate::traits::{ReadFrom, WriteTo};
use super::indexer::{Indexer, Status as IndexStatus};
use super::indexer::value::{MatchFlag, Data as IndexData, Value as IndexValue};
use super::table::Table;
use super::table::record::Record;

/// Represents a data source single record.
#[derive(Debug, Serialize, PartialEq)]
pub struct Data {
    pub input: JSMap<String, JSValue>,
    pub index: IndexData,
    pub record: Record
}

/// Represents a source readers involved in a join operation.
pub struct SourceJoinItem<R, T> {
    pub index: R,
    pub table: T
}

impl SourceJoinItem<BufReader<File>, BufReader<File>> {
    /// Creates a new instance as reader from a source.
    /// 
    /// # Arguments
    /// 
    /// * `source` - Source to create the readers from.
    pub fn as_reader_from(source: &Source) -> Result<Self> {
        Ok(Self{
            index: source.index.new_index_reader()?,
            table: source.table.new_reader()?
        })
    }
}

impl SourceJoinItem<BufWriter<File>, BufWriter<File>> {
    /// Creates a new instance as writer from a source.
    /// 
    /// # Arguments
    /// 
    /// * `source` - Source to create the writers from.
    pub fn as_writer_from(source: &Source, create: bool) -> Result<Self> {
        Ok(Self{
            index: source.index.new_index_writer(create)?,
            table: source.table.new_writer(create)?
        })
    }
}

/// Represents a data source.
#[derive(Debug, Clone)]
pub struct Source {
    /// Indexer.
    pub index: Indexer,
    /// Table.
    pub table: Table
}

impl Source {
    /// Regenerates the index file based on the input file.
    /// 
    /// # Arguments
    /// 
    /// * `override_on_error` - Overrides the index or table file if corrupted instead of error.
    /// * `force_override` - Always creates a new table file with the current headers.
    pub fn init(&mut self, override_on_error: bool, force_override: bool) -> Result<()> {
        if let Err(e) = self.index.index() {
            match e.downcast::<IndexError>() {
                Ok(ex) => match ex {
                    IndexError::Unavailable(status) => match status {
                        IndexStatus::Indexing => bail!(IndexError::Unavailable(IndexStatus::Indexing)),
                        _ => if override_on_error {
                            // truncate the file then index again
                            let file = OpenOptions::new()
                                .create(true)
                                .open(&self.index.index_path)?;
                            file.set_len(0)?;
                            self.index.index()?;
                        }
                    },
                    err => bail!(err)
                },
                Err(ex) => bail!(ex)
            }
        }
        if self.table.header.record_count < 1 {
            self.table.header.record_count = self.index.header.indexed_count;
        }
        self.table.load_or_create(override_on_error, force_override)?;
        Ok(())
    }

    /// Search the next unprocessed record an return the index if any.
    /// 
    /// # Arguments
    /// 
    /// * `from_index` - Index offset from which start searching.
    pub fn find_pending(&self, from_index: u64) -> Result<Option<u64>> {
        self.index.find_pending(from_index)
    }

    /// Retrive a record input data from a specific index.
    /// 
    /// $ Arguments
    /// 
    /// * `index` - Record index.
    pub fn data(&self, index: u64) -> Result<Option<Data>> {
        let index_value = match self.index.value(index)? {
            Some(v) => v,
            None => return Ok(None)
        };
        let record = match self.table.record(index)? {
            Some(v) => v,
            None => return Ok(None)
        };
        let input_data = self.index.parse_input(&index_value)?;
        Ok(Some(Data{
            index: index_value.data,
            record,
            input: input_data
        }))
    }

    /// Check if the source is indexed.
    pub fn is_indexed(&self) -> bool {
        // check that the index has been indexed
        if !self.index.header.indexed {
            return false;
        }

        // check that the indexed count match the record count
        if self.index.header.indexed_count != self.table.header.record_count {
            return false;
        }
        true
    }

    /// Check if the source is compatible for joining with this source.
    /// 
    /// # Arguments
    /// 
    /// * `source` - Source to check compatibility.
    pub fn is_join_compatible(&self, source: &Source) -> (bool, &str) {
        // ensure sources are indexed
        if !self.is_indexed() || !source.is_indexed() {
            return (false, "should be indexed");
        }

        // ensure sources belong to the same input
        if self.index.header.hash != source.index.header.hash {
            return (false, "should have the same input hash")
        }

        // ensure sources have the same record count
        if self.index.header.indexed_count != source.index.header.indexed_count {
            return (false, "indexed count doesn't match")
        }

        // ensure tables has the same fields count
        if self.table.record_header.len() != source.table.record_header.len() {
            return (false, "table field count doesn't match")
        }

        // ensure tables has the same fields
        let limit = self.table.record_header.len();
        if limit > 0 {
            for i in 0..limit {
                if self.table.record_header.get_by_index(i) != source.table.record_header.get_by_index(i) {
                    return (false, "table fields doesn't match")
                }
            }
        }

        return (true, "")
    }

    /// Join index files into a single one using a >50% rule to decide on match flags.
    /// 
    /// # Arguments
    /// 
    /// * `index_path` - Target index file path.
    /// * `table_path` - Target table file path.
    /// * `sources` - Source list to join.
    pub fn join(index_path: &PathBuf, table_path: &PathBuf, sources: &[Source]) -> Result<Source> {
        // validate source list size
        let limit = sources.len();
        if limit < 1 {
            bail!("can't merge an empty source list")
        }
        if limit < 2 {
            bail!("join requires at least 2 sources")
        }
        let base_source = sources[0].clone();

        // validate source list index and table
        let err_msg = "source files can't be the same as the target files";
        let target_file_paths = [index_path, table_path];
        for path in target_file_paths {
            if &base_source.index.index_path == path || &base_source.table.path == path {
                bail!(err_msg)
            }
        }
        for i in 1..limit {
            // ensure all sources are indexed
            let (compatible, reason) = base_source.is_join_compatible(&sources[1]);
            if !compatible {    
                bail!("sources aren't join compatible: {}", reason)
            }
            
            // ensure the target files aren't the same as any source
            for path in target_file_paths {
                if &sources[i].index.index_path == path || &sources[i].table.path == path {
                    bail!(err_msg)
                }
            }
        }

        // create target source
        let mut target = base_source.clone();
        target.index.index_path = index_path.clone();
        target.table.path = table_path.clone();

        // create target writers and write the target headers
        let mut target_wrt = SourceJoinItem::as_writer_from(&target, true)?;
        target.index.save_header_into(&mut target_wrt.index)?;
        target.table.save_headers_into(&mut target_wrt.table)?;

        // move target writers to the first record position
        let index_pos = Indexer::calc_value_pos(0);
        let table_pos = target.table.calc_record_pos(0);
        target_wrt.index.seek(SeekFrom::Start(index_pos))?;
        target_wrt.table.seek(SeekFrom::Start(table_pos))?;

        // create readers
        let mut readers = Vec::new();
        for source in sources.iter() {
            let mut source_rdr = SourceJoinItem::as_reader_from(source)?;
            source_rdr.index.seek(SeekFrom::Start(index_pos))?;
            source_rdr.table.seek(SeekFrom::Start(table_pos))?;
            readers.push(source_rdr);
        }
        let mut base_reader = SourceJoinItem::as_reader_from(&base_source)?;
        base_reader.index.seek(SeekFrom::Start(index_pos))?;
        base_reader.table.seek(SeekFrom::Start(table_pos))?;

        // iterate and join sources
        let total_sources = sources.len() as f64;
        let match_values = MatchFlag::as_array();
        let record_size = target.table.record_header.record_byte_size() as usize;
        let mut base_record_buf = vec![0u8; record_size as usize];
        let mut record_buf = vec![0u8; record_size as usize];
        for index in 0..target.index.header.indexed_count {
            // initialize hash maps
            let mut matches: HashMap<u8, f64> = HashMap::new();
            let mut samples: HashMap<u8, Vec<u8>> = HashMap::new();
            for k in match_values {
                matches.insert(k.into(), 0f64);
            }

            // create base index data
            let mut base_value = IndexValue::read_from(&mut base_reader.index)?;

            // read base record bytes
            base_reader.table.read_exact(&mut base_record_buf)?;

            // iterate source readers and count match flag values
            let mut spent_time = 0;
            for i in 0..limit {
                // get and validate source index value
                let value = IndexValue::read_from(&mut readers[i].index)?;
                if base_value.input_start_pos != value.input_start_pos || base_value.input_end_pos != value.input_end_pos {
                    bail!("source index value doesn't match base value at record {}", index);
                }
                let match_flag_byte: u8 = value.data.match_flag.into();

                // record match flag counter
                let count = match matches.get_mut(&match_flag_byte) {
                    Some(v) => v,
                    None => bail!(ParseError::InvalidValue)
                };
                *count += 1f64;

                // sample source
                readers[i].table.read_exact(&mut record_buf)?;
                if let None = samples.get(&match_flag_byte) {
                    samples.insert(match_flag_byte, record_buf.clone());
                }

                // keep track of spent time
                spent_time += value.data.spent_time;
            }

            // calculate match_flag value and average spent time
            let mut match_flag = MatchFlag::None;
            for k in match_values {
                if *matches.get(&k.into()).unwrap() / total_sources > 0.5 {
                    match_flag = k;
                    break;
                }
            }
            if match_flag == MatchFlag::Skip {
                match_flag = MatchFlag::None
            }
            base_value.data.match_flag = match_flag;
            base_value.data.spent_time = (spent_time as f64 / total_sources) as u64;

            // save index value into target
            base_value.write_to(&mut target_wrt.index)?;

            // save sample target record into target
            let buf = match samples.get(&match_flag.into()) {
                Some(v) => v,
                None => &base_record_buf
            };
            target_wrt.table.write_all(&buf)?;
        }
        Ok(target)
    }
}

#[cfg(test)]
mod test_helper {
    use super::*;
    use tempfile::TempDir;
    use crate::test_helper::*;
    use crate::db::indexer::header::InputType;

    /// Execute a function with both a temp directory and a new Source.
    /// 
    /// # Arguments
    /// 
    /// * `f` - Function to execute.
    pub fn with_tmpdir_and_source(f: &impl Fn(&TempDir, &mut Source) -> Result<()>) {
        let sub = |dir: &TempDir| -> Result<()> {
            // generate default file names for files
            let input_path = dir.path().join("i.csv");
            let index_path = dir.path().join("i.fmindex");
            let table_path = dir.path().join("t.fmtable");

            // create source
            let mut source = Source{
                index: Indexer::new(
                    input_path,
                    index_path,
                    InputType::Unknown
                ),
                table: Table::new(
                    table_path,
                    "my_table"
                )?
            };

            // execute function
            match f(&dir, &mut source) {
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
    use super::test_helper::*;
    // use crate::test_helper::*;
    use crate::db::indexer::test_helper::{create_fake_index};
    use crate::db::table::test_helper::create_fake_table;
    use crate::db::indexer::header::{Header as IndexHeader};
    use crate::db::table::header::{Header as TableHeader};
    use crate::db::table::record::header::{Header as RecordHeader};

    mod source_join_item {
        use super::*;

        #[test]
        fn as_reader_from() {
            with_tmpdir_and_source(&|_, source| -> Result<()> {
                create_fake_index(&source.index.index_path, true)?;
                create_fake_table(&source.table.path, true)?;

                // test method
                let mut readers = SourceJoinItem::as_reader_from(&source)?;

                // test index reader
                let mut index_rdr = source.index.new_index_reader()?;
                let expected = IndexHeader::read_from(&mut index_rdr)?;
                source.index.load_header_from(&mut readers.index)?;
                assert_eq!(expected, source.index.header);
                
                // test table reader
                let mut table_rdr = source.table.new_reader()?;
                let expected = TableHeader::read_from(&mut table_rdr)?;
                source.table.load_headers_from(&mut readers.table)?;
                assert_eq!(expected, source.table.header);
                let expected = RecordHeader::read_from(&mut table_rdr)?;
                assert_eq!(expected, source.table.record_header);

                Ok(())
            });
        }
    }
}