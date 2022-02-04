pub mod header;
pub mod value;

use anyhow::{bail, Result};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use crate::error::ParseError;
use crate::{file_size, generate_hash};
use crate::traits::{ByteSized, LoadFrom, ReadFrom, WriteTo};
use super::record::{Header as RecordHeader, Record, Value};
use header::{Header as IndexHeader, HASH_SIZE};
use value::{MatchFlag, Value as IndexValue};

/// Indexer version.
const VERSION: u32 = 2;

/// Default indexing batch size before updating headers.
const DEFAULT_INDEXING_BATCH_SIZE: u64 = 100;

/// index healthcheck status.
#[derive(Debug, PartialEq)]
pub enum IndexStatus {
    New,
    Indexed,
    Incomplete,
    Corrupted,
    Indexing,
    WrongInputFile
}

impl Display for IndexStatus{
    fn fmt(&self, f: &mut Formatter) -> FmtResult { 
        write!(f, "{}", match self {
            Self::New => "new",
            Self::Indexed => "indexed",
            Self::Incomplete => "incomplete",
            Self::Corrupted => "corrupted",
            Self::Indexing => "indexing",
            Self::WrongInputFile => "wrong input file"
        })
    }
}

/// Indexer engine.
#[derive(Debug, PartialEq)]
pub struct Indexer {
    /// Input file path.
    pub input_path: PathBuf,

    /// Index file path.
    pub index_path: PathBuf,

    /// Index header data.
    pub index_header: IndexHeader,

    /// Record header data. It contains information about the custom fields.
    pub record_header: RecordHeader,

    /// Indexing batch size before updating the index header.
    pub indexing_batch_size: u64
}

impl Indexer {
    /// Create a new indexer object with default empety extra fields.
    /// 
    /// # Arguments
    /// 
    /// * `input_path` - Source Input file path.
    /// * `index_path` - Target index file path.
    pub fn new(input_path: &str, index_path: &str) -> Self {
        Self{
            input_path: input_path.to_string(),
            index_path: index_path.to_string(),
            index_header: IndexHeader::new(),
            record_header: RecordHeader::new(),
            indexing_batch_size: DEFAULT_INDEXING_BATCH_SIZE
        }
    }

    /// Returns an input file buffered reader.
    pub fn new_input_reader(&self) -> Result<BufReader<File>> {
        let file = File::open(&self.input_path)?;
        Ok(BufReader::new(file))
    }

    /// Returns an index file buffered reader.
    pub fn new_index_reader(&self) -> Result<BufReader<File>> {
        let file = File::open(&self.index_path)?;
        Ok(BufReader::new(file))
    }

    /// Returns an index file buffered writer.
    /// 
    /// # Arguments
    /// 
    /// * `create` - Set to `true` when the file should be created.
    pub fn new_index_writer(&self, create: bool) -> Result<BufWriter<File>> {
        let mut options = OpenOptions::new();
        options.write(true);
        if create {
            options.create(true);
        }
        let file = options.open(&self.index_path)?;
        Ok(BufWriter::new(file))
    }

    /// Calculate the target record position at the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn calc_record_pos(&self, index: u64) -> u64 {
        let data_size = IndexValue::BYTES as u64 + self.record_header.record_byte_size() as u64;
        IndexHeader::BYTES as u64 + self.record_header.size_as_bytes() + index * data_size
    }

    /// Get the record's index headers.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    pub fn load_headers_from(&mut self, reader: &mut (impl Read + Seek)) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        self.index_header.load_from(reader)?;
        self.record_header.load_from(reader)?;
        Ok(())
    }

    /// Read both the index value and record from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    pub fn read_record_from(&self, reader: &mut (impl Read + Seek), index: u64) -> Result<Option<(IndexValue, Record)>> {
        let pos = self.calc_record_pos(index);

        // move to record position
        if let Err(e) = reader.seek(SeekFrom::Start(pos)) {
            match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    if self.index_header.indexed {
                        return Ok(None)
                    }
                    bail!(ParseError::Unavailable(IndexStatus::Indexing))
                },
                _ => bail!(e)
            }
        }
        
        // read index value data
        let index_value = match IndexValue::read_from(reader) {
            Ok(v) => v,
            Err(e) => match e.downcast::<std::io::Error>() {
                Ok(ex) => match ex.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        if self.index_header.indexed {
                            return Ok(None);
                        }
                        bail!(ParseError::Unavailable(IndexStatus::Indexing));
                    },
                    _ => bail!(ex)
                },
                Err(ex) => bail!(ex)
            }
        };
        
        // read record value data
        let record = match self.record_header.read_record(reader) {
            Ok(v) => v,
            Err(e) => match e.downcast::<std::io::Error>() {
                Ok(ex) => match ex.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        if self.index_header.indexed {
                            return Ok(None);
                        }
                        bail!(ParseError::Unavailable(IndexStatus::Indexing));
                    },
                    _ => bail!(ex)
                },
                Err(ex) => bail!(ex)
            }
        };

        Ok(Some((index_value, record)))
    }

    /// Read both the index value and record from the index file.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    pub fn record(&self, index: u64) -> Result<Option<(IndexValue, Record)>> {
        let mut reader = self.new_index_reader()?;
        self.read_record_from(&mut reader, index)
    }

    /// Updates a record date into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - File writer to save data into.
    /// * `index` - Index value index.
    /// * `value` - Index value to save.
    pub fn write_record_into(&self, writer: &mut (impl Write + Seek), index: u64, index_value: &IndexValue, record: &Record) -> Result<()> {
        let pos = self.calc_record_pos(index);
        writer.seek(SeekFrom::Start(pos))?;
        index_value.write_to(writer)?;
        self.record_header.write_record(writer, record)?;
        Ok(())
    }

    /// Updates or append both an index value and record into the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Index value index.
    /// * `index_value` - Index value to save.
    /// * `record` - Record to save
    pub fn save_record(&self, index: u64, index_value: &IndexValue, record: &Record) -> Result<()> {
        let mut writer = self.new_index_writer(false)?;
        self.write_record_into(&mut writer, index, index_value, record)?;
        writer.flush()?;
        Ok(())
    }

    /// Return the index and index value of the closest non matched record.
    /// 
    /// # Arguments
    /// 
    /// * `from_index` - Index offset as search starting point.
    pub fn find_unmatched(&self, from_index: u64) -> Result<Option<u64>> {
        // validate indexed
        if !self.index_header.indexed {
            bail!(ParseError::Unavailable(IndexStatus::Incomplete));
        }

        // validate index size
        if self.index_header.indexed_count < 1 {
            return Ok(None);
        }

        // find index size
        let size = self.calc_record_pos(self.index_header.indexed_count);

        // seek start point by using the provided offset
        let mut reader = self.new_index_reader()?;
        let mut index = from_index;
        let mut pos = self.calc_record_pos(index);
        reader.seek(SeekFrom::Start(pos))?;

        // search next unmatched record
        let buf_size = self.calc_record_pos(1) - self.calc_record_pos(0);
        let mut buf = vec![0u8; buf_size as usize];
        while pos < size {
            reader.read_exact(&mut buf)?;
            if buf[IndexValue::MATCH_FLAG_BYTE_INDEX] < 1u8 {
                return Ok(Some(index));
            }
            index += 1;
            pos += buf_size;
        }

        Ok(None)
    }

    /// Perform a healthckeck over the index file by reading
    /// the headers and checking the file size.
    pub fn healthcheck(&mut self) -> Result<IndexStatus> {
        // calculate the input hash
        let hash = generate_hash(&self.input_path)?;

        // check whenever index file exists
        match self.new_index_reader() {
            // try to load the index headers
            Ok(mut reader) => if let Err(e) = self.load_headers_from(&mut reader) {
                match e.downcast::<std::io::Error>() {
                    Ok(ex) => match ex.kind() {
                        std::io::ErrorKind::UnexpectedEof => {
                            // EOF eror means the index is corrupted
                            return Ok(IndexStatus::Corrupted);
                        },
                        _ => bail!(ex)
                    },
                    Err(ex) => return Err(ex)
                }
            },
            Err(e) => match e.downcast::<std::io::Error>() {
                Ok(ex) => match ex.kind() {
                    std::io::ErrorKind::NotFound => {
                        // store hash and return as new index
                        self.index_header.hash = Some(hash);
                        return Ok(IndexStatus::New)
                    },
                    _ => bail!(ex)
                },
                Err(ex) => bail!(ex)
            }
        };
        
        // validate headers
        match self.index_header.hash {
            Some(saved_hash) => {
                // validate input file hash
                if saved_hash != hash {
                    return Ok(IndexStatus::WrongInputFile);
                }
            },
            None => {
                // not having a hash means the index is corrupted
                return Ok(IndexStatus::Corrupted)
            }
        }

        // validate incomplete index
        if !self.index_header.indexed {
            return Ok(IndexStatus::Incomplete);
        }

        // validate corrupted index
        let real_size = file_size(&self.index_path)?;
        let expected_size = self.calc_record_pos(self.index_header.indexed_count);
        if real_size != expected_size {
            // sizes don't match, the file is corrupted
            return Ok(IndexStatus::Corrupted);
        }

        // all good, the index is indexed
        Ok(IndexStatus::Indexed)
    }

    /// Get the latest indexed record.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    fn last_indexed_record(&self, reader: &mut (impl Read + Seek)) -> Result<Option<(IndexValue, Record)>> {
        if self.index_header.indexed_count < 1 {
            return Ok(None);
        }
        self.read_record_from(reader, self.index_header.indexed_count - 1)
    }

    /// Saves the index header and then jump back to the last writer stream position.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    fn save_index_header(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.flush()?;
        let old_pos = writer.stream_position()?;
        writer.rewind()?;
        self.index_header.write_to(writer)?;
        writer.flush()?;
        writer.seek(SeekFrom::Start(old_pos))?;
        Ok(())
    }

    /// Process a CSV item into an IndexValue.
    /// 
    /// # Arguments
    /// 
    /// * `iter` - CSV iterator.
    /// * `item` - Last CSV item read from the iterator.
    /// * `input_reader` - Input navigation reader used to adjust positions.
    fn index_record(&self, iter: &csv::StringRecordsIter<BufReader<File>>, item: csv::StringRecord, input_reader: &mut (impl Read + Seek)) -> Result<IndexValue> {
        // calculate input positions
        let mut start_pos = item.position().unwrap().byte();
        let mut end_pos = iter.reader().position().byte();
        let length: usize = (end_pos - start_pos) as usize;

        // read CSV file line and store it on the buffer
        let mut buf: Vec<u8> = vec![0u8; length];
        input_reader.seek(SeekFrom::Start(start_pos))?;
        input_reader.read_exact(&mut buf)?;

        // remove new line at the beginning and end of buffer
        let mut limit = buf.len();
        let mut start_index = 0;
        for _ in 0..2 {
            if limit - start_index + 1 < 1 {
                break;
            }
            if buf[limit-1] == b'\n' || buf[limit-1] == b'\r' {
                end_pos -= 1;
                limit -= 1;
            }
            if limit - start_index + 1 < 1 {
                break;
            }
            if buf[start_index] == b'\n' || buf[start_index] == b'\r' {
                start_pos += 1;
                start_index += 1;
            }
        }

        // create index value
        Ok(IndexValue{
            input_start_pos: start_pos,
            input_end_pos: end_pos,
            spent_time: 0,
            match_flag: MatchFlag::None
        })
    }

    /// Index a new or incomplete index by tracking each item position
    /// from the input file.
    pub fn index(&mut self) -> Result<()> {
        // create reader and writer buffers
        let mut input_rdr = self.new_input_reader()?;
        let mut input_rdr_nav = self.new_input_reader()?;
        let mut index_wrt = self.new_index_writer(true)?;
        let mut is_first = true;

        // perform index healthcheck
        match self.healthcheck() {
            Ok(v) => match v {
                IndexStatus::Indexed => return Ok(()),
                IndexStatus::Incomplete => {
                    // read last indexed record or create the index file
                    let mut reader = self.new_index_reader()?;
                    match self.last_indexed_record(&mut reader)? {
                        Some((v, _)) => {
                            // load last known indexed value position
                            is_first = false;
                            input_rdr.seek(SeekFrom::Start(v.input_end_pos + 1))?;
                            let next_pos = self.calc_record_pos(self.index_header.indexed_count);
                            index_wrt.seek(SeekFrom::Start(next_pos))?;
                        },
                        None => {}
                    }
                },
                IndexStatus::New => {
                    // create index headers
                    self.index_header.write_to(&mut index_wrt)?;
                    self.record_header.write_to(&mut index_wrt)?;
                    index_wrt.flush()?;
                }
                vu => bail!(ParseError::Unavailable(vu))
            },
            Err(e) => return Err(e)
        }
        
        // index records
        let mut input_csv = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(input_rdr);
        let mut iter = input_csv.records();
        loop {
            // break when no more items
            let item = iter.next();
            if item.is_none() {
                break;
            }

            // skip CSV headers
            if is_first {
                is_first = false;
                continue;
            }

            // create index value
            let value = match item.unwrap() {
                Ok(v) => self.index_record(&iter, v, &mut input_rdr_nav)?,
                Err(e) => bail!(ParseError::from(e))
            };

            // write index value for this record
            value.write_to(&mut index_wrt)?;
            self.index_header.indexed_count += 1;

            // save headers every batch
            if self.index_header.indexed_count % self.indexing_batch_size < 1 {
                self.save_index_header(&mut index_wrt)?;
            }
        }

        // write headers
        self.index_header.indexed = true;
        self.save_index_header(&mut index_wrt)?;

        Ok(())
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;
    use crate::test_helper::*;
    use crate::db::record::header::{FieldType, Field};
    use crate::db::indexer::header::test_helper::{random_hash, build_header_bytes};
//     use crate::index::index_header::test_helper::build_INDEX_HEADER_BYTES;
//     use crate::index::index_value::test_helper::build_value_bytes;
//     use crate::index::index_header::HASH_SIZE;
//     use tempfile::TempDir;
//     use std::io::{Write, BufWriter};

    /// It's the size of a record header without any field.
    pub const EMPTY_RECORD_HEADER_BYTES: usize = u32::BYTES;

    /// Record header size generated by add_fields function.
    pub const ADD_FIELDS_HEADER_BYTES: usize = Field::BYTES * 2 + u32::BYTES;

    /// Record size generated by aADD_FIELDS_RECORD_BYTESdd_fields function.
    pub const ADD_FIELDS_RECORD_BYTES: usize = IndexValue::BYTES + 13;

    /// Fake records bytes size generated by fake_records.
    pub const FAKE_RECORDS_BYTES: usize = ADD_FIELDS_RECORD_BYTES * 3;

    /// Fake records without fields bytes.
    pub const FAKE_RECORDS_WITHOUT_FIELDS_BYTES: usize = IndexValue::BYTES * 3;

    /// Fake index with fields byte size.
    pub const FAKE_INDEX_BYTES: usize = IndexHeader::BYTES + ADD_FIELDS_HEADER_BYTES + FAKE_RECORDS_BYTES;

    /// Fake index without fields byte size.
    pub const FAKE_INDEX_WITHOUT_FIELDS_BYTES: usize = IndexHeader::BYTES + EMPTY_RECORD_HEADER_BYTES + FAKE_RECORDS_WITHOUT_FIELDS_BYTES;

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
        // start_pos
        0, 0, 0, 0, 0, 0, 0, 50u8,
        // end_pos
        0, 0, 0, 0, 0, 0, 0, 100u8,
        // spent_time
        0, 0, 0, 0, 0, 0, 0, 150u8,
        // match flag
        b'Y',
        // foo field
        13u8, 246u8, 33u8, 122u8,
        // bar field
        0, 0, 0, 3u8, 97u8, 98u8, 99u8, 0, 0,

        // second record
        // start_pos
        0, 0, 0, 0, 0, 0, 0, 200u8,
        // end_pos
        0, 0, 0, 0, 0, 0, 0, 250u8,
        // spent_time
        0, 0, 0, 0, 0, 0, 1u8, 44u8,
        // match flag
        0,
        // foo field
        20u8, 149u8, 141u8, 65u8,
        // bar field
        0, 0, 0, 4u8, 100u8, 102u8, 101u8, 103u8, 0,

        // third record
        // start_pos
        0, 0, 0, 0, 0, 0, 1u8, 94u8,
        // end_pos
        0, 0, 0, 0, 0, 0, 1u8, 144u8,
        // spent_time
        0, 0, 0, 0, 0, 0, 1u8, 194u8,
        // match flag
        b'S',
        // foo field
        51u8, 29u8, 39u8, 30u8,
        // bar field
        0, 0, 0, 5u8, 104u8, 105u8, 49u8, 50u8, 51u8
    ];

    pub const FAKE_RECORDS_WITHOUT_FIELDS_BYTE_SLICE: [u8; FAKE_RECORDS_WITHOUT_FIELDS_BYTES] = [
        // first record
        // start_pos
        0, 0, 0, 0, 0, 0, 0, 50u8,
        // end_pos
        0, 0, 0, 0, 0, 0, 0, 100u8,
        // spent_time
        0, 0, 0, 0, 0, 0, 0, 150u8,
        // match flag
        b'Y',

        // second record
        // start_pos
        0, 0, 0, 0, 0, 0, 0, 200u8,
        // end_pos
        0, 0, 0, 0, 0, 0, 0, 250u8,
        // spent_time
        0, 0, 0, 0, 0, 0, 1u8, 44u8,
        // match flag
        0,

        // third record
        // start_pos
        0, 0, 0, 0, 0, 0, 1u8, 94u8,
        // end_pos
        0, 0, 0, 0, 0, 0, 1u8, 144u8,
        // spent_time
        0, 0, 0, 0, 0, 0, 1u8, 194u8,
        // match flag
        b'S'
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
    pub fn fake_records() -> Result<Vec<(IndexValue, Record)>> {
        let mut header = RecordHeader::new();
        add_fields(&mut header)?;
        let mut records = Vec::new();

        // add first record
        let index_value = IndexValue{
            input_start_pos: 50,
            input_end_pos: 100,
            match_flag: MatchFlag::Yes,
            spent_time: 150
        };
        let mut record = header.new_record()?;
        record.set(header.get_by_index(0).unwrap(), Value::I32(234234234i32))?;
        record.set(header.get_by_index(1).unwrap(), Value::Str("abc".to_string()))?;
        records.push((index_value, record));

        // add second record
        let index_value = IndexValue{
            input_start_pos: 200,
            input_end_pos: 250,
            match_flag: MatchFlag::None,
            spent_time: 300
        };
        let mut record = header.new_record()?;
        record.set(header.get_by_index(0).unwrap(), Value::I32(345345345i32))?;
        record.set(header.get_by_index(1).unwrap(), Value::Str("dfeg".to_string()))?;
        records.push((index_value, record));

        // add third record
        let index_value = IndexValue{
            input_start_pos: 350,
            input_end_pos: 400,
            match_flag: MatchFlag::Skip,
            spent_time: 450
        };
        let mut record = header.new_record()?;
        record.set(header.get_by_index(0).unwrap(), Value::I32(857548574i32))?;
        record.set(header.get_by_index(1).unwrap(), Value::Str("hi123".to_string()))?;
        records.push((index_value, record));

        Ok(records)
    }

    /// Create fake records without fields.
    /// 
    /// # Arguments
    /// 
    /// * `records` - Record vector to add records into.
    pub fn fake_records_without_fields() -> Result<Vec<(IndexValue, Record)>> {
        let header = RecordHeader::new();
        let mut records = Vec::new();

        // add first record
        let index_value = IndexValue{
            input_start_pos: 50,
            input_end_pos: 100,
            match_flag: MatchFlag::Yes,
            spent_time: 150
        };
        let record = header.new_record()?;
        records.push((index_value, record));

        // add second record
        let index_value = IndexValue{
            input_start_pos: 200,
            input_end_pos: 250,
            match_flag: MatchFlag::None,
            spent_time: 300
        };
        let record = header.new_record()?;
        records.push((index_value, record));

        // add third record
        let index_value = IndexValue{
            input_start_pos: 350,
            input_end_pos: 400,
            match_flag: MatchFlag::Skip,
            spent_time: 450
        };
        let record = header.new_record()?;
        records.push((index_value, record));

        Ok(records)
    }

    /// Return a fake index file with fields as byte slice.
    pub fn fake_index_with_fields() -> Result<[u8; FAKE_INDEX_BYTES]> {
        // init buffer
        let mut buf = [0u8; FAKE_INDEX_BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 3245634545244324234u64);
        copy_bytes(&mut buf, &index_header_buf, 0)?;
        copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, IndexHeader::BYTES)?;
        copy_bytes(&mut buf, &FAKE_RECORDS_BYTE_SLICE, IndexHeader::BYTES + ADD_FIELDS_HEADER_BYTES)?;
        Ok(buf)
    }

    /// Return a fake index file without fields as byte slice.
    pub fn fake_index_without_fields() -> Result<[u8; FAKE_INDEX_WITHOUT_FIELDS_BYTES]> {
        // init buffer
        let mut buf = [0u8; FAKE_INDEX_WITHOUT_FIELDS_BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 3245634545244324234u64);
        copy_bytes(&mut buf, &index_header_buf, 0)?;
        copy_bytes(&mut buf, &EMPTY_RECORD_HEADER_BYTE_SLICE, IndexHeader::BYTES)?;
        copy_bytes(&mut buf, &FAKE_RECORDS_WITHOUT_FIELDS_BYTE_SLICE, IndexHeader::BYTES + EMPTY_RECORD_HEADER_BYTES)?;
        Ok(buf)
    }

    /// Returns the fake input content as bytes.
    pub fn fake_input_bytes() -> Vec<u8> {
        "\
            name,size,price,color\n\
            fork,\"1 inch\",12.34,red\n\
            keyboard,medium,23.45,\"black\nwhite\"\n\
            mouse,\"12 cm\",98.76,white\n\
            \"rust book\",500 pages,1,\"orange\"\
        ".as_bytes().to_vec()
    }

    /// Returns the fake input hash value.
    pub fn fake_input_hash() -> [u8; HASH_SIZE] {
        [ 47, 130, 231, 73, 14, 84, 144, 114, 198, 155, 94, 35, 15,
          101, 71, 156, 48, 113, 13, 217, 129, 108, 130, 240, 24, 19,
          159, 141, 205, 59, 71, 227]
    }

    /// Create a fake input file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Input file path.
    pub fn create_fake_input(path: &str) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&fake_input_bytes())?;
        writer.flush()?;

        Ok(())
    }

//     /// Returns the empty extra fields value.
//     pub fn build_empty_extra_fields() -> [u8; 226] {
//         let mut buf = [0u8; 226];
//         buf[0] = 44;
//         buf[1] = 32;
//         buf[2] = 44;
//         buf[23] = 44;
//         buf[24] = 34;
//         buf[225] = 34;
//         for i in 0..20 {
//             buf[3+i] = 48;
//         }
//         for i in 0..200 {
//             buf[25+i] = 32;
//         }
//         buf
//     }

//     /// Return the fake output content as bytes.
//     pub fn fake_output_bytes() -> Vec<u8> {
//         let buf = build_empty_extra_fields().to_vec();
//         let eef = String::from_utf8(buf).unwrap();
//         format!("\
//             name,size,price,color,match,time,comments\n\
//             fork,\"1 inch\",12.34,red{}\n\
//             keyboard,medium,23.45,\"black\nwhite\"{}\n\
//             mouse,\"12 cm\",98.76,white{}\n\
//             \"rust book\",500 pages,1,\"orange\"{}\
//         ", eef, eef, eef, eef).as_bytes().to_vec()
//     }

//     /// Create a fake output file based on the default fake input file.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `path` - Output file path.
//     pub fn create_fake_output(path: &str) -> Result<()> {
//         let file = OpenOptions::new()
//             .create(true)
//             .truncate(true)
//             .write(true)
//             .open(path)?;
//         let mut writer = BufWriter::new(file);
//         writer.write_all(&fake_output_bytes())?;
//         writer.flush()?;

//         Ok(())
//     }

//     /// Return the fake index content as bytes.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `empty` - If `true` then build all records with MatchFlag::None.
//     pub fn fake_index_bytes(empty: bool) -> Vec<u8> {
//         let mut buf: Vec<u8> = vec!();

//         // write header
//         append_bytes(&mut buf, &build_INDEX_HEADER_BYTES(true, &fake_input_hash(), true, 4));

//         // write values
//         append_bytes(&mut buf, &build_value_bytes(22, 45, 65, if empty { 0 } else { b'Y' }));
//         append_bytes(&mut buf, &build_value_bytes(46, 81, 327, 0));
//         append_bytes(&mut buf, &build_value_bytes(82, 107, 579, if empty { 0 } else { b'N' }));
//         append_bytes(&mut buf, &build_value_bytes(108, 140, 838, 0));

//         buf
//     }

//     /// Create a fake index file based on the default fake input file.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `path` - Index file path.
//     /// * `empty` - If `true` then build all records with MatchFlag::None.
//     pub fn create_fake_index(path: &str, empty: bool) -> Result<()> {
//         let file = OpenOptions::new()
//             .create(true)
//             .truncate(true)
//             .write(true)
//             .open(path)?;
//         let mut writer = BufWriter::new(file);
//         writer.write_all(fake_index_bytes(empty).as_slice())?;
//         writer.flush()?;

//         Ok(())
//     }

//     /// Execute a function with both a temp directory and a new Indexer.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `f` - Function to execute.
//     pub fn with_tmpdir_and_indexer(f: &impl Fn(&TempDir, &mut Indexer) -> Result<()>) {
//         let sub = |dir: &TempDir| -> Result<()> {
//             // generate default file names for files
//             let input_path = dir.path().join("i.csv");
//             let output_path = dir.path().join("o.csv");
//             let index_path = dir.path().join("i.index");

//             // create Indexer and execute
//             let input_path_str = input_path.to_str().unwrap().to_string();
//             let mut indexer = Indexer::new(
//                 &input_path_str,
//                 output_path.to_str().unwrap(),
//                 index_path.to_str().unwrap()
//             );

//             // execute function
//             match f(&dir, &mut indexer) {
//                 Ok(_) => Ok(()),
//                 Err(e) => bail!(e)
//             }
//         };
//         with_tmpdir(&sub)
//     }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helper::*;
    use std::io::Cursor;
    use crate::test_helper::*;
//     use crate::index::field::FieldTypeHeader;
    use crate::db::indexer::header::test_helper::{random_hash, build_header_bytes};
//     use crate::index::index_header::test_helper::{random_hash, build_INDEX_HEADER_BYTES};
//     use crate::index::index_value::test_helper::{build_value_bytes};
//     use crate::index::index_header::{INDEX_HEADER_BYTES, HASH_SIZE};
//     use crate::index::index_value::INDEX_VALUE_BYTES;
//     use crate::index::POSITION_SIZE;
//     use tempfile::TempDir;

    #[test]
    fn new() {
        let expected = Indexer{
            input_path: "my_input.csv".to_string(),
            index_path: "my_index.fmidx".to_string(),
            index_header: IndexHeader::new(),
            record_header: RecordHeader::new(),
            indexing_batch_size: DEFAULT_INDEXING_BATCH_SIZE
        };
        let indexer = Indexer::new("my_input.csv", "my_index.fmidx");
        assert_eq!(expected, indexer);
    }

    #[test]
    fn calc_record_pos_without_fields() {
        let indexer = Indexer::new("my_input.csv", "my_index.fmidx");
        assert_eq!(111, indexer.calc_record_pos(2));
    }

    #[test]
    fn calc_record_pos_with_fields() {
        let mut indexer = Indexer::new("my_input.csv", "my_index.fmidx");

        // add fields
        if let Err(e) = add_fields(&mut indexer.record_header) {
            assert!(false, "expected to add fields, but got error: {:?}", e);
        }
        assert_eq!(255, indexer.calc_record_pos(2));
    }

    #[test]
    fn load_headers_from_with_fields() {
        // create buffer
        let mut buf = [0u8; IndexHeader::BYTES + ADD_FIELDS_HEADER_BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 3245634545244324234u64);
        if let Err(e) = copy_bytes(&mut buf, &index_header_buf, 0) {
            assert!(false, "{:?}", e);
        }
        if let Err(e) = copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, IndexHeader::BYTES) {
            assert!(false, "{:?}", e);
        }
        let mut reader = Cursor::new(buf.to_vec());

        // test load_headers
        let mut indexer = Indexer::new("my_input.csv", "my_index.fmidx");
        if let Err(e) = indexer.load_headers_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
        }

        // check expected index header
        let mut expected = IndexHeader::new();
        expected.indexed = true;
        expected.hash = Some(hash_buf);
        expected.indexed_count = 3245634545244324234u64;
        assert_eq!(expected, indexer.index_header);

        // check expected record header
        let mut expected = RecordHeader::new();
        if let Err(e) = add_fields(&mut expected) {
            assert!(false, "expected to add fields, but got error: {:?}", e);
        }
        assert_eq!(expected, indexer.record_header);
    }

    #[test]
    fn load_headers_from_without_fields() {
        // create buffer
        let mut buf = [0u8; IndexHeader::BYTES + EMPTY_RECORD_HEADER_BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 5245634545244324234u64);
        if let Err(e) = copy_bytes(&mut buf, &index_header_buf, 0) {
            assert!(false, "{:?}", e);
        }
        if let Err(e) = copy_bytes(&mut buf, &EMPTY_RECORD_HEADER_BYTE_SLICE, IndexHeader::BYTES) {
            assert!(false, "{:?}", e);
        }
        let mut reader = Cursor::new(buf.to_vec());

        // test load_headers
        let mut indexer = Indexer::new("my_input.csv", "my_index.fmidx");
        if let Err(e) = indexer.load_headers_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
        }

        // check expected index header
        let mut expected = IndexHeader::new();
        expected.indexed = true;
        expected.hash = Some(hash_buf);
        expected.indexed_count = 5245634545244324234u64;
        assert_eq!(expected, indexer.index_header);

        // check expected record header
        let expected = RecordHeader::new();
        assert_eq!(expected, indexer.record_header);
    }

    #[test]
    fn read_record_from_with_fields() {
        // init buffer
        let buf = match fake_index_with_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init indexer and expected records
        let mut indexer = Indexer::new("my_input.csv", "my_index.fmidx");
        if let Err(e) = add_fields(&mut indexer.record_header) {
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
        let (index_value, record) = match indexer.read_record_from(&mut reader, 0) {
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
        assert_eq!(expected[0].0, index_value);
        assert_eq!(expected[0].1, record);

        // test second record
        let (index_value, record) = match indexer.read_record_from(&mut reader, 1) {
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
        assert_eq!(expected[1].0, index_value);
        assert_eq!(expected[1].1, record);

        // test third record
        let (index_value, record) = match indexer.read_record_from(&mut reader, 2) {
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
        assert_eq!(expected[2].0, index_value);
        assert_eq!(expected[2].1, record);
    }

    #[test]
    fn read_record_from_without_fields() {
        // init buffer
        let buf = match fake_index_without_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init indexer and expected records
        let indexer = Indexer::new("my_input.csv", "my_index.fmidx");
        let expected = match fake_records_without_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test first record
        let (index_value, record) = match indexer.read_record_from(&mut reader, 0) {
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
        assert_eq!(expected[0].0, index_value);
        assert_eq!(expected[0].1, record);

        // test second record
        let (index_value, record) = match indexer.read_record_from(&mut reader, 1) {
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
        assert_eq!(expected[1].0, index_value);
        assert_eq!(expected[1].1, record);

        // test third record
        let (index_value, record) = match indexer.read_record_from(&mut reader, 2) {
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
        assert_eq!(expected[2].0, index_value);
        assert_eq!(expected[2].1, record);
    }

//     #[test]
//     fn init_index_non_trucate() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             let buf: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
//             create_file_with_bytes(&indexer.index_path, &buf)?;

//             // create expected file contents
//             let mut expected = [0u8; INDEX_HEADER_BYTES];
//             for i in 0..buf.len() {
//                 expected[i] = buf[i];
//             }
//             for i in buf.len()..INDEX_HEADER_BYTES {
//                 expected[i] = 0;
//             }

//             indexer.init_index(false)?;

//             let index = File::open(&indexer.index_path)?;
//             let mut reader = BufReader::new(index);
//             let mut buf_after: Vec<u8> = vec!();
//             reader.read_to_end(&mut buf_after)?;
//             assert_eq!(expected, buf_after.as_slice());

//             Ok(())
//         });
//     }

//     #[test]
//     fn count_indexed() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             create_fake_index(&indexer.index_path, false)?;
//             assert_eq!(4u64, indexer.count_indexed()?);
//             Ok(())
//         });
//     }

//     #[test]
//     fn get_record_index() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             create_fake_input(&indexer.input_path)?;
//             create_fake_index(&indexer.index_path, false)?;

//             // first line
//             let expected = IndexValue{
//                 input_start_pos: 22,
//                 input_end_pos: 45,
//                 output_pos: 65,
//                 match_flag: MatchFlag::Yes
//             };
//             match indexer.get_record_index(0)? {
//                 Some(v) => assert_eq!(expected, v),
//                 None => assert!(false, "should have return an IndexValue")
//             }

//             // second line
//             let expected = IndexValue{
//                 input_start_pos: 46,
//                 input_end_pos: 81,
//                 output_pos: 327,
//                 match_flag: MatchFlag::None
//             };
//             match indexer.get_record_index(1)? {
//                 Some(v) => assert_eq!(expected, v),
//                 None => assert!(false, "should have return an IndexValue")
//             }

//             // third line
//             let expected = IndexValue{
//                 input_start_pos: 82,
//                 input_end_pos: 107,
//                 output_pos: 579,
//                 match_flag: MatchFlag::No
//             };
//             match indexer.get_record_index(2)? {
//                 Some(v) => assert_eq!(expected, v),
//                 None => assert!(false, "should have return an IndexValue")
//             }

//             Ok(())
//         });
//     }

//     #[test]
//     fn healthcheck_new_index() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             create_file_with_bytes(&indexer.index_path, &[0u8; INDEX_HEADER_BYTES])?;
//             assert_eq!(IndexStatus::New, indexer.healthcheck()?);
//             Ok(())
//         });
//     }

//     #[test]
//     fn healthcheck_hash_mismatch() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             let mut buf = [0u8; INDEX_HEADER_BYTES];

//             // set valid_hash flag as true
//             buf[9] = 1u8;

//             // force hash bytes to be invalid
//             buf[10] = 3u8;

//             create_file_with_bytes(&indexer.index_path, &buf)?;
//             create_fake_input(&indexer.input_path)?;
//             assert_eq!(IndexStatus::Corrupted, indexer.healthcheck()?);
//             Ok(())
//         });
//     }
    
//     #[test]
//     fn healthcheck_incomplete_corrupted() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             let mut buf = [0u8; INDEX_HEADER_BYTES+5];

//             // set indexed flag as false
//             buf[0] = 0u8;

//             // set valid_hash flag as true
//             buf[9] = 1u8;

//             // set fake input file hash value
//             let buf_hash = &mut buf[10..10+HASH_SIZE];
//             buf_hash.copy_from_slice(fake_input_hash().as_slice());

//             create_file_with_bytes(&indexer.index_path, &buf)?;
//             create_fake_input(&indexer.input_path)?;
//             assert_eq!(IndexStatus::Corrupted, indexer.healthcheck()?);
//             Ok(())
//         });
//     }
    
//     #[test]
//     fn healthcheck_incomplete_valid() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             let mut buf = [0u8; INDEX_HEADER_BYTES+INDEX_VALUE_BYTES];

//             // set indexed flag as false
//             buf[0] = 0u8;

//             // set valid_hash flag as true
//             buf[9] = 1u8;

//             // set fake input file hash value
//             let buf_hash = &mut buf[10..10+HASH_SIZE];
//             buf_hash.copy_from_slice(fake_input_hash().as_slice());

//             // set fake index value
//             let buf_value = &mut buf[10+HASH_SIZE..10+HASH_SIZE+INDEX_VALUE_BYTES];
//             buf_value.copy_from_slice(&build_value_bytes(10, 20, 21, b'Y'));

//             create_file_with_bytes(&indexer.index_path, &buf)?;
//             create_fake_input(&indexer.input_path)?;
//             assert_eq!(IndexStatus::Incomplete, indexer.healthcheck()?);
//             Ok(())
//         });
//     }
    
//     #[test]
//     fn healthcheck_indexed_corrupted() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             let mut buf = [0u8; INDEX_HEADER_BYTES];

//             // set indexed flag as true
//             buf[0] = 1u8;

//             // force indexed_count to be invalid
//             let buf_indexed_count = &mut buf[1..1+POSITION_SIZE];
//             buf_indexed_count.copy_from_slice(&10000u64.to_be_bytes());

//             // set valid_hash flag as true
//             buf[9] = 1u8;

//             // set fake input file hash value
//             let buf_hash = &mut buf[10..10+HASH_SIZE];
//             buf_hash.copy_from_slice(fake_input_hash().as_slice());

//             create_file_with_bytes(&indexer.index_path, &buf)?;
//             create_fake_input(&indexer.input_path)?;
//             assert_eq!(IndexStatus::Corrupted, indexer.healthcheck()?);
//             Ok(())
//         });
//     }
    
//     #[test]
//     fn healthcheck_indexed_valid() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             create_fake_index(&indexer.index_path, false)?;
//             create_fake_input(&indexer.input_path)?;
//             assert_eq!(IndexStatus::Indexed, indexer.healthcheck()?);
//             Ok(())
//         });
//     }

//     #[test]
//     fn last_indexed_record() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             create_fake_index(&indexer.index_path, false)?;
//             let expected = Some(IndexValue{
//                 input_start_pos: 108,
//                 input_end_pos: 140,
//                 output_pos: 838,
//                 match_flag: MatchFlag::None
//             });

//             indexer.header.indexed_count = 4;
//             assert_eq!(expected, indexer.last_indexed_record()?);
            
//             Ok(())
//         });
//     }

//     #[test]
//     fn last_indexed_record_with_zero_indexed() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             create_fake_index(&indexer.index_path, false)?;
//             indexer.header.indexed_count = 0;
//             assert_eq!(None, indexer.last_indexed_record()?);
            
//             Ok(())
//         });
//     }

//     #[test]
//     fn index_records() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             create_fake_input(&indexer.input_path)?;

//             // index records
//             indexer.index()?;

//             // validate index bytes
//             let expected = fake_index_bytes(true);
//             let file = File::open(&indexer.index_path)?;
//             let mut reader = BufReader::new(file);
//             let mut buf: Vec<u8> = vec!();
//             reader.read_to_end(&mut buf)?;
//             assert_eq!(expected, buf);
            
//             // validate output bytes
//             let expected = fake_output_bytes();
//             let file = File::open(&indexer.output_path)?;
//             let mut reader = BufReader::new(file);
//             let mut buf: Vec<u8> = vec!();
//             reader.read_to_end(&mut buf)?;
//             assert_eq!(expected, buf);
            
//             Ok(())
//         });
//     }
}