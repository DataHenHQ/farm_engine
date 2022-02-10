pub mod header;
pub mod value;
pub mod exporter;

use anyhow::{bail, Result};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
use crate::error::ParseError;
use crate::{file_size, generate_hash};
use crate::traits::{ByteSized, LoadFrom, ReadFrom, WriteTo};
use super::record::{Header as RecordHeader, Record};
use header::{Header as IndexHeader, InputType};
use value::{MatchFlag, Value as IndexValue};
use exporter::{ExportFileType, ExporterWriter, ExportField, ExporterJSONWriter, ExporterCSVWriter};

/// Indexer version.
pub const VERSION: u32 = 2;

/// Index file extension.
pub const INDEX_FILE_EXTENSION: &str = "fmindex";

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

/// Represents an indexer record data.
#[derive(Debug, PartialEq)]
pub struct Data {
    pub index: IndexValue,
    pub record: Record
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
    pub fn new(input_path: PathBuf, index_path: PathBuf) -> Self {
        Self{
            input_path: input_path,
            index_path: index_path,
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
    pub fn record_from(&self, reader: &mut impl Read) -> Result<Data> {
        // read index value data
        let index_value = IndexValue::read_from(reader)?;
        
        // read record value data
        let record = self.record_header.read_record(reader)?;

        Ok(Data{
            index: index_value,
            record
        })
    }

    /// Move to index position and then read both the index value and record from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    pub fn seek_record_from(&self, reader: &mut (impl Read + Seek), index: u64, force: bool) -> Result<Option<Data>> {
        if !force && !self.index_header.indexed {
            bail!("input file must be indexed before reading records")
        }
        if self.index_header.indexed_count > index {
            let pos = self.calc_record_pos(index);
            reader.seek(SeekFrom::Start(pos))?;
            return Ok(Some(self.record_from(reader)?));
        }
        Ok(None)
    }

    /// Read both the index value and record from the index file.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    pub fn record(&self, index: u64) -> Result<Option<Data>> {
        let mut reader = self.new_index_reader()?;
        self.seek_record_from(&mut reader, index, false)
    }

    /// Updates a record date into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - File writer to save data into.
    /// * `index` - Index value index.
    /// * `value` - Index value to save.
    pub fn write_record_into(&self, writer: &mut (impl Write + Seek), data: &Data) -> Result<()> {
        data.index.write_to(writer)?;
        self.record_header.write_record(writer, &data.record)?;
        Ok(())
    }

    /// Updates or append both an index value and record into the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Index value index.
    /// * `index_value` - Index value to save.
    /// * `record` - Record to save
    pub fn save_record(&self, index: u64, data: &Data) -> Result<()> {
        let pos = self.calc_record_pos(index);
        let mut writer = self.new_index_writer(false)?;
        writer.seek(SeekFrom::Start(pos))?;
        self.write_record_into(&mut writer, data)?;
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
        let mut reader = self.new_input_reader()?;
        let hash = generate_hash(&mut reader)?;

        // check whenever index file exists
        match self.new_index_reader() {
            // try to load the index headers
            Ok(mut reader) => if let Err(e) = self.load_headers_from(&mut reader) {
                match e.downcast::<std::io::Error>() {
                    Ok(ex) => match ex.kind() {
                        std::io::ErrorKind::NotFound => {
                            // File not found so the index is new
                            return Ok(IndexStatus::New);
                        }
                        std::io::ErrorKind::UnexpectedEof => {
                            // if the file is empty then is new
                            let real_size = file_size(&self.index_path)?;
                            if real_size < 1 {
                                // store hash and return as new index
                                self.index_header.hash = Some(hash);
                                return Ok(IndexStatus::New);
                            }

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
        

        // validate corrupted index
        let real_size = file_size(&self.index_path)?;
        let expected_size = self.calc_record_pos(self.index_header.indexed_count);
        if self.index_header.indexed {
            if real_size != expected_size {
                // sizes don't match, the file is corrupted
                return Ok(IndexStatus::Corrupted);
            }
        } else {
            if real_size < expected_size {
                // sizes is smaller, the file is corrupted
                return Ok(IndexStatus::Corrupted);
            }
            // index is incomplete
            return Ok(IndexStatus::Incomplete);
        }

        // all good, the index is indexed
        Ok(IndexStatus::Indexed)
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
    fn index_csv_record(&self, iter: &csv::StringRecordsIter<impl Read>, item: csv::StringRecord, input_reader: &mut (impl Read + Seek)) -> Result<IndexValue> {
        // calculate input positions
        let mut start_pos = item.position().unwrap().byte();
        let mut end_pos = iter.reader().position().byte() - 1;
        let length: usize = (end_pos - start_pos + 1) as usize;

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
    /// from a CSV input file.
    /// 
    /// # Arguments
    /// 
    /// * `input_rdr` - Input byte reader.
    /// * `index_wrt` - Index byte writer.
    /// * `is_first` - `true` when the input reader is set at position 0.
    fn index_csv(&mut self, input_rdr: impl Read, index_wrt: &mut (impl Seek + Write), is_first: bool) -> Result<()> {
        // index records
        let mut record_data = Data{
            index: IndexValue::new(),
            record: self.record_header.new_record()?
        };
        let mut is_first = is_first;
        let mut input_rdr_nav = self.new_input_reader()?;
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
                Ok(v) => self.index_csv_record(&iter, v, &mut input_rdr_nav)?,
                Err(e) => bail!(ParseError::from(e))
            };

            // write index value for this record
            record_data.index = value;
            self.write_record_into(index_wrt, &record_data)?;
            self.index_header.indexed_count += 1;

            // save headers every batch
            if self.index_header.indexed_count % self.indexing_batch_size < 1 {
                self.save_index_header(index_wrt)?;
            }
        }

        // write headers
        self.index_header.indexed = true;
        self.save_index_header(index_wrt)?;

        Ok(())
    }

    /// Index a new or incomplete index by tracking each item position
    /// from the input file.
    pub fn index(&mut self) -> Result<()> {
        // create reader and writer buffers
        let mut input_rdr = self.new_input_reader()?;
        let mut index_wrt = self.new_index_writer(true)?;
        let mut is_first = true;

        // perform index healthcheck
        match self.healthcheck() {
            Ok(v) => match v {
                IndexStatus::Indexed => return Ok(()),
                IndexStatus::Incomplete => {
                    // read last indexed record or create the index file
                    let mut reader = self.new_index_reader()?;
                    match self.seek_record_from(&mut reader, self.index_header.indexed_count, true)? {
                        Some(data) => {
                            // load last known indexed value position
                            is_first = false;
                            input_rdr.seek(SeekFrom::Start(data.index.input_end_pos + 1))?;
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

        // index input file
        match self.index_header.input_type {
            InputType::CSV => self.index_csv(&mut input_rdr, &mut index_wrt, is_first),
            InputType::JSON => unimplemented!(),
            InputType::Unknown => bail!("not supported input file type")
        }
    }

    /// Export the input plus records data into a csv writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `fields` - List of fields to export.
    fn export_from_csv(&self, writer: &mut impl ExporterWriter, index_rdr: &mut impl Read, fields: &[ExportField]) -> Result<()> {
        // write headers
        let mut headers = Vec::new();
        for field in fields {
            let field_name = match field {
                ExportField::SpentTime => "spent_time".to_string(),
                ExportField::MatchFlag => "matched".to_string(),
                ExportField::Input(s) => s.to_string(),
                ExportField::Record(s) => s.to_string()
            };
            headers.push(field_name);
        }
        writer.write_headers(&headers)?;

        // create input CSV reader
        let input_rdr = self.new_input_reader()?;
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(input_rdr);
        
        // iterate input as CSV
        for result in csv_reader.deserialize() {
            // read input and indexer data
            let input_data = result?;
            let indexer_data = self.record_from(index_rdr)?;

            // write data
            writer.write_data(fields, input_data, indexer_data)?;
        };
        Ok(())
    }

    /// Export the input plus records data into a json writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `fields` - List of fields to export.
    fn export_from_json(&self, writer: &mut impl ExporterWriter, index_rdr: &mut impl Read, fields: &[ExportField]) -> Result<()> {
        // // write array start
        // write!(writer, "[")?;

        unimplemented!()

        // // write array end
        // write!(writer, "]");
        // Ok(())
    }

    /// Export the input plus records data into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `fields` - List of fields to export.
    pub fn export(&self, writer: &mut impl Write, file_type: ExportFileType, fields: &[ExportField]) -> Result<()> {
        // validate before export
        if !self.index_header.indexed {
            bail!("input file must be indexed to be exported");
        }

        // create the index reader and move to first record
        let mut index_rdr = self.new_index_reader()?;
        let pos = self.calc_record_pos(0);
        index_rdr.seek(SeekFrom::Start(pos))?;

        // export data
        match file_type {
            ExportFileType::CSV => {
                let mut exporter_writer = ExporterCSVWriter{
                    writer: csv::Writer::from_writer(writer)
                };
                match self.index_header.input_type {
                    InputType::CSV => self.export_from_csv(
                        &mut exporter_writer,
                        &mut index_rdr, fields
                    ),
                    InputType::JSON => self.export_from_json(
                        &mut exporter_writer,
                        &mut index_rdr, fields
                    ),
                    InputType::Unknown => bail!("unsupported input file type")
                }
            },
            ExportFileType::JSON => {
                let mut exporter_writer = ExporterJSONWriter{
                    writer
                };
                match self.index_header.input_type {
                    InputType::CSV => self.export_from_csv(
                        &mut exporter_writer,
                        &mut index_rdr, fields
                    ),
                    InputType::JSON => self.export_from_json(
                        &mut exporter_writer,
                        &mut index_rdr, fields
                    ),
                    InputType::Unknown => bail!("unsupported input file type")
                }
            }
        }
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;
    use crate::test_helper::*;
    use crate::db::indexer::header::{HASH_SIZE};
    use crate::db::record::header::{FieldType, Field};
    use crate::db::record::value::{Value};
    use crate::db::indexer::header::test_helper::{random_hash, build_header_bytes};
//     use crate::index::index_header::test_helper::build_INDEX_HEADER_BYTES;
//     use crate::index::index_value::test_helper::build_value_bytes;
    use tempfile::TempDir;
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
    pub fn fake_records() -> Result<Vec<Data>> {
        let mut header = RecordHeader::new();
        add_fields(&mut header)?;
        let mut records = Vec::new();

        // add first record
        let mut record = header.new_record()?;
        record.set_by_index(0, Value::I32(234234234i32));
        record.set_by_index(1, Value::Str("abc".to_string()));
        let data = Data{
            index: IndexValue{
                input_start_pos: 50,
                input_end_pos: 100,
                match_flag: MatchFlag::Yes,
                spent_time: 150
            },
            record
        };
        records.push(data);

        // add second record
        let mut record = header.new_record()?;
        record.set_by_index(0, Value::I32(345345345i32));
        record.set_by_index(1, Value::Str("dfeg".to_string()));
        let data = Data{
            index: IndexValue{
                input_start_pos: 200,
                input_end_pos: 250,
                match_flag: MatchFlag::None,
                spent_time: 300
            },
            record
        };
        records.push(data);

        // add third record
        let mut record = header.new_record()?;
        record.set_by_index(0, Value::I32(857548574i32));
        record.set_by_index(1, Value::Str("hi123".to_string()));
        let data = Data{
            index: IndexValue{
                input_start_pos: 350,
                input_end_pos: 400,
                match_flag: MatchFlag::Skip,
                spent_time: 450
            },
            record
        };
        records.push(data);

        Ok(records)
    }

    /// Create fake records without fields.
    /// 
    /// # Arguments
    /// 
    /// * `records` - Record vector to add records into.
    pub fn fake_records_without_fields() -> Result<Vec<Data>> {
        let header = RecordHeader::new();
        let mut records = Vec::new();

        // add first record
        let data = Data{
            index: IndexValue{
                input_start_pos: 50,
                input_end_pos: 100,
                match_flag: MatchFlag::Yes,
                spent_time: 150
            },
            record: header.new_record()?
        };
        records.push(data);

        // add second record
        let data = Data{
            index: IndexValue{
                input_start_pos: 200,
                input_end_pos: 250,
                match_flag: MatchFlag::None,
                spent_time: 300
            },
            record: header.new_record()?
        };
        records.push(data);

        // add third record
        let data = Data{
            index: IndexValue{
                input_start_pos: 350,
                input_end_pos: 400,
                match_flag: MatchFlag::Skip,
                spent_time: 450
            },
            record: header.new_record()?
        };
        records.push(data);

        Ok(records)
    }

    /// Return a fake index file with fields as byte slice and the record count.
    pub fn fake_index_with_fields() -> Result<([u8; FAKE_INDEX_BYTES], u64)> {
        // init buffer
        let mut buf = [0u8; FAKE_INDEX_BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 3245634545244324234u64, InputType::CSV);
        copy_bytes(&mut buf, &index_header_buf, 0)?;
        copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, IndexHeader::BYTES)?;
        copy_bytes(&mut buf, &FAKE_RECORDS_BYTE_SLICE, IndexHeader::BYTES + ADD_FIELDS_HEADER_BYTES)?;
        Ok((buf, 3))
    }

    /// Return a fake index file without fields as byte slice.
    pub fn fake_index_without_fields() -> Result<([u8; FAKE_INDEX_WITHOUT_FIELDS_BYTES], u64)> {
        // init buffer
        let mut buf = [0u8; FAKE_INDEX_WITHOUT_FIELDS_BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 3245634545244324234u64, InputType::CSV);
        copy_bytes(&mut buf, &index_header_buf, 0)?;
        copy_bytes(&mut buf, &EMPTY_RECORD_HEADER_BYTE_SLICE, IndexHeader::BYTES)?;
        copy_bytes(&mut buf, &FAKE_RECORDS_WITHOUT_FIELDS_BYTE_SLICE, IndexHeader::BYTES + EMPTY_RECORD_HEADER_BYTES)?;
        Ok((buf, 3))
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
    pub fn create_fake_input(path: &PathBuf) -> Result<()> {
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

    /// Write a fake index bytes into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `with_fields` - If `true` then add record fields.
    /// * `unprocessed` - If `true` then build all records with MatchFlag::None.
    pub fn write_fake_index(writer: &mut (impl Seek + Write), with_fields: bool, unprocessed: bool) -> Result<Vec<Data>> {
        let mut records = Vec::new();

        // write index header
        let mut index_header = IndexHeader::new();
        index_header.indexed = true;
        index_header.indexed_count = 4;
        index_header.input_type = InputType::CSV;
        index_header.hash = Some(fake_input_hash());
        index_header.write_to(writer)?;

        // write record header
        let mut record_header = RecordHeader::new();
        if with_fields {
            add_fields(&mut record_header)?;
        }
        record_header.write_to(writer)?;
        
        // write first record date
        let mut index_value = IndexValue::new();
        index_value.input_start_pos = 22;
        index_value.input_end_pos = 44;
        if !unprocessed {
            index_value.match_flag = MatchFlag::Yes;
            index_value.spent_time = 23;
        }
        let mut record = record_header.new_record()?;
        if with_fields && !unprocessed {
            record.set("foo", Value::I32(111i32))?;
            record.set("bar", Value::Str("first".to_string()))?;
        }
        index_value.write_to(writer)?;
        record_header.write_record(writer, &record)?;
        records.push(Data{
            index: index_value,
            record
        });
        
        // write second record date
        let mut index_value = IndexValue::new();
        index_value.input_start_pos = 46;
        index_value.input_end_pos = 80;
        if !unprocessed {
            index_value.match_flag = MatchFlag::No;
            index_value.spent_time = 25;
        }
        let mut record = record_header.new_record()?;
        if with_fields && !unprocessed {
            record.set("foo", Value::I32(222i32))?;
            record.set("bar", Value::Str("2th".to_string()))?;
        }
        index_value.write_to(writer)?;
        record_header.write_record(writer, &record)?;
        records.push(Data{
            index: index_value,
            record
        });
        
        // write third record date
        let mut index_value = IndexValue::new();
        index_value.input_start_pos = 82;
        index_value.input_end_pos = 106;
        if !unprocessed {
            index_value.match_flag = MatchFlag::None;
            index_value.spent_time = 30;
        }
        let mut record = record_header.new_record()?;
        if with_fields && !unprocessed {
            record.set("foo", Value::I32(333i32))?;
            record.set("bar", Value::Str("3rd".to_string()))?;
        }
        index_value.write_to(writer)?;
        record_header.write_record(writer, &record)?;
        records.push(Data{
            index: index_value,
            record
        });

        // write fourth record date
        let mut index_value = IndexValue::new();
        index_value.input_start_pos = 108;
        index_value.input_end_pos = 139;
        if !unprocessed {
            index_value.match_flag = MatchFlag::Skip;
            index_value.spent_time = 41;
        }
        let mut record = record_header.new_record()?;
        if with_fields && !unprocessed {
            record.set("foo", Value::I32(444i32))?;
            record.set("bar", Value::Str("4th".to_string()))?;
        }
        index_value.write_to(writer)?;
        record_header.write_record(writer, &record)?;
        records.push(Data{
            index: index_value,
            record
        });

        Ok(records)
    }

    /// Create a fake index file based on the default fake input file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Index file path.
    /// * `empty` - If `true` then build all records with MatchFlag::None.
    pub fn create_fake_index(path: &PathBuf, with_fields: bool, unprocessed: bool) -> Result<Vec<Data>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        let records = write_fake_index(&mut writer, with_fields, unprocessed)?;
        writer.flush()?;

        Ok(records)
    }

    /// Execute a function with both a temp directory and a new Indexer.
    /// 
    /// # Arguments
    /// 
    /// * `f` - Function to execute.
    pub fn with_tmpdir_and_indexer(f: &impl Fn(&TempDir, &mut Indexer) -> Result<()>) {
        let sub = |dir: &TempDir| -> Result<()> {
            // generate default file names for files
            let input_path = dir.path().join("i.csv");
            let index_path = dir.path().join("i.fmindex");

            // create Indexer and execute
            let mut indexer = Indexer::new(
                input_path,
                index_path
            );

            // execute function
            match f(&dir, &mut indexer) {
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
    use crate::db::indexer::header::{HASH_SIZE};
    use crate::db::record::Value;
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
            input_path: "my_input.csv".into(),
            index_path: "my_index.fmidx".into(),
            index_header: IndexHeader::new(),
            record_header: RecordHeader::new(),
            indexing_batch_size: DEFAULT_INDEXING_BATCH_SIZE
        };
        let indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into());
        assert_eq!(expected, indexer);
    }

    #[test]
    fn calc_record_pos_without_fields() {
        let indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into());
        assert_eq!(112, indexer.calc_record_pos(2));
    }

    #[test]
    fn calc_record_pos_with_fields() {
        let mut indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into());

        // add fields
        if let Err(e) = add_fields(&mut indexer.record_header) {
            assert!(false, "expected to add fields, but got error: {:?}", e);
        }
        assert_eq!(256, indexer.calc_record_pos(2));
    }

    #[test]
    fn load_headers_from_with_fields() {
        // create buffer
        let mut buf = [0u8; IndexHeader::BYTES + ADD_FIELDS_HEADER_BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 3245634545244324234u64, InputType::CSV);
        if let Err(e) = copy_bytes(&mut buf, &index_header_buf, 0) {
            assert!(false, "{:?}", e);
        }
        if let Err(e) = copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, IndexHeader::BYTES) {
            assert!(false, "{:?}", e);
        }
        let mut reader = Cursor::new(buf.to_vec());

        // test load_headers
        let mut indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into());
        if let Err(e) = indexer.load_headers_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
        }

        // check expected index header
        let mut expected = IndexHeader::new();
        expected.indexed = true;
        expected.hash = Some(hash_buf);
        expected.indexed_count = 3245634545244324234u64;
        expected.input_type = InputType::CSV;
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
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 5245634545244324234u64, InputType::CSV);
        if let Err(e) = copy_bytes(&mut buf, &index_header_buf, 0) {
            assert!(false, "{:?}", e);
        }
        if let Err(e) = copy_bytes(&mut buf, &EMPTY_RECORD_HEADER_BYTE_SLICE, IndexHeader::BYTES) {
            assert!(false, "{:?}", e);
        }
        let mut reader = Cursor::new(buf.to_vec());

        // test load_headers
        let mut indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into());
        if let Err(e) = indexer.load_headers_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
        }

        // check expected index header
        let mut expected = IndexHeader::new();
        expected.indexed = true;
        expected.hash = Some(hash_buf);
        expected.indexed_count = 5245634545244324234u64;
        expected.input_type = InputType::CSV;
        assert_eq!(expected, indexer.index_header);

        // check expected record header
        let expected = RecordHeader::new();
        assert_eq!(expected, indexer.record_header);
    }

    #[test]
    fn record_from_with_fields() {
        // init buffer
        let (buf, record_count) = match fake_index_with_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init indexer and expected records
        let mut indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into());
        indexer.index_header.indexed = true;
        indexer.index_header.indexed_count = record_count;
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
        let pos = indexer.calc_record_pos(0);
        if let Err(e) = reader.seek(SeekFrom::Start(pos)) {
            assert!(false, "{}", e);
        };
        let data = match indexer.record_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[0], data);

        // test second record
        let pos = indexer.calc_record_pos(1);
        if let Err(e) = reader.seek(SeekFrom::Start(pos)) {
            assert!(false, "{}", e);
        };
        let data = match indexer.record_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[1], data);

        // test third record
        let pos = indexer.calc_record_pos(2);
        if let Err(e) = reader.seek(SeekFrom::Start(pos)) {
            assert!(false, "{}", e);
        };
        let data = match indexer.record_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[2], data);
    }

    #[test]
    fn record_from_without_fields() {
        // init buffer
        let (buf, record_count) = match fake_index_without_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init indexer and expected records
        let mut indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into());
        indexer.index_header.indexed = true;
        indexer.index_header.indexed_count = record_count;
        let expected = match fake_records_without_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test first record
        let pos = indexer.calc_record_pos(0);
        if let Err(e) = reader.seek(SeekFrom::Start(pos)) {
            assert!(false, "{}", e);
        };
        let data = match indexer.record_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[0], data);

        // test second record
        let pos = indexer.calc_record_pos(1);
        if let Err(e) = reader.seek(SeekFrom::Start(pos)) {
            assert!(false, "{}", e);
        };
        let data = match indexer.record_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[1], data);

        // test third record
        let pos = indexer.calc_record_pos(2);
        if let Err(e) = reader.seek(SeekFrom::Start(pos)) {
            assert!(false, "{}", e);
        };
        let data = match indexer.record_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[2], data);
    }

    #[test]
    fn seek_record_from_with_fields() {
        // init buffer
        let (buf, record_count) = match fake_index_with_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init indexer and expected records
        let mut indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into());
        indexer.index_header.indexed = true;
        indexer.index_header.indexed_count = record_count;
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
        let data = match indexer.seek_record_from(&mut reader, 0, false) {
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
        let data = match indexer.seek_record_from(&mut reader, 1, false) {
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
        let data = match indexer.seek_record_from(&mut reader, 2, false) {
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
    fn seek_record_from_without_fields() {
        // init buffer
        let (buf, record_count) = match fake_index_without_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init indexer and expected records
        let mut indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into());
        indexer.index_header.indexed = true;
        indexer.index_header.indexed_count = record_count;
        let expected = match fake_records_without_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test first record
        let data = match indexer.seek_record_from(&mut reader, 0, false) {
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
        let data = match indexer.seek_record_from(&mut reader, 1, false) {
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
        let data = match indexer.seek_record_from(&mut reader, 2, false) {
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
    fn record_with_fields() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // init buffer
            let (buf, record_count) = match fake_index_with_fields() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            create_file_with_bytes(&indexer.index_path, &buf)?;

            // init indexer and expected records
            indexer.index_header.indexed = true;
            indexer.index_header.indexed_count = record_count;
            if let Err(e) = add_fields(&mut indexer.record_header) {
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
            let data = match indexer.record(0) {
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
            let data = match indexer.record(1) {
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
            let data = match indexer.record(2) {
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
        with_tmpdir_and_indexer(&|_, indexer| {
            // init buffer
            let (buf, record_count) = match fake_index_without_fields() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            create_file_with_bytes(&indexer.index_path, &buf)?;

            // init indexer and expected records
            indexer.index_header.indexed = true;
            indexer.index_header.indexed_count = record_count;
            let expected = match fake_records_without_fields() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };

            // test first record
            let data = match indexer.record(0) {
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
            assert_eq!(expected[0], data);

            // test second record
            let data = match indexer.record(1) {
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
            let data = match indexer.record(2) {
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
    fn save_record_into_with_fields() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            let mut records = create_fake_index(&indexer.index_path, true, false)?;
            add_fields(&mut indexer.record_header)?;
            let pos = indexer.calc_record_pos(2);
            let mut buf = [0u8; ADD_FIELDS_RECORD_BYTES];
            let file = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(file);
            let mut old_bytes_before = vec!(0u8; pos as usize);
            let mut old_bytes_after = vec!(0u8; ADD_FIELDS_RECORD_BYTES);
            reader.read_exact(&mut old_bytes_before)?;
            reader.read_exact(&mut buf)?;
            reader.read_exact(&mut old_bytes_after)?;
            let expected = [
                // start_pos
                0, 0, 0, 0, 0, 0, 0, 82u8,
                // end_pos
                0, 0, 0, 0, 0, 0, 0, 106u8,
                // spent_time
                0, 0, 0, 0, 0, 0, 0, 30u8,
                // match flag
                0,
                // foo field
                0, 0, 1u8, 77u8,
                // bar field
                0, 0, 0, 3u8, 51u8, 114u8, 100u8, 0, 0
            ];
            assert_eq!(expected, buf);

            // save record and check value
            let expected = [
                // start_pos
                0, 0, 0, 0, 0, 0, 0, 12u8,
                // end_pos
                0, 0, 0, 0, 0, 0, 0, 25u8,
                // spent_time
                0, 0, 0, 0, 0, 0, 0, 43u8,
                // match flag
                b'Y',
                // foo field
                0, 0, 0, 11u8,
                // bar field
                0, 0, 0, 5u8, 104u8, 101u8, 108u8, 108u8, 111u8
            ];
            records[2].index.input_start_pos = 12;
            records[2].index.input_end_pos = 25;
            records[2].index.match_flag = MatchFlag::Yes;
            records[2].index.spent_time = 43;
            records[2].record.set("foo", Value::I32(11))?;
            records[2].record.set("bar", Value::Str("hello".to_string()))?;
            indexer.save_record(2, &records[2])?;
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
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            let mut records = create_fake_index(&indexer.index_path, false, true)?;
            let pos = indexer.calc_record_pos(2);
            let mut buf = [0u8; IndexValue::BYTES];
            let file = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(file);
            let mut old_bytes_before = vec!(0u8; pos as usize);
            let mut old_bytes_after = vec!(0u8; IndexValue::BYTES);
            reader.read_exact(&mut old_bytes_before)?;
            reader.read_exact(&mut buf)?;
            reader.read_exact(&mut old_bytes_after)?;
            let expected = [
                // start_pos
                0, 0, 0, 0, 0, 0, 0, 82u8,
                // end_pos
                0, 0, 0, 0, 0, 0, 0, 106u8,
                // spent_time
                0, 0, 0, 0, 0, 0, 0, 0,
                // match flag
                0
            ];
            assert_eq!(expected, buf);

            // save record and check value
            let expected = [
                // start_pos
                0, 0, 0, 0, 0, 0, 0, 10u8,
                // end_pos
                0, 0, 0, 0, 0, 0, 0, 27u8,
                // spent_time
                0, 0, 0, 0, 0, 0, 0, 93u8,
                // match flag
                b'Y'
            ];
            records[2].index.input_start_pos = 10;
            records[2].index.input_end_pos = 27;
            records[2].index.match_flag = MatchFlag::Yes;
            records[2].index.spent_time = 93;
            indexer.save_record(2, &records[2])?;
            reader.seek(SeekFrom::Start(0))?;
            let mut new_bytes_before = vec!(0u8; pos as usize);
            let mut new_bytes_after = vec!(0u8; IndexValue::BYTES);
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
    fn find_unmatched() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index
            let mut records = create_fake_index(&indexer.index_path, true, false)?;
            add_fields(&mut indexer.record_header)?;
            indexer.index_header.indexed = true;
            indexer.index_header.indexed_count = 4;

            // find existing unmatched from start position
            match indexer.find_unmatched(0) {
                Ok(opt) => match opt {
                    Some(v) => assert_eq!(2, v),
                    None => assert!(false, "expected 2 but got None")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            // find non-existing unmatched from starting point
            records[2].index.match_flag = MatchFlag::Yes;
            indexer.save_record(2, &records[2])?;
            match indexer.find_unmatched(3) {
                Ok(opt) => match opt {
                    Some(v) => assert!(false, "expected None but got {:?}", v),
                    None => assert!(true, "")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            Ok(())
        });
    }

    #[test]
    fn find_unmatched_with_offset() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            create_fake_index(&indexer.index_path, true, false)?;
            add_fields(&mut indexer.record_header)?;
            indexer.index_header.indexed = true;
            indexer.index_header.indexed_count = 4;

            // find existing unmatched with offset
            match indexer.find_unmatched(1) {
                Ok(opt) => match opt {
                    Some(v) => assert_eq!(2, v),
                    None => assert!(false, "expected 2 but got None")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            // find non-existing unmatched with offset
            match indexer.find_unmatched(3) {
                Ok(opt) => match opt {
                    Some(v) => assert!(false, "expected None but got {:?}", v),
                    None => assert!(true, "")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            Ok(())
        });
    }

    #[test]
    fn find_unmatched_with_non_indexed() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            create_fake_index(&indexer.index_path, true, false)?;
            add_fields(&mut indexer.record_header)?;
            indexer.index_header.indexed_count = 4;

            // find existing unmatched with offset
            match indexer.find_unmatched(1) {
                Ok(opt) => assert!(false, "expected error but got {:?}", opt),
                Err(e) => match e.downcast::<ParseError>(){
                    Ok(ex) => match ex {
                        ParseError::Unavailable(status) => match status {
                            IndexStatus::Incomplete => {},
                            s => assert!(false, "expected ParseError::Unavailable(Incomplete) but got: {:?}", s)
                        },
                        err => assert!(false, "{:?}", err)
                    },
                    Err(ex) => assert!(false, "{:?}", ex)
                }
            }

            Ok(())
        });
    }

    #[test]
    fn find_unmatched_with_offset_overflow() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            create_fake_index(&indexer.index_path, true, false)?;
            add_fields(&mut indexer.record_header)?;
            indexer.index_header.indexed = true;
            indexer.index_header.indexed_count = 2;

            // find existing unmatched with offset
            match indexer.find_unmatched(5) {
                Ok(opt) => match opt {
                    Some(v) => assert!(false, "expected None but got {:?}", v),
                    None => assert!(true, "")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_new_index() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            create_fake_input(&indexer.input_path)?;

            // test index status
            let expected = IndexStatus::New;
            match indexer.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test fake hash
            let expected = fake_input_hash();
            match indexer.index_header.hash {
                Some(hash) => assert_eq!(expected, hash),
                None => assert!(false, "expected a hash but got None")
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_new_index_with_empty_file() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            create_fake_input(&indexer.input_path)?;

            // test index status
            indexer.new_index_writer(true)?;
            let expected = IndexStatus::New;
            match indexer.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test fake hash
            let expected = fake_input_hash();
            match indexer.index_header.hash {
                Some(hash) => assert_eq!(expected, hash),
                None => assert!(false, "expected a hash but got None")
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_corrupted_headers() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let buf = [0u8; 5];
            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            let expected = IndexStatus::Corrupted;
            match indexer.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
            Ok(())
        });
    }

    #[test]
    fn healthcheck_hash_mismatch() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; IndexHeader::BYTES];
            let mut writer = &mut buf as &mut [u8];
            let mut header = IndexHeader::new();
            header.hash = Some([3u8; HASH_SIZE]);
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_incomplete_corrupted() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; IndexHeader::BYTES+EMPTY_RECORD_HEADER_BYTES+5];
            let mut writer = &mut buf as &mut [u8];
            let mut header = IndexHeader::new();
            header.indexed_count = 10;
            header.hash = Some(fake_input_hash());
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_incomplete_valid() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; IndexHeader::BYTES+EMPTY_RECORD_HEADER_BYTES+FAKE_RECORDS_WITHOUT_FIELDS_BYTES];
            let mut writer = &mut buf as &mut [u8];
            let mut header = IndexHeader::new();
            header.indexed_count = 3;
            header.hash = Some(fake_input_hash());
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Incomplete, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_indexed_corrupted() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; IndexHeader::BYTES+EMPTY_RECORD_HEADER_BYTES+5];
            let mut writer = &mut buf as &mut [u8];
            let mut header = IndexHeader::new();
            header.indexed = true;
            header.indexed_count = 8;
            header.hash = Some(fake_input_hash());
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_indexed_valid() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            create_fake_index(&indexer.index_path, true, false)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Indexed, indexer.healthcheck()?);
            Ok(())
        });
    }

    #[test]
    fn save_index_header() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            // create index file and read index header data
            create_fake_index(&indexer.index_path, true, false)?;
            let mut reader = indexer.new_index_reader()?;
            let mut expected = [0u8; IndexHeader::BYTES];
            reader.read_exact(&mut expected)?;
            reader.seek(SeekFrom::Start(0))?;
            indexer.index_header.load_from(&mut reader)?;

            // test save index header
            let mut buf = [0u8; IndexHeader::BYTES];
            let wrt = &mut buf as &mut [u8];
            let mut writer = Cursor::new(wrt);
            if let Err(e) = indexer.save_index_header(&mut writer) {
                assert!(false, "expected success but got error: {:?}", e);
            };
            assert_eq!(expected, buf);
            
            Ok(())
        });
    }

    #[test]
    fn index_records() {
        with_tmpdir_and_indexer(&|dir, indexer| -> Result<()> {
            create_fake_input(&indexer.input_path)?;
            indexer.index_header.input_type = InputType::CSV;

            // add record fields and index records
            add_fields(&mut indexer.record_header)?;
            if let Err(e) = indexer.index() {
                assert!(false, "expected success but got error: {:?}", e);
            }

            // create expected index
            let tmp_path = dir.path().join("test.fmindex");
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            write_fake_index(&mut writer, true, true)?;
            writer.flush()?;

            // read expected index bytes
            let file = File::open(&tmp_path)?;
            let mut reader = BufReader::new(file);
            let mut expected = Vec::new();
            reader.read_to_end(&mut expected)?;
            

            // validate index bytes
            let file = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf)?;
            assert_eq!(expected, buf);
            
            Ok(())
        });
    }
}