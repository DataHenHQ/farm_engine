pub mod header;
pub mod value;

use anyhow::{bail, Result};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::convert::TryFrom;
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use crate::error::ParseError;
//use crate::{file_size, fill_file, generate_hash};
use crate::traits::{ByteSized, LoadFrom, ReadFrom, WriteTo};
use super::record::{Header as RecordHeader, Record};
use header::Header as IndexHeader;
use value::{MatchFlag, Value as IndexValue};

/// Indexer version.
const VERSION: u32 = 2;

/// index healthcheck status.
#[derive(Debug, PartialEq)]
pub enum IndexStatus {
    New,
    Indexed,
    Incomplete,
    Corrupted,
    Indexing
}

impl Display for IndexStatus{
    fn fmt(&self, f: &mut Formatter) -> FmtResult { 
        write!(f, "{}", match self {
            Self::New => "new",
            Self::Indexed => "indexed",
            Self::Incomplete => "incomplete",
            Self::Corrupted => "corrupted",
            Self::Indexing => "indexing"
        })
    }
}

/// Indexer engine.
#[derive(Debug, PartialEq)]
pub struct Indexer {
    /// Input file path.
    pub input_path: String,

    /// Index file path.
    pub index_path: String,

    /// Index header data.
    pub index_header: IndexHeader,

    /// Record header data. It contains information about the custom fields.
    pub record_header: RecordHeader
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
            record_header: RecordHeader::new()
        }
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
    pub fn read_record(&self, reader: &mut (impl Read + Seek), index: u64) -> Result<Option<(IndexValue, Record)>> {
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

//     /// Updates an index value.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `writer` - File writer to save data into.
//     /// * `index` - Index value index.
//     /// * `value` - Index value to save.
//     pub fn update_index_file_value(writer: &mut (impl Write + Seek), index: u64, value: &IndexValue) -> Result<()> {
//         let pos = self.calc_record_pos(index);
//         writer.seek(SeekFrom::Start(pos))?;
//         let buf: Vec<u8> = value.into();
//         writer.write_all(buf.as_slice())?;
//         writer.flush()?;

//         Ok(())
//     }

//     /// Initialize index file.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `truncate` - If `true` then it truncates de file and initialize it.
//     pub fn init_index(&self, truncate: bool) -> std::io::Result<()> {
//         fill_file(&self.index_path, INDEX_HEADER_BYTES as u64, truncate)?;
//         Ok(())
//     }

//     /// Load index headers.
//     pub fn load_headers(&mut self) -> Result<()> {
//         let file = File::open(&self.index_path)?;
//         let mut reader = BufReader::new(file);

//         Self::header_from_file(&mut reader, &mut self.index_header)?;

//         Ok(())
//     }

//     /// Count how many records has been indexed so far.
//     pub fn count_indexed(&self) -> Result<u64, ParseError> {
//         let size = file_size(&self.index_path)?;

//         // get and validate file size
//         if INDEX_HEADER_BYTES as u64 > size {
//             return Err(ParseError::InvalidSize);
//         }

//         // calculate record count
//         let record_count = (size - INDEX_HEADER_BYTES as u64) / INDEX_VALUE_BYTES as u64;
//         Ok(record_count)
//     }

//     /// Get the record's index data.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `index` - Record index.
//     pub fn get_record_index(&self, index: u64) -> Result<Option<IndexValue>, ParseError> {
//         let index_file = File::open(&self.index_path)?;
//         let mut reader = BufReader::new(index_file);
        
//         Self::value_from_file(&mut reader, self.index_header.indexed, index)
//     }

//     /// Updates an index value.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `index` - Index value index.
//     /// * `value` - Index value to save.
//     pub fn update_index_value(&self, index: u64, value: &IndexValue) -> Result<()> {
//         let file = OpenOptions::new()
//             .write(true)
//             .open(&self.index_path)?;
//         let mut writer = BufWriter::new(file);
//         Self::update_index_file_value(&mut writer, index, value)
//     }

//     /// Return the index and index value of the closest non matched record.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `from_index` - Index offset as search starting point.
//     pub fn find_unmatched(&self, from_index: u64) -> Result<Option<(u64, IndexValue)>, ParseError> {
//         // validate index size
//         if self.index_header.indexed_count < 1 {
//             return Ok(None);
//         }

//         // find index size
//         let size = self.calc_record_pos(self.index_header.indexed_count);

//         // seek start point by using the provided offset
//         let file = File::open(&self.index_path)?;
//         let mut reader = BufReader::new(file);
//         let mut pos = INDEX_HEADER_BYTES as u64;
//         let mut index = from_index;
//         pos += INDEX_VALUE_BYTES as u64 * index;
//         reader.seek(SeekFrom::Start(pos))?;

//         // search next unmatched record
//         let mut buf = [0u8; INDEX_VALUE_BYTES];
//         while pos < size {
//             reader.read_exact(&mut buf)?;
//             if buf[INDEX_VALUE_BYTES - 1] < 1u8 {
//                 return Ok(Some((index, IndexValue::try_from(&buf[..])?)));
//             }
//             index += 1;
//             pos += INDEX_VALUE_BYTES as u64;
//         }

//         Ok(None)
//     }

//     /// Perform a healthckeck over the index file by reading
//     /// the headers and checking the file size.
//     pub fn healthcheck(&mut self) -> Result<IndexStatus> {
//         self.load_headers()?;
        
//         // validate headers
//         match self.index_header.hash {
//             Some(saved_hash) => {
//                 let hash = generate_hash(&self.input_path)?;
//                 if saved_hash != hash {
//                     return Ok(IndexStatus::Corrupted);
//                 }
//             },
//             None => return Ok(IndexStatus::New)
//         }

//         // validate incomplete
//         if !self.index_header.indexed {
//             // count indexed records to make sure at least 1 record was indexed
//             if self.count_indexed()? < 1 {
//                 // if not a single record was indexed, then treat it as corrupted
//                 return Ok(IndexStatus::Corrupted);
//             }
//             return Ok(IndexStatus::Incomplete);
//         }

//         // validate file size
//         let real_size = file_size(&self.index_path)?;
//         let size = self.calc_record_pos(self.index_header.indexed_count);
//         if real_size != size {
//             return Ok(IndexStatus::Corrupted);
//         }

//         Ok(IndexStatus::Indexed)
//     }

//     /// Get the last index position.
//     fn last_index_pos(&self) -> u64 {
//         self.calc_record_pos(self.index_header.indexed_count - 1)
//     }

//     /// Get the latest indexed record.
//     fn last_indexed_record(&self) -> Result<Option<IndexValue>, ParseError> {
//         if self.index_header.indexed_count < 1 {
//             return Ok(None);
//         }
//         self.get_record_index(self.index_header.indexed_count - 1)
//     }

//     /// Index a new or incomplete index.
//     fn index_records(&mut self) -> Result<()> {
//         let last_index = self.last_indexed_record()?;

//         // open files to create index
//         let input_file = File::open(&self.input_path)?;
//         let input_file_nav = File::open(&self.input_path)?;
//         let index_file = OpenOptions::new()
//             .create(true)
//             .write(true)
//             .open(&self.index_path)?;

//         // create reader and writer buffers
//         let mut input_rdr = BufReader::new(input_file);
//         let mut input_rdr_nav = BufReader::new(input_file_nav);
//         let mut index_wrt = BufWriter::new(index_file);

//         // find input file size
//         input_rdr_nav.seek(SeekFrom::End(0))?;

//         // seek latest record when exists
//         let mut is_first = true;
//         if let Some(value) = last_index {
//             is_first = false;
//             input_rdr.seek(SeekFrom::Start(value.input_end_pos + 1))?;
//             index_wrt.seek(SeekFrom::Start(self.last_index_pos() + INDEX_VALUE_BYTES as u64))?;
//         }

//         // create index headers
//         if is_first {
//             let header = &self.index_header;
//             let buf_header: Vec<u8> = header.into();
//             index_wrt.write_all(buf_header.as_slice())?;
//             index_wrt.flush()?;
//         }
        
//         // index records
//         let mut input_csv = csv::ReaderBuilder::new()
//             .has_headers(false)
//             .flexible(true)
//             .from_reader(input_rdr);
//         let header_extra_fields_bytes = HEADER_EXTRA_FIELDS.as_bytes();
//         let mut iter = input_csv.records();
//         let mut values_indexed = 0u64;
//         let mut input_start_pos: u64;
//         let mut input_end_pos: u64;
//         loop {
//             let item = iter.next();
//             if item.is_none() {
//                 break;
//             }
//             match item.unwrap() {
//                 Ok(record) => {
//                     // calculate input positions
//                     input_start_pos = record.position().unwrap().byte();
//                     input_end_pos = iter.reader().position().byte();
//                     let length: usize = (input_end_pos - input_start_pos) as usize;

//                     // read CSV file line and store it on the buffer
//                     let mut buf: Vec<u8> = vec![0u8; length];
//                     input_rdr_nav.seek(SeekFrom::Start(input_start_pos))?;
//                     input_rdr_nav.read_exact(&mut buf)?;

//                     // remove new line at the beginning and end of buffer

//                     let mut limit = buf.len();
//                     let mut start_index = 0;
//                     for _ in 0..2 {
//                         if limit - start_index + 1 < 1 {
//                             break;
//                         }
//                         if buf[limit-1] == b'\n' || buf[limit-1] == b'\r' {
//                             input_end_pos -= 1;
//                             limit -= 1;
//                         }
//                         if limit - start_index + 1 < 1 {
//                             break;
//                         }
//                         if buf[start_index] == b'\n' || buf[start_index] == b'\r' {
//                             input_start_pos += 1;
//                             start_index += 1;
//                         }
//                     }

//                     // copy input record into output and add extras
//                     if !is_first {
//                         output_wrt.write_all(&[b'\n'])?;
//                     }
//                     output_wrt.write_all(&buf[start_index..limit])?;
//                     output_pos = output_wrt.stream_position()?;
//                     if is_first {
//                         // write header extra fields when first row
//                         output_wrt.write_all(header_extra_fields_bytes)?;
//                     } else {
//                         // write value extra fields when non first row
//                         values_indexed = 1;
//                         output_wrt.write_all(&self.empty_extra_fields)?;
//                     }
//                 },
//                 Err(e) => bail!(ParseError::from(e))
//             }

//             // skip index write when input headers
//             if is_first{
//                 is_first = false;
//                 continue;
//             }

//             // write index value for this record
//             let value = IndexValue{
//                 input_start_pos,
//                 input_end_pos,
//                 spent_time: 0,
//                 match_flag: MatchFlag::None
//             };
//             //println!("{:?}", value);
//             let buf: Vec<u8> = Vec::from(&value);
//             index_wrt.write_all(&buf[..])?;
//             self.index_header.indexed_count += values_indexed;
//         }

//         // write headers
//         index_wrt.rewind()?;
//         self.index_header.indexed = true;
//         let header = &self.index_header;
//         let buf_header: Vec<u8> = header.into();
//         index_wrt.write_all(buf_header.as_slice())?;
//         index_wrt.flush()?;

//         Ok(())
//     }
    
//     /// Analyze an input file to track each record position
//     /// into an index file.
//     pub fn index(&mut self) -> Result<()> {
//         let mut retry_count = 0;
//         let retry_limit = 3;

//         // initialize index file when required
//         self.init_index(false)?;
//         loop {
//             // retry a few times to fix corrupted index files
//             retry_count += 1;
//             if retry_count > retry_limit {
//                 bail!(ParseError::RetryLimit);
//             }

//             // perform healthcheck over the index file
//             match self.healthcheck()? {
//                 IndexStatus::Indexed => return Ok(()),
//                 IndexStatus::New => {
//                     // create initial header
//                     self.index_header.hash = Some(generate_hash(&self.input_path)?);
//                     break;
//                 },
//                 IndexStatus::Incomplete => break,
//                 IndexStatus::Indexing => bail!(ParseError::Unavailable(IndexStatus::Indexing)),

//                 // recreate index file and retry healthcheck when corrupted
//                 IndexStatus::Corrupted => {
//                     self.init_index(true)?;
//                     continue;
//                 }
//             }
//         }

//         self.index_records()
//     }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;
    use crate::db::record::header::{FieldType, Field};
//     use crate::test_helper::*;
//     use crate::index::index_header::test_helper::build_INDEX_HEADER_BYTES;
//     use crate::index::index_value::test_helper::build_value_bytes;
//     use crate::index::index_header::HASH_SIZE;
//     use tempfile::TempDir;
//     use std::io::{Write, BufWriter};

    /// It's the size of a record header without any field.
    pub const EMPTY_RECORD_BYTES: usize = u32::BYTES;

    /// Record header size generated by add_fields function.
    pub const ADD_FIELDS_HEADER_BYTES: usize = Field::BYTES * 2 + u32::BYTES;

    /// Record size generated by add_fields function.
    pub const ADD_FIELDS_RECORD_BYTES: usize = 17;

    /// Byte slice that represents an empty record header.
    pub const EMPTY_RECORD_HEADER_BYTE_SLICE: [u8; EMPTY_RECORD_BYTES] = [
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

    /// Add test fields into record header.
    /// 
    /// # Arguments
    /// 
    /// * `record_header` - Record header to add fields into.
    pub fn add_fields(record_header: &mut RecordHeader) -> Result<()> {
        record_header.add("foo", FieldType::I32)?;
        record_header.add("bar", FieldType::Str(5))?;

        Ok(())
    }

//     /// Returns the fake input content as bytes.
//     pub fn fake_input_bytes() -> Vec<u8> {
//         "\
//             name,size,price,color\n\
//             fork,\"1 inch\",12.34,red\n\
//             keyboard,medium,23.45,\"black\nwhite\"\n\
//             mouse,\"12 cm\",98.76,white\n\
//             \"rust book\",500 pages,1,\"orange\"\
//         ".as_bytes().to_vec()
//     }

//     /// Returns the fake input hash value.
//     pub fn fake_input_hash() -> [u8; HASH_SIZE] {
//         [ 47, 130, 231, 73, 14, 84, 144, 114, 198, 155, 94, 35, 15,
//           101, 71, 156, 48, 113, 13, 217, 129, 108, 130, 240, 24, 19,
//           159, 141, 205, 59, 71, 227]
//     }

//     /// Create a fake input file.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `path` - Input file path.
//     pub fn create_fake_input(path: &str) -> Result<()> {
//         let file = OpenOptions::new()
//             .create(true)
//             .truncate(true)
//             .write(true)
//             .open(path)?;
//         let mut writer = BufWriter::new(file);
//         writer.write_all(&fake_input_bytes())?;
//         writer.flush()?;

//         Ok(())
//     }

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
            record_header: RecordHeader::new()
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
        let mut buf = [0u8; IndexHeader::BYTES + EMPTY_RECORD_BYTES];
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
    fn read_record_with_fields() {
        
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
//     fn load_headers() {
//         with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<()> {
//             // build fake index file
//             let hash = random_hash();
//             let buf_header = build_INDEX_HEADER_BYTES(true, &hash, true, 3554645435937);
//             let mut buf = [0u8; INDEX_HEADER_BYTES + 20];
//             let buf_frag = &mut buf[..INDEX_HEADER_BYTES];
//             buf_frag.copy_from_slice(&buf_header);
//             create_file_with_bytes(&indexer.index_path, &buf)?;

//             // create expected file contents
//             let expected = IndexHeader{
//                 indexed: true,
//                 hash: Some(hash),
//                 indexed_count: 3554645435937,
//                 fields: FieldTypeHeader::new()
//             };

//             indexer.load_headers()?;

//             assert_eq!(expected, indexer.header);

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