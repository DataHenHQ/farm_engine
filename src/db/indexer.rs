pub mod header;
pub mod value;

use anyhow::{bail, Result};
use regex::Regex;
use serde_json::{Map as JSMap, Value as JSValue};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
use crate::error::ParseError;
use crate::{file_size, generate_hash};
use crate::error::IndexError;
use crate::traits::{ByteSized, LoadFrom, ReadFrom, WriteTo};
use header::{Header, InputType};
use value::{MatchFlag, Data, Value};

/// Indexer version.
pub const VERSION: u32 = 2;

/// Index file extension.
pub const FILE_EXTENSION: &str = "fmindex";

/// Default indexing batch size before updating headers.
const DEFAULT_BATCH_SIZE: u64 = 100;

/// index healthcheck status.
#[derive(Debug, PartialEq)]
pub enum Status {
    New,
    Indexed,
    Incomplete,
    Corrupted,
    Indexing,
    WrongInputFile
}

impl Display for Status{
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
#[derive(Debug, PartialEq, Clone)]
pub struct Indexer {
    /// Input file path.
    pub input_path: PathBuf,

    /// Index file path.
    pub index_path: PathBuf,

    /// Index header data.
    pub header: Header,

    /// Indexing batch size before updating the index header.
    pub batch_size: u64,

    /// Input field name list.
    pub input_fields: Vec<String>,
}

impl Indexer {
    /// Generates a regex expression to validate the index file extension.
    pub fn file_extension_regex() -> Regex {
        let expression = format!(r"(?i)\.{}$", FILE_EXTENSION);
        Regex::new(&expression).unwrap()
    }

    /// Calculate the target value position at the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn calc_value_pos(index: u64) -> u64 {
        Header::BYTES as u64 + index * Value::BYTES as u64
    }

    /// Create a new indexer instance.
    /// 
    /// # Arguments
    /// 
    /// * `input_path` - Source Input file path.
    /// * `index_path` - Target index file path.
    pub fn new(input_path: PathBuf, index_path: PathBuf, input_type: InputType) -> Self {
        let mut header = Header::new();
        header.input_type = input_type;
        Self{
            input_path,
            index_path,
            header,
            batch_size: DEFAULT_BATCH_SIZE,
            input_fields: Vec::new()
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

    /// Move to the index header position and then loads it.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    pub fn load_header_from(&mut self, reader: &mut (impl Read + Seek)) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        self.header.load_from(reader)?;
        Ok(())
    }

    /// Move to index position and then read the index value from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    pub fn seek_value_from(&self, reader: &mut (impl Read + Seek), index: u64, force: bool) -> Result<Option<Value>> {
        if !force && !self.header.indexed {
            bail!("input file must be indexed before reading values")
        }
        if self.header.indexed_count > index {
            let pos = Self::calc_value_pos(index);
            reader.seek(SeekFrom::Start(pos))?;
            return Ok(Some(Value::read_from(reader)?));
        }
        Ok(None)
    }

    /// Read the index value from the index file.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    pub fn value(&self, index: u64) -> Result<Option<Value>> {
        let mut reader = self.new_index_reader()?;
        self.seek_value_from(&mut reader, index, false)
    }

    /// Parse the input record from an index value as CSV.
    /// 
    /// # Arguments
    /// 
    /// * `value` - Index value
    fn parse_csv_input(&self, value: &Value) -> Result<JSMap<String, JSValue>> {
        // create CSV headers
        let mut buf = Vec::new();
        let limit = self.input_fields.len();
        if limit < 1 {
            bail!(IndexError::NoInputFields)
        }
        buf.extend_from_slice(self.input_fields[0].as_bytes());
        if limit > 1 {
            for i in 1..limit {
                buf.push(b',');
                buf.extend(self.input_fields[i].as_bytes());
            }
        }
        buf.push(b'\n');

        // read input record
        let mut reader = self.new_input_reader()?;
        buf.append(&mut value.read_input_from(&mut reader)?);

        // deserialize CSV string object into a JSON map
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(buf.as_slice());
        match csv_reader.deserialize().next() {
            Some(result) => match result {
                Ok(record) => Ok(record),
                Err(e) => {
                    eprintln!(
                        "Couldn't parse input record at byte position {}: {}",
                        value.input_start_pos,
                        e
                    );
                    bail!(ParseError::InvalidFormat)
                }
            },
            None => bail!(ParseError::InvalidValue)
        }
    }

    /// Parse the input record from an index value as JSON.
    /// 
    /// # Arguments
    /// 
    /// * `value` - Index value
    fn parse_json_input(&self, value: &Value) -> Result<JSMap<String, JSValue>> {
        let mut reader = self.new_input_reader()?;
        let buf = value.read_input_from(&mut reader)?;
        Ok(serde_json::from_reader(buf.as_slice())?)
    }

    /// Parse the input record from an index value.
    /// 
    /// # Arguments
    /// 
    /// * `value` - Index value
    pub fn parse_input(&self, value: &Value) -> Result<JSMap<String, JSValue>> {
        if !self.header.indexed {
            bail!("input file must be indexed before parsing and input value")
        }

        match self.header.input_type {
            InputType::CSV => self.parse_csv_input(value),
            InputType::JSON => self.parse_json_input(value),
            InputType::Unknown => bail!("not supported input file type")
        }
    }

    /// Updates or append an index value into the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Value index.
    /// * `value` - Index value data to save.
    pub fn save_value(&self, index: u64, value: &Value) -> Result<()> {
        let pos = Self::calc_value_pos(index);
        let mut writer = self.new_index_writer(false)?;
        writer.seek(SeekFrom::Start(pos))?;
        value.write_to(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    /// Updates or append an index value data into the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Value index.
    /// * `data` - Index value data to save.
    pub fn save_data(&self, index: u64, data: &Data) -> Result<()> {
        let pos = Self::calc_value_pos(index) + Value::DATA_OFFSET as u64;
        let mut writer = self.new_index_writer(false)?;
        writer.seek(SeekFrom::Start(pos))?;
        data.write_to(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    /// Return the index of the closest non-processed value.
    /// 
    /// # Arguments
    /// 
    /// * `from_index` - Index offset as search starting point.
    pub fn find_pending(&self, from_index: u64) -> Result<Option<u64>> {
        // validate indexed
        if !self.header.indexed {
            bail!(IndexError::Unavailable(Status::Incomplete));
        }

        // validate index size
        if self.header.indexed_count < 1 {
            return Ok(None);
        }

        // seek start point by using the provided offset
        let mut reader = self.new_index_reader()?;
        let mut index = from_index;
        let mut pos = Self::calc_value_pos(index);
        reader.seek(SeekFrom::Start(pos))?;

        // search next unmatched record
        let mut buf = [0u8; Value::BYTES];
        let limit = Self::calc_value_pos(self.header.indexed_count);
        while pos < limit {
            reader.read_exact(&mut buf)?;
            if buf[Value::MATCH_FLAG_BYTE_INDEX] < 1u8 {
                return Ok(Some(index));
            }
            index += 1;
            pos += Value::BYTES as u64;
        }

        Ok(None)
    }

    /// Perform a healthckeck over the index file by reading
    /// the headers and checking the file size.
    pub fn healthcheck(&mut self) -> Result<Status> {
        // calculate the input hash
        let mut reader = self.new_input_reader()?;
        let hash = generate_hash(&mut reader)?;

        // check whenever index file exists
        match self.new_index_reader() {
            // try to load the index headers
            Ok(mut reader) => if let Err(e) = self.load_header_from(&mut reader) {
                match e.downcast::<std::io::Error>() {
                    Ok(ex) => match ex.kind() {
                        std::io::ErrorKind::NotFound => {
                            // File not found so the index is new
                            return Ok(Status::New);
                        }
                        std::io::ErrorKind::UnexpectedEof => {
                            // if the file is empty then is new
                            let real_size = file_size(&self.index_path)?;
                            if real_size < 1 {
                                // store hash and return as new index
                                self.header.hash = Some(hash);
                                return Ok(Status::New);
                            }

                            // EOF eror means the index is corrupted
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
                        // store hash and return as new index
                        self.header.hash = Some(hash);
                        return Ok(Status::New)
                    },
                    _ => bail!(ex)
                },
                Err(ex) => bail!(ex)
            }
        };
        
        // validate input hash match
        match self.header.hash {
            Some(saved_hash) => {
                // validate input file hash
                if saved_hash != hash {
                    return Ok(Status::WrongInputFile);
                }
            },
            None => {
                // not having a hash means the index is corrupted
                return Ok(Status::Corrupted)
            }
        }
        

        // validate corrupted index
        let real_size = file_size(&self.index_path)?;
        let expected_size = Self::calc_value_pos(self.header.indexed_count);
        if self.header.indexed {
            if real_size != expected_size {
                // sizes don't match, the file is corrupted
                return Ok(Status::Corrupted);
            }
        } else {
            if real_size < expected_size {
                // sizes is smaller, the file is corrupted
                return Ok(Status::Corrupted);
            }
            // index is incomplete
            return Ok(Status::Incomplete);
        }

        // all good, the index is indexed
        Ok(Status::Indexed)
    }

    /// Saves the index header and then jump back to the last writer stream position.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    pub fn save_header(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.flush()?;
        let old_pos = writer.stream_position()?;
        writer.rewind()?;
        self.header.write_to(writer)?;
        writer.flush()?;
        writer.seek(SeekFrom::Start(old_pos))?;
        Ok(())
    }

    /// Loads fields names from a CSV input file.
    fn load_input_csv_fields(&mut self) -> Result<()> {
        let reader = self.new_input_reader()?;
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(reader);
        let mut fields = Vec::new();
        for field in csv_reader.headers()? {
            fields.push(field.to_string());
        }
        self.input_fields = fields;
        Ok(())
    }

    /// Loads fields names from a CSV input file.
    fn load_input_json_fields(&mut self) -> Result<()> {
        unimplemented!()
    }

    /// Loads the input fields.
    pub fn load_input_fields(&mut self) -> Result<()> {
        match self.header.input_type {
            InputType::CSV => self.load_input_csv_fields(),
            InputType::JSON => self.load_input_json_fields(),
            InputType::Unknown => bail!("not supported input file type")
        }
    }

    /// Process a CSV item into an Value.
    /// 
    /// # Arguments
    /// 
    /// * `iter` - CSV iterator.
    /// * `item` - Last CSV item read from the iterator.
    /// * `input_reader` - Input navigation reader used to adjust positions.
    fn index_csv_record(&self, iter: &csv::StringRecordsIter<impl Read>, item: csv::StringRecord, input_reader: &mut (impl Read + Seek)) -> Result<Value> {
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
        Ok(Value{
            input_start_pos: start_pos,
            input_end_pos: end_pos,
            data: Data{
                spent_time: 0,
                match_flag: MatchFlag::None
            }
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
        let mut is_first = is_first;
        let mut input_rdr_nav = self.new_input_reader()?;
        let mut input_csv = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(input_rdr);
        let mut iter = input_csv.records();
        'records: loop {
            match iter.next() {
                None => break 'records,
                Some(item) => {
                    // skip CSV headers
                    if is_first {
                        is_first = false;
                        continue 'records;
                    }

                    // create index value
                    let value = match item {
                        Ok(v) => self.index_csv_record(&iter, v, &mut input_rdr_nav)?,
                        Err(e) => bail!(e)
                    };

                    // write index value for this record
                    value.write_to(index_wrt)?;
                    self.header.indexed_count += 1;

                    // save headers every batch
                    if self.header.indexed_count % self.batch_size < 1 {
                        self.save_header(index_wrt)?;
                    }
                }
            }
        }

        // write headers
        self.header.indexed = true;
        self.save_header(index_wrt)?;

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
                Status::Indexed => {
                    self.load_input_fields()?;
                    return Ok(())
                },
                Status::Incomplete => {
                    // read last indexed record or create the index file
                    let mut reader = self.new_index_reader()?;
                    match self.seek_value_from(&mut reader, self.header.indexed_count, true)? {
                        Some(value) => {
                            // load last known indexed value position
                            is_first = false;
                            input_rdr.seek(SeekFrom::Start(value.input_end_pos + 1))?;
                            let next_pos = Self::calc_value_pos(self.header.indexed_count);
                            index_wrt.seek(SeekFrom::Start(next_pos))?;
                        },
                        None => {}
                    }
                },
                Status::New => {
                    // create index headers
                    self.header.write_to(&mut index_wrt)?;
                    index_wrt.flush()?;
                }
                vu => bail!(IndexError::Unavailable(vu))
            },
            Err(e) => return Err(e)
        }

        // index input file
        self.load_input_fields()?;
        match self.header.input_type {
            InputType::CSV => self.index_csv(&mut input_rdr, &mut index_wrt, is_first),
            InputType::JSON => unimplemented!(),
            InputType::Unknown => bail!("not supported input file type")
        }
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;
    use crate::test_helper::*;
    use crate::db::indexer::header::{HASH_SIZE};
    use crate::db::indexer::header::test_helper::{random_hash, build_header_bytes};
//     use crate::index::header::test_helper::build_INDEX_HEADER_BYTES;
//     use crate::index::value::test_helper::build_value_bytes;
    use tempfile::TempDir;
//     use std::io::{Write, BufWriter};

    /// Fake records without fields bytes.
    pub const FAKE_VALUES_BYTES: usize = Value::BYTES * 3;

    /// Fake index with fields byte size.
    pub const FAKE_INDEX_BYTES: usize = Header::BYTES + FAKE_VALUES_BYTES;

    /// Fake values only byte slice.
    pub const FAKE_VALUES_BYTE_SLICE: [u8; FAKE_VALUES_BYTES] = [
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

    /// Create fake records without fields.
    /// 
    /// # Arguments
    /// 
    /// * `records` - Record vector to add records into.
    pub fn fake_values() -> Result<Vec<Value>> {
        let mut values = Vec::new();

        // add first value
        values.push(Value{
            input_start_pos: 50,
            input_end_pos: 100,
            data: Data{
                match_flag: MatchFlag::Yes,
                spent_time: 150
            }
        });

        // add second value
        values.push(Value{
            input_start_pos: 200,
            input_end_pos: 250,
            data: Data{
                match_flag: MatchFlag::None,
                spent_time: 300
            }
        });

        // add third value
        values.push(Value{
            input_start_pos: 350,
            input_end_pos: 400,
            data: Data{
                match_flag: MatchFlag::Skip,
                spent_time: 450
            }
        });

        Ok(values)
    }

    /// Return a fake index file without fields as byte slice.
    pub fn fake_index() -> Result<([u8; FAKE_INDEX_BYTES], u64)> {
        // init buffer
        let mut buf = [0u8; FAKE_INDEX_BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 3245634545244324234u64, InputType::CSV);
        copy_bytes(&mut buf, &index_header_buf, 0)?;
        copy_bytes(&mut buf, &FAKE_VALUES_BYTE_SLICE, Header::BYTES)?;
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

    /// Write a fake index bytes into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `unprocessed` - If `true` then build all values with MatchFlag::None.
    pub fn write_fake_index(writer: &mut (impl Seek + Write), unprocessed: bool) -> Result<Vec<Value>> {
        let mut values = Vec::new();

        // write index header
        let mut header = Header::new();
        header.indexed = true;
        header.indexed_count = 4;
        header.input_type = InputType::CSV;
        header.hash = Some(fake_input_hash());
        header.write_to(writer)?;
        
        // write first value
        let mut value = Value::new();
        value.input_start_pos = 22;
        value.input_end_pos = 44;
        if !unprocessed {
            value.data.match_flag = MatchFlag::Yes;
            value.data.spent_time = 23;
        }
        value.write_to(writer)?;
        values.push(value);
        
        // write second value
        let mut value = Value::new();
        value.input_start_pos = 46;
        value.input_end_pos = 80;
        if !unprocessed {
            value.data.match_flag = MatchFlag::No;
            value.data.spent_time = 25;
        }
        value.write_to(writer)?;
        values.push(value);
        
        // write third value
        let mut value = Value::new();
        value.input_start_pos = 82;
        value.input_end_pos = 106;
        if !unprocessed {
            value.data.match_flag = MatchFlag::None;
            value.data.spent_time = 30;
        }
        value.write_to(writer)?;
        values.push(value);

        // write fourth value
        let mut value = Value::new();
        value.input_start_pos = 108;
        value.input_end_pos = 139;
        if !unprocessed {
            value.data.match_flag = MatchFlag::Skip;
            value.data.spent_time = 41;
        }
        value.write_to(writer)?;
        values.push(value);

        Ok(values)
    }

    /// Create a fake index file based on the default fake input file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Index file path.
    /// * `empty` - If `true` then build all values with MatchFlag::None.
    pub fn create_fake_index(path: &PathBuf, unprocessed: bool) -> Result<Vec<Value>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        let values = write_fake_index(&mut writer, unprocessed)?;
        writer.flush()?;

        Ok(values)
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
                index_path,
                InputType::Unknown
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
    use serde_json::Number as JSNumber;
    use std::io::Cursor;
    use crate::test_helper::*;
    use crate::db::indexer::header::{HASH_SIZE};
    use crate::db::indexer::header::test_helper::{random_hash, build_header_bytes};

    #[test]
    fn file_extension_regex() {
        let rx = Indexer::file_extension_regex();
        assert!(rx.is_match("hello.fmindex"), "expected to match \"hello.fmindex\" but got false");
        assert!(rx.is_match("/path/to/hello.fmindex"), "expected to match \"/path/to/hello.fmindex\" but got false");
        assert!(!rx.is_match("hello.index"), "expected to not match \"hello.index\" but got true");
    }

    #[test]
    fn new() {
        let mut header = Header::new();
        header.input_type = InputType::JSON;
        let expected = Indexer{
            input_path: "my_input.csv".into(),
            index_path: "my_index.fmidx".into(),
            header,
            batch_size: DEFAULT_BATCH_SIZE,
            input_fields: Vec::new()
        };
        let indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::JSON);
        assert_eq!(expected, indexer);
    }

    #[test]
    fn calc_record_pos() {
        assert_eq!(108, Indexer::calc_value_pos(2));
    }

    #[test]
    fn load_header_from() {
        // create buffer
        let mut buf = [0u8; Header::BYTES + Header::BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 5245634545244324234u64, InputType::CSV);
        if let Err(e) = copy_bytes(&mut buf, &index_header_buf, 0) {
            assert!(false, "{:?}", e);
        }
        let mut reader = Cursor::new(buf.to_vec());

        // test load_headers
        let mut indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        if let Err(e) = indexer.load_header_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
        }

        // check expected index header
        let mut expected = Header::new();
        expected.indexed = true;
        expected.hash = Some(hash_buf);
        expected.indexed_count = 5245634545244324234u64;
        expected.input_type = InputType::CSV;
        assert_eq!(expected, indexer.header);
    }

    #[test]
    fn seek_value_from() {
        // init buffer
        let (buf, record_count) = match fake_index() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init indexer and expected records
        let mut indexer = Indexer::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        indexer.header.indexed = true;
        indexer.header.indexed_count = record_count;
        let expected = match fake_values() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test first value
        let value = match indexer.seek_value_from(&mut reader, 0, false) {
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
        assert_eq!(expected[0], value);

        // test second value
        let value = match indexer.seek_value_from(&mut reader, 1, false) {
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
        assert_eq!(expected[1], value);

        // test third value
        let value = match indexer.seek_value_from(&mut reader, 2, false) {
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
        assert_eq!(expected[2], value);
    }

    #[test]
    fn value() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // init buffer
            let (buf, value_count) = match fake_index() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            create_file_with_bytes(&indexer.index_path, &buf)?;

            // init indexer and expected records
            indexer.header.indexed = true;
            indexer.header.indexed_count = value_count;
            let expected = match fake_values() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };

            // test first value
            let value = match indexer.value(0) {
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
            assert_eq!(expected[0], value);

            // test second value
            let value = match indexer.value(1) {
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
            assert_eq!(expected[1], value);

            // test third value
            let value = match indexer.value(2) {
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
            assert_eq!(expected[2], value);

            Ok(())
        });
    }

    #[test]
    fn parse_csv_input() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create input and setup indexer
            create_fake_input(&indexer.input_path)?;
            indexer.input_fields = vec![
                "name".to_string(),
                "size".to_string(),
                "price".to_string(),
                "color".to_string()
            ];
            let value = Value{
                input_start_pos: 46,
                input_end_pos: 80,
                data: Data{
                    spent_time: 0,
                    match_flag: MatchFlag::None
                }
            };
            
            // test
            let mut expected = JSMap::new();
            expected.insert("name".to_string(), JSValue::String("keyboard".to_string()));
            expected.insert("size".to_string(), JSValue::String("medium".to_string()));
            expected.insert("price".to_string(), JSValue::Number(JSNumber::from_f64(23.45f64).unwrap()));
            expected.insert("color".to_string(), JSValue::String("black\nwhite".to_string()));
            match indexer.parse_csv_input(&value) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            Ok(())
        });
    }

    #[test]
    fn parse_csv_input_without_input_fields() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create input
            create_fake_input(&indexer.input_path)?;
            let value = Value{
                input_start_pos: 46,
                input_end_pos: 80,
                data: Data{
                    spent_time: 0,
                    match_flag: MatchFlag::None
                }
            };
            
            // test
            let expected = "the input doesn't have any fields";
            match indexer.parse_csv_input(&value) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }

            Ok(())
        });
    }

    #[test]
    fn save_value() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            let mut values = create_fake_index(&indexer.index_path, true)?;
            let pos = Indexer::calc_value_pos(2);
            let mut buf = [0u8; Value::BYTES];
            let file = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(file);
            let mut old_bytes_before = vec!(0u8; pos as usize);
            let mut old_bytes_after = [0u8; Value::BYTES];
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

            // save value and check value
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
            values[2].input_start_pos = 10;
            values[2].input_end_pos = 27;
            values[2].data.match_flag = MatchFlag::Yes;
            values[2].data.spent_time = 93;
            if let Err(e) = indexer.save_value(2, &values[2]) {
                assert!(false, "expected success but got error: {:?}", e)
            }
            reader.seek(SeekFrom::Start(0))?;
            let mut new_bytes_before = vec!(0u8; pos as usize);
            let mut new_bytes_after = [0u8; Value::BYTES];
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
    fn save_data() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            let mut values = create_fake_index(&indexer.index_path, true)?;
            let pos = Indexer::calc_value_pos(2);
            let mut buf = [0u8; Value::BYTES];
            let file = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(file);
            let mut old_bytes_before = vec!(0u8; pos as usize);
            let mut old_bytes_after = [0u8; Value::BYTES];
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

            // save value and check value
            let expected = [
                // start_pos
                0, 0, 0, 0, 0, 0, 0, 82u8,
                // end_pos
                0, 0, 0, 0, 0, 0, 0, 106u8,
                // spent_time
                0, 0, 0, 0, 0, 0, 0, 93u8,
                // match flag
                b'Y'
            ];
            values[2].data.match_flag = MatchFlag::Yes;
            values[2].data.spent_time = 93;
            if let Err(e) = indexer.save_data(2, &values[2].data) {
                assert!(false, "expected success but got error: {:?}", e)
            }
            reader.seek(SeekFrom::Start(0))?;
            let mut new_bytes_before = vec!(0u8; pos as usize);
            let mut new_bytes_after = [0u8; Value::BYTES];
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
    fn find_pending() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index
            let mut values = create_fake_index(&indexer.index_path, false)?;
            indexer.header.indexed = true;
            indexer.header.indexed_count = 4;

            // find existing unmatched from start position
            match indexer.find_pending(0) {
                Ok(opt) => match opt {
                    Some(v) => assert_eq!(2, v),
                    None => assert!(false, "expected 2 but got None")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            // find non-existing unmatched from starting point
            values[2].data.match_flag = MatchFlag::Yes;
            indexer.save_value(2, &values[2])?;
            match indexer.find_pending(3) {
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
    fn find_pending_with_offset() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            create_fake_index(&indexer.index_path, false)?;
            indexer.header.indexed = true;
            indexer.header.indexed_count = 4;

            // find existing unmatched with offset
            match indexer.find_pending(1) {
                Ok(opt) => match opt {
                    Some(v) => assert_eq!(2, v),
                    None => assert!(false, "expected 2 but got None")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            // find non-existing unmatched with offset
            match indexer.find_pending(3) {
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
    fn find_pending_with_non_indexed() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            create_fake_index(&indexer.index_path, false)?;
            indexer.header.indexed_count = 4;

            // find existing unmatched with offset
            match indexer.find_pending(1) {
                Ok(opt) => assert!(false, "expected error but got {:?}", opt),
                Err(e) => match e.downcast::<IndexError>(){
                    Ok(ex) => match ex {
                        IndexError::Unavailable(status) => match status {
                            Status::Incomplete => {},
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
    fn find_pending_with_offset_overflow() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            create_fake_index(&indexer.index_path, false)?;
            indexer.header.indexed = true;
            indexer.header.indexed_count = 2;

            // find existing unmatched with offset
            match indexer.find_pending(5) {
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
            let expected = Status::New;
            match indexer.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test fake hash
            let expected = fake_input_hash();
            match indexer.header.hash {
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
            let expected = Status::New;
            match indexer.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test fake hash
            let expected = fake_input_hash();
            match indexer.header.hash {
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
            let expected = Status::Corrupted;
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
            let mut buf = [0u8; Header::BYTES];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new();
            header.hash = Some([3u8; HASH_SIZE]);
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::WrongInputFile, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_incomplete_corrupted() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; Header::BYTES+Header::BYTES+5];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new();
            header.indexed_count = 10;
            header.hash = Some(fake_input_hash());
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_incomplete_valid() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; Header::BYTES+Header::BYTES+FAKE_VALUES_BYTES];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new();
            header.indexed_count = 3;
            header.hash = Some(fake_input_hash());
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::Incomplete, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_indexed_corrupted() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; Header::BYTES+Header::BYTES+5];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new();
            header.indexed = true;
            header.indexed_count = 8;
            header.hash = Some(fake_input_hash());
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_indexed_valid() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            create_fake_index(&indexer.index_path, false)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::Indexed, indexer.healthcheck()?);
            Ok(())
        });
    }

    #[test]
    fn save_header() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            // create index file and read index header data
            create_fake_index(&indexer.index_path, false)?;
            let mut reader = indexer.new_index_reader()?;
            let mut expected = [0u8; Header::BYTES];
            reader.read_exact(&mut expected)?;
            reader.seek(SeekFrom::Start(0))?;
            indexer.header.load_from(&mut reader)?;

            // test save index header
            let mut buf = [0u8; Header::BYTES];
            let wrt = &mut buf as &mut [u8];
            let mut writer = Cursor::new(wrt);
            if let Err(e) = indexer.save_header(&mut writer) {
                assert!(false, "expected success but got error: {:?}", e);
            };
            assert_eq!(expected, buf);
            
            Ok(())
        });
    }

    #[test]
    fn load_input_csv_fields() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let expected = vec![
                "name".to_string(),
                "size".to_string(),
                "price".to_string(),
                "color".to_string()
            ];
            create_fake_input(&indexer.input_path)?;
            if let Err(e) = indexer.load_input_csv_fields() {
                assert!(false, "expected success but got error: {:?}", e)
            }
            assert_eq!(expected, indexer.input_fields);

            Ok(())
        });
    }

    #[test]
    fn load_input_fields_as_csv() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let expected = vec![
                "name".to_string(),
                "size".to_string(),
                "price".to_string(),
                "color".to_string()
            ];
            create_fake_input(&indexer.input_path)?;
            if let Err(e) = indexer.load_input_csv_fields() {
                assert!(false, "expected success but got error: {:?}", e)
            }
            assert_eq!(expected, indexer.input_fields);
            Ok(())
        });
    }

    #[test]
    fn index_new() {
        with_tmpdir_and_indexer(&|dir, indexer| -> Result<()> {
            create_fake_input(&indexer.input_path)?;
            indexer.header.input_type = InputType::CSV;

            // index input file
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
            write_fake_index(&mut writer, true)?;
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

            // validate input fields
            let expected = vec![
                "name".to_string(),
                "size".to_string(),
                "price".to_string(),
                "color".to_string()
            ];
            assert_eq!(expected, indexer.input_fields);
            
            Ok(())
        });
    }

    #[test]
    fn index_existing() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            create_fake_input(&indexer.input_path)?;
            create_fake_index(&indexer.index_path, true)?;

            // index input file
            if let Err(e) = indexer.index() {
                assert!(false, "expected success but got error: {:?}", e);
            }

            // create expected index
            let mut expected = Indexer::new(
                indexer.input_path.clone(),
                indexer.index_path.clone(),
                InputType::CSV
            );
            expected.input_fields.push("name".to_string());
            expected.input_fields.push("size".to_string());
            expected.input_fields.push("price".to_string());
            expected.input_fields.push("color".to_string());
            expected.header.indexed = true;
            expected.header.hash = Some(fake_input_hash());
            expected.header.indexed_count = 4;
            assert_eq!(&mut expected, indexer);
            
            Ok(())
        });
    }
}