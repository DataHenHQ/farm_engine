use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::str::FromStr;
use std::convert::TryFrom;
use std::io::{Seek, SeekFrom, Read, Write, BufRead, BufReader, BufWriter};
use crate::engine::error::ParseError;
use crate::engine::{FillAction, file_size, fill_file, generate_hash};
use crate::engine::index::IndexStatus;
use super::data::{
    HEADER_LINE_SIZE,
    VALUE_LINE_SIZE,
    MatchFlag,
    IndexHeader,
    IndexValue,
    LoadFrom
};

const HEADER_EXTRA_FIELDS: &'static str = ",match,time,comments";

/// Indexer engine.
#[derive(Debug)]
pub struct Indexer<'index> {
    /// Input file path.
    pub input_path: &'index str,

    /// Output file path.
    pub output_path: &'index str,

    /// Index file path.
    pub index_path: String,

    /// Current index header.
    pub header: IndexHeader,

    /// Empty extra fields line.
    empty_extra_fields: Vec<u8>
}

impl<'index> Indexer<'index> {
    pub fn new(input_path: &'index str, output_path: &'index str, index_path: &str) -> Self {
        Self{
            input_path,
            output_path,
            index_path: index_path.to_string(),
            header: IndexHeader::new(),
            empty_extra_fields: format!(
                ",{:match_flag$},{:time$},{:comments$}",
                "",
                "",
                "",
                match_flag=1,
                time=20,
                comments=200
            ).as_bytes().to_vec()
        }
    }

    /// Initialize index file.
    /// 
    /// # Arguments
    /// 
    /// * `truncate` - If `true` then it truncates de file and initialize it.
    pub fn init_index(&self, truncate: bool) -> std::io::Result<()> {
        fill_file(&self.index_path, HEADER_LINE_SIZE as u64, truncate)?;
        Ok(())
    }

    /// Load index headers.
    pub fn load_headers(&self) -> Result<(), ParseError> {
        let file = File::open(self.index_path)?;
        let reader = BufReader::new(file);

        let mut buf: Vec<u8> = vec![0u8; HEADER_LINE_SIZE];
        reader.read_exact(&mut buf)?;
        self.header.load_from(&buf[..])?;

        Ok(())
    }

    /// Count how many records has been indexed so far.
    pub fn count_indexed(&self) -> Result<u64, ParseError> {
        let file = File::open(&self.input_path)?;

        // get and validate file size
        let limit = HEADER_LINE_SIZE as u64;
        let mut size = file.metadata()?.len() - HEADER_LINE_SIZE as u64;
        if size < 0 {
            return Err(ParseError::InvalidSize);
        }

        // calculate record count
        let record_count = size % VALUE_LINE_SIZE as u64;
        Ok(record_count)
    }

    /// Calculate the target record's index data position at the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn calc_record_index_pos(index: u64) -> u64 {
        HEADER_LINE_SIZE as u64 + index * VALUE_LINE_SIZE as u64
    }

    /// Get the record's index data.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn get_record_index(&self, index: u64) -> Result<Option<IndexValue>, ParseError> {
        let index_pos = Self::calc_record_index_pos(index);

        let index_file = File::open(self.index_path)?;
        let mut reader = BufReader::new(index_file);

        // validate record index position
        reader.seek(SeekFrom::End(0))?;
        let size = reader.stream_position()?;
        if size < index_pos {
            if self.header.indexed {
                return Ok(None);
            }
            return Err(ParseError::Unavailable(IndexStatus::Indexing))
        }

        // retrive input pos
        reader.seek(SeekFrom::Start(index_pos))?;
        let buf = [0u8; VALUE_LINE_SIZE];
        reader.read_exact(&mut buf)?;

        Ok(Some(IndexValue::try_from(&buf[..])?))
    }

    /// Perform a healthckeck over the index file by reading
    /// the headers and checking the file size.
    /// 
    /// # Arguments
    /// 
    /// * `update_header` - If `true` then it updates index header attibute.
    pub fn healthcheck(&self) -> Result<IndexStatus, ParseError> {
        self.load_headers()?;
        
        // validate headers
        match self.header.hash {
            Some(saved_hash) => {
                let hash = generate_hash(&self.input_path)?;
                if saved_hash != hash {
                    return Ok(IndexStatus::Corrupted);
                }
            },
            None => return Ok(IndexStatus::New)
        }
        if !self.header.indexed {
            return Ok(IndexStatus::Incomplete);
        }

        // validate file size
        match self.header.indexed_count {
            Some(total) => {
                let real_size = file_size(&self.index_path, false)?;
                let size = Self::calc_record_index_pos(total + 1);
                if real_size != size {
                    return Ok(IndexStatus::Corrupted);
                }
            },
            None => return Ok(IndexStatus::Corrupted)
        }

        Ok(IndexStatus::Indexed)
    }

    /// Index a new or incomplete index.
    fn index_internal(&self) -> Result<(), ParseError> {
        // count indexed records to recover any incomplete index
        let indexed_count = self.count_indexed()?;
        self.header.indexed_count = Some(indexed_count);

        // seek latest indexed
        let mut last_input_pos = 0u64;
        let mut output_pos = 0u64;
        if indexed_count > 0 {
            let value = self.get_record_index(indexed_count)?.unwrap();
            last_input_pos = value.input_pos.unwrap();
            output_pos = value.output_pos.unwrap();

            if indexed_count > 1 {
                let prev_value = self.get_record_index(indexed_count - 1)?.unwrap();
                last_input_pos = prev_value.input_pos.unwrap();
            }
        }

        // open files to create index
        let input_file = File::open(self.input_path)?;
        let output_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.output_path)?;
        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.index_path)?;

        // create reader and writer buffers
        let mut input_rdr = BufReader::new(input_file);
        let mut output_wrt = BufWriter::new(index_file);
        let mut index_wrt = BufWriter::new(index_file);

        // read CSV and create index
        let mut input_csv = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(input_rdr);
        for result in input_csv.byte_records() {
            match result {
                Ok(record) => {
                    let buf = &record.as_slice();


                    // copy input record into output and add extras
                    output_wrt.write(buf);
                    output_pos = output_wrt.stream_position()?;
                    output_wrt.write(&self.empty_extra_fields);
                },
                Err(e) => return Err(ParseError::CSV(e))
            }
            
            // write index value for this record
            let value = IndexValue{
                input_pos: Some(last_input_pos),
                output_pos: Some(output_pos),
                match_flag: MatchFlag::None
            };
            let buf: Vec<u8> = Vec::from(&value);
            index_wrt.write(&buf[..]);
        }

        unimplemented!();
    }
    
    /// Analyze an input file to track each record position
    /// into an index file.
    pub fn index(&self) -> Result<(), ParseError> {
        let retry_count = 0;
        let retry_limit = 3;

        // initialize index file when required
        self.init_index(false)?;
        loop {
            // retry a few times to fix corrupted index files
            retry_count += 1;
            if retry_count > retry_limit {
                return Err(ParseError::RetryLimit);
            }

            // perform healthcheck over the index file
            match self.healthcheck()? {
                IndexStatus::Indexed => return Ok(()),
                IndexStatus::New => {
                    // create initial header
                    self.header.hash = Some(generate_hash(&self.input_path)?);
                    break;
                },
                IndexStatus::Incomplete => break,

                // recreate index file and retry healthcheck when corrupted
                IndexStatus::Corrupted => {
                    self.init_index(true)?;
                    continue;
                }
            }
        }

        self.index_internal()
    }
}