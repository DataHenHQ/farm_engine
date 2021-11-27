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
    IndexHeader,
    IndexValue,
    LoadFrom
};

/// Indexer engine.
#[derive(Debug)]
pub struct Indexer<'index> {
    /// Input file path.
    pub input_path: &'index str,

    /// Index file path.
    pub index_path: String,

    /// Current index header.
    pub header: IndexHeader
}

impl<'index> Indexer<'index> {
    pub fn new(input_path: &'index str, index_path: &str) -> Self {
        Self{
            input_path,
            index_path: index_path.to_string(),
            header: IndexHeader::new()
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
        let hash = generate_hash(&self.input_path)?;
        
        // validate headers
        match self.header.hash {
            Some(saved_hash) => if saved_hash != hash {
                return Ok(IndexStatus::Corrupted);
            },
            None => return Ok(IndexStatus::New)
        }
        if !self.header.indexed {
            return Ok(IndexStatus::Incomplete);
        }

        // validate file size
        // TODO: Use header.indexed_count to check on file size
        aaaaaaaaaaaa
        let size = file_size(self.input_path, false)?;
        let total = match self.count_indexed() {
            Ok(v) => v,
            Err(e) => match e {
                ParseError::IO(_) => return Err(e),
                _ => return Ok(IndexStatus::Corrupted)
            }
        };
        if size != HEADER_LINE_SIZE as u64 + (total * VALUE_LINE_SIZE as u64) {
            return Ok(IndexStatus::Corrupted);
        }

        Ok(IndexStatus::Indexed)
    }
    
    /// Analyze an input file to track new lines and record it's positions into an index file.
    /// Returns total line count.
    /// 
    /// # Arguments
    /// 
    /// * `input_path` - File path to analize.
    /// * `index_path` - File path to write the index.
    pub fn index(&self) -> Result<u64, ParseError> {
        let total = 0;
        self.init_index(false)?;
        loop {
            match self.healthcheck(true)? {
                IndexStatus::Indexed => return self.indexed_count(),
                IndexStatus::New => break,
                IndexStatus::Incomplete => {
                    total = self.indexed_count()?;
                    break;
                },

                // recreate index file and retry healthcheck when corrupted
                IndexStatus::Corrupted => {
                    self.init_index(true)?;
                    continue;
                }
            }
        }

        self.header.record_count = total;
    
        let input_file = File::open(self.input_path)?;
        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(index_path)?;
        
        // config reader and writer buffers
        let mut rdr = BufReader::new(input_file);
        let mut wrt = BufWriter::new(index_file);
    
        // generate index file
        wrt.write
        wrt.write_all()
        wrt.write_all(&EMPTY_INDEX_LINE)?;
        let next_pos = 0;
        for line in rdr.lines() {
            
        }
    
        unimplemented!();
    }
}