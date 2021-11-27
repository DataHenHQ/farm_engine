use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::str::FromStr;
use std::convert::TryFrom;
use std::io::{Seek, SeekFrom, Read, Write, BufRead, BufReader, BufWriter};
use crate::engine::error::ParseError;
use crate::engine::{FillAction, file_size, fill_file, generate_hash};
use super::data::{
    HEADER_LINE_SIZE,
    VALUE_LINE_SIZE,
    IndexHeader,
    IndexValue,
    IndexStatus
};

/// Indexer engine.
#[derive(Debug)]
pub struct Indexer<'index> {
    /// Input file path.
    pub input_path: &'index str,

    /// Index file path.
    pub index_path: String,

    /// Record count.
    pub record_count: u64,

    /// Current index header.
    pub header: IndexHeader
}

impl<'index> Indexer<'index> {
    /// Initialize index file.
    /// 
    /// # Arguments
    /// 
    /// * `truncate` - If `true` then it truncates de file and initialize it.
    pub fn init_index(&self, truncate: bool) -> std::io::Result<()> {
        fill_file(&self.index_path, HEADER_LINE_SIZE as u64, truncate)?;
        Ok(())
    }

    /// Read index headers.
    pub fn read_header(&self) -> Result<IndexHeader, ParseError> {
        let file = File::open(self.index_path)?;
        let reader = BufReader::new(file);

        let mut buf: Vec<u8> = vec![0u8; HEADER_LINE_SIZE];
        reader.read_exact(&mut buf)?;
        let header = match IndexHeader::try_from(&buf) {
            Ok(h) => h,
            Err(e) => return Err(e)
        };

        Ok(header)
    }

    /// Count how many records has been indexed.
    pub fn indexed_count(&self,) -> Result<u64, ParseError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.input_path)?;
        file.sync_all()?;

        // get and validate file size
        let limit = HEADER_LINE_SIZE as u64;
        let mut size = file.metadata()?.len() - HEADER_LINE_SIZE as u64;
        if size < 0 {
            return Err(ParseError::InvalidSize);
        }

        // validate size and calculate record count
        if size % VALUE_LINE_SIZE as u64 != 0 {
            return Err(ParseError::InvalidSize);
        }
        let record_count = size / VALUE_LINE_SIZE as u64;
        Ok(record_count)
    }

    /// Perform a healthckeck over the index file by reading
    /// the headers and checking the file size.
    /// 
    /// # Arguments
    /// 
    /// * `update_header` - If `true` then it updates index header attibute.
    pub fn healthcheck(&self, update_header: bool) -> Result<IndexStatus, ParseError> {
        let header = self.read_header()?;
        let hash = generate_hash(&self.input_path)?;
        
        // validate headers
        match header.hash {
            Some(saved_hash) => if saved_hash != hash {
                return Ok(IndexStatus::Corrupted);
            },
            None => return Ok(IndexStatus::New)
        }
        if !header.indexed {
            if update_header {
                self.header = header;

            }
            return Ok(IndexStatus::Incomplete);
        }

        // validate file size
        let size = file_size(self.input_path, false)?;
        let total = match self.indexed_count() {
            Ok(v) => v,
            Err(e) => match e {
                ParseError::IO(_) => return Err(e),
                _ => return Ok(IndexStatus::Corrupted)
            }
        };
        if size != HEADER_LINE_SIZE as u64 + (total * VALUE_LINE_SIZE as u64) {
            return Ok(IndexStatus::Corrupted);
        }

        if update_header {
            self.header = header;
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