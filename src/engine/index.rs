use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::str::FromStr;
use std::convert::TryFrom;
use std::io::{Seek, SeekFrom, Write, BufRead, BufReader, BufWriter};

/// Index line size.
const INDEX_LINE_SIZE: usize = 41;

/// Empty index line formats:
/// 
/// ```
/// <input_pos:20><output_pos:20><match:1>
/// <indexed:1><output_file_size:20><null:20>
/// ```
const EMPTY_INDEX_LINE: [u8; INDEX_LINE_SIZE] = [0u8; INDEX_LINE_SIZE];


/// Match flag enumerator.
#[derive(Debug, Serialize, Deserialize)]
pub enum MatchFlag {
    Yes = b'Y' as isize,
    No = b'N' as isize,
    Skip = b'S' as isize
}

impl TryFrom<u8> for MatchFlag {
    type Error = super::ParseError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        let match_flag = match v {
            b'Y' => MatchFlag::Yes,
            b'N' => MatchFlag::No,
            b'S' => MatchFlag::Skip,
            _ => return Err(super::ParseError::InvalidFormat)
        };

        Ok(match_flag)
    }
}

/// Describes an Indexer file header.
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexHeader {
    /// `true` when the input file has been indexed successfully.
    pub indexed: bool,
    pub output_file_size: Option<u64>
}

impl FromStr for IndexHeader {
    type Err = super::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // validate string size
        if s.len() != INDEX_LINE_SIZE {
            return Err(super::ParseError::InvalidSize);
        }

        // validate format and value
        let indexed = match s.as_bytes()[0] {
            b'0' => false,
            b'1' => true,
            b' ' => false,
            _ => return Err(super::ParseError::InvalidFormat)
        };
        let output_file_size = super::position_from_str(&s[1..21])?;

        Ok(Self{
            indexed,
            output_file_size
        })
    }
}

/// Describes an Indexer file value.
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexerValue {
    /// Input file position for the record.
    pub input_pos: Option<u64>,
    /// Output file position for the record.
    pub output_pos: Option<u64>,
    /// Match flag for the record (Y,N,S).
    pub match_flag: MatchFlag
}

impl FromStr for IndexerValue {
    type Err = super::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // validate string size
        if s.len() != INDEX_LINE_SIZE {
            return Err(super::ParseError::InvalidSize);
        }

        // validate format and values
        let input_pos = super::position_from_str(&s[..20])?;
        let output_pos = super::position_from_str(&s[20..40])?;
        let match_flag = s.as_bytes()[40].try_into()?;

        Ok(IndexerValue{
            input_pos,
            output_pos,
            match_flag
        })
    }
}

/// Analyze an input file to track new lines and record it's positions into an index file.
/// Returns total line count.
/// 
/// # Arguments
/// 
/// * `input_path` - File path to analize.
/// * `index_path` - File path to write the index.
pub fn analize(input_path: &str, index_path: &str) -> std::io::Result<u64> {
    let file = File::open(input_path)?;
    let index_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(index_path)?;
    
    // config reader and writer buffers
    let mut rdr = BufReader::new(file);
    let mut wrt = BufWriter::new(index_file);

    // generate result file
    wrt.write_all(&EMPTY_INDEX_LINE)?;
    let next_pos = 0;
    for line in rdr.lines() {
        
    }

    unimplemented!();
}

/// Fill a file with zero byte until the target size or ignore if
/// bigger. Return true if file is bigger.
/// 
/// # Arguments
/// 
/// * `path` - File path to fill.
/// * `target_size` - Target file size in bytes.
pub fn fill_file(path: &str, target_size: u64) -> std::io::Result<bool> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)?;

    file.sync_all()?;
    let mut size = file.metadata()?.len();

    // validate file current size vs target size
    if target_size < size {
        // file is bigger, return true
        return Ok(true);
    }
    if target_size == size {
        return Ok(false);
    }

    // fill file with zeros until target size is match
    let buf_size = 4096u64;
    let buf = [0u8; 4096];
    let mut wrt = BufWriter::new(file);
    while size + buf_size < target_size {
        wrt.write_all(&buf)?;
        size += buf_size;
    }
    let remaining = (target_size - size) as usize;
    if remaining > 0 {
        wrt.write_all(&buf[..remaining])?;
    }

    Ok(false)
}