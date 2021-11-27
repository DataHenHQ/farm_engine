use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::convert::TryFrom;
use std::io::{Seek, SeekFrom, Write, BufRead, BufReader, BufWriter};
use crate::engine::error::ParseError;

/// Index header line size.
/// 
/// Format:
/// ```
/// <indexed:1><count_valid:1><indexed_count:8><hash_valid:1><hash:32>
/// ```
pub const HEADER_LINE_SIZE: usize = 43;

/// Index value line size.
/// 
/// Format:
/// ```
/// <input_valid:1><input_pos:8><output_valid:1><output_pos:8><match:1>
/// ```
pub const VALUE_LINE_SIZE: usize = 19;

/// Unsigned position value size.
const POSITION_SIZE: usize = 8;

/// Signed position value size.
const POSITION_U_SIZE: usize = POSITION_SIZE + 1;

// Unsigned hash value size.
const HASH_SIZE: usize = blake3::OUT_LEN;

// Signed hash value size.
const HASH_U_SIZE: usize = HASH_SIZE + 1;

pub trait LoadFrom<T> {
    /// Load values from a source.
    /// 
    /// # Arguments
    /// 
    /// * `source` - Source to load values from.
    fn load_from(&mut self, source: T) -> Result<(), ParseError>;
}

/// Match flag enumerator.
#[derive(Debug)]
pub enum MatchFlag {
    Yes = b'Y' as isize,
    No = b'N' as isize,
    Skip = b'S' as isize,
    None = 0
}

impl TryFrom<u8> for MatchFlag {
    type Error = ParseError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        let match_flag = match v {
            b'Y' => MatchFlag::Yes,
            b'N' => MatchFlag::No,
            b'S' => MatchFlag::Skip,
            0 => MatchFlag::None,
            _ => return Err(ParseError::InvalidFormat)
        };

        Ok(match_flag)
    }
}

/// Describes an Indexer file header.
#[derive(Debug)]
pub struct IndexHeader {
    /// `true` when the input file has been indexed successfully.
    pub indexed: bool,

    // Input file hash
    pub hash: Option<[u8; HASH_SIZE]>,

    // Indexed records count.
    pub indexed_count: Option<u64>
}

impl IndexHeader {
    pub fn new() -> Self {
        Self{
            indexed: false,
            hash: None,
            indexed_count: None
        }
    }

    /// Clone input file hash value.
    /// 
    /// # Arguments
    /// 
    /// * `buf` - Bytes to clone hash from.
    pub fn clone_hash(buf: &[u8]) -> Result<[u8; HASH_SIZE], ParseError> {
        if buf.len() != HASH_SIZE {
            return Err(ParseError::InvalidSize);
        }

        let mut hash = [0u8; HASH_SIZE];
        hash.copy_from_slice(buf);
        Ok(hash)
    }
}

impl LoadFrom<&[u8]> for IndexHeader {
    fn load_from(&mut self, buf: &[u8]) -> Result<(), ParseError> {
        // validate string size
        if buf.len() != HEADER_LINE_SIZE {
            return Err(ParseError::InvalidSize);
        }

        // extract indexed
        let indexed = match buf[0] {
            0 => false,
            1 => true,
            _ => return Err(ParseError::InvalidValue)
        };

        // extract indexed record count
        let indexed_count = pos_from_bytes(&buf[1..1+POSITION_U_SIZE])?;

        // extract hash
        let hash = if buf[1+POSITION_U_SIZE] > 0 {
            Some(Self::clone_hash(&buf[2+POSITION_U_SIZE..2+POSITION_U_SIZE+HASH_SIZE])?)
        } else {
            None
        };

        self.indexed = indexed;
        self.hash = hash;
        self.indexed_count = indexed_count;

        Ok(())
    }
}

impl TryFrom<&[u8]> for IndexHeader {
    type Error = ParseError;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        let header = Self::new();
        header.load_from(buf)?;
        Ok(header)
    }
}

impl From<&IndexHeader> for [u8; HEADER_LINE_SIZE] {
    fn from(header: &IndexHeader) -> [u8; HEADER_LINE_SIZE] {
        let mut buf = [0u8; HEADER_LINE_SIZE];

        buf[0] = header.indexed as u8;
        pos_into_bytes(header.indexed_count, &mut buf[1..1+POSITION_U_SIZE]);

        // copy hash as bytes
        if let Some(hash) = header.hash {
            buf[1+POSITION_U_SIZE] = 1u8;
            let mut hash_buf = &buf[2+POSITION_U_SIZE..2+POSITION_U_SIZE+HASH_SIZE];
            hash_buf.copy_from_slice(&hash);
        }
        
        buf
    }
}

/// Describes an Indexer file value.
#[derive(Debug)]
pub struct IndexValue {
    /// Input file position for the record.
    pub input_pos: Option<u64>,

    /// Output file position for the record.
    pub output_pos: Option<u64>,

    /// Match flag for the record (Y,N,S).
    pub match_flag: MatchFlag
}

impl IndexValue {
    pub fn new() -> Self {
        Self{
            input_pos: None,
            output_pos: None,
            match_flag: MatchFlag::None
        }
    }
}

impl LoadFrom<&[u8]> for IndexValue {
    fn load_from(&mut self, buf: &[u8]) -> Result<(), ParseError> {
        // validate line size
        if buf.len() != VALUE_LINE_SIZE {
            return Err(ParseError::InvalidSize);
        }

        // validate format and values
        let input_pos = pos_from_bytes(&buf[..POSITION_SIZE])?;
        let output_pos = pos_from_bytes(&buf[POSITION_SIZE..2*POSITION_SIZE])?;
        let match_flag = buf[2*POSITION_SIZE].try_into()?;

        self.input_pos = input_pos;
        self.output_pos = output_pos;
        self.match_flag = match_flag;

        Ok(())
    }
}

impl TryFrom<&[u8]> for IndexValue {
    type Error = ParseError;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        let mut value = Self::new();
        value.load_from(buf)?;
        Ok(value)
    }
}

impl From<&IndexValue> for [u8; VALUE_LINE_SIZE] {
    fn from(value: &IndexValue) -> [u8; VALUE_LINE_SIZE] {
        let mut buf = [0u8; VALUE_LINE_SIZE];

        // convert value attributes into bytes and save it on buf
        pos_into_bytes(value.input_pos, &mut buf[..POSITION_U_SIZE]);
        pos_into_bytes(value.output_pos, &mut buf[POSITION_U_SIZE..2*POSITION_U_SIZE]);
        buf[2*POSITION_U_SIZE+1] = value.match_flag as u8;
        
        buf
    }
}

/// Extract a valid position value from a byte buffer.
/// 
/// # Arguments
/// 
/// * `buf` - Byte buffer.
fn pos_from_bytes(buf: &[u8]) -> Result<Option<u64>, ParseError> {
    // validate value size
    if buf.len() != POSITION_U_SIZE {
        return Err(ParseError::InvalidSize);
    }

    // extract pos from bytes buffer
    let pos = match buf[0] {
        0 => None,
        1 => {
            let v = [0u8; POSITION_SIZE];
            v.copy_from_slice(&buf[1..]);
            Some(u64::from_be_bytes(v))
        },
        _ => return Err(ParseError::InvalidValue)
    };

    Ok(pos)
}

/// Extract a valid position value from a byte buffer.
/// 
/// # Arguments
/// 
/// * `buf` - Byte buffer.
fn pos_into_bytes(pos: Option<u64>, buf: &mut [u8]) -> Result<(), ParseError> {
    // validate value size
    if buf.len() != POSITION_U_SIZE {
        return Err(ParseError::InvalidSize);
    }

    // save position into bytes
    match pos {
        Some(v) => {
            buf[0] = 1u8;
            let pos_bytes = v.to_be_bytes();
            for i in 0..POSITION_SIZE {
                buf[1+i] = pos_bytes[i];
            }
        },
        None => for i in 0..POSITION_U_SIZE {
            buf[i] = 0u8
        }
    }

    Ok(())
}