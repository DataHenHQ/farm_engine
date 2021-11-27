use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::convert::TryFrom;
use std::io::{Seek, SeekFrom, Write, BufRead, BufReader, BufWriter};
use crate::engine::error::ParseError;

/// Index header line size.
/// 
/// Format:
/// ```
/// <indexed:1><hash:32>
/// ```
pub const HEADER_LINE_SIZE: usize = 33;

/// Index value line size.
/// 
/// Format:
/// ```
/// <input_valid:1><input_pos:8><output_valid:1><output_pos:8><match:1>
/// ```
pub const VALUE_LINE_SIZE: usize = 19;

/// Unsigned position value size.
const POSITION_U_SIZE: usize = 8;

/// Signed position value size.
const POSITION_SIZE: usize = POSITION_SIZE + 1;

/// index healthcheck status.
#[derive(Debug)]
pub enum IndexStatus {
    New,
    Indexed,
    Incomplete,
    Corrupted
}

/// Match flag enumerator.
#[derive(Debug)]
pub enum MatchFlag {
    Yes = b'Y' as isize,
    No = b'N' as isize,
    Skip = b'S' as isize
}

impl TryFrom<u8> for MatchFlag {
    type Error = ParseError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        let match_flag = match v {
            b'Y' => MatchFlag::Yes,
            b'N' => MatchFlag::No,
            b'S' => MatchFlag::Skip,
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
    pub hash: Option<[u8; blake3::OUT_LEN]>
}

impl IndexHeader {
    fn clone_hash(buf: &[u8]) -> Result<[u8; blake3::OUT_LEN], ParseError> {
        if buf.len() != blake3::OUT_LEN {
            return Err(ParseError::InvalidSize);
        }

        let mut hash = [0u8; blake3::OUT_LEN];
        hash.copy_from_slice(buf);
        Ok(hash)
    }
}

impl TryFrom<&Vec<u8>> for IndexHeader {
    type Error = ParseError;

    fn try_from(buf: &Vec<u8>) -> Result<Self, Self::Error> {
        // validate string size
        if buf.len() != HEADER_LINE_SIZE {
            return Err(ParseError::InvalidSize);
        }

        // extract indexed and hash fields
        let indexed = match buf[0] {
            0 => false,
            1 => true,
            _ => return Err(ParseError::InvalidValue)
        };
        let hash = &buf[1..1+blake3::OUT_LEN];
        let hash = if hash != &[0u8; blake3::OUT_LEN] {
            Some(Self::clone_hash(hash)?)
        } else {
            None
        };

        Ok(Self{
            indexed,
            hash
        })
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
    /// Extract a position value from a byte buffer.
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
                let v = [0u8; POSITION_U_SIZE];
                v.copy_from_slice(&buf[1..]);
                Some(u64::from_be_bytes(v))
            },
            _ => return Err(ParseError::InvalidValue)
        };

        Ok(pos)
    }
}

impl TryFrom<&Vec<u8>> for IndexValue {
    type Error = ParseError;

    fn try_from(buf: &Vec<u8>) -> Result<Self, Self::Error> {
        // validate line size
        if buf.len() != VALUE_LINE_SIZE {
            return Err(ParseError::InvalidSize);
        }

        // validate format and values
        let input_pos = Self::pos_from_bytes(&buf[..POSITION_SIZE])?;
        let output_pos = Self::pos_from_bytes(&buf[POSITION_SIZE..2*POSITION_SIZE])?;
        let match_flag = buf[2*POSITION_SIZE].try_into()?;

        Ok(IndexValue{
            input_pos,
            output_pos,
            match_flag
        })
    }
}