pub mod indexer;
pub mod index_header;
pub mod index_value;

use std::fmt::{Display, Formatter, Result as FmtResult};
use crate::engine::parse_error::ParseError;

/// Position value size.
const POSITION_SIZE: usize = 8;

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

pub trait LoadFrom<T> {
    /// Load values from a source.
    /// 
    /// # Arguments
    /// 
    /// * `source` - Source to load values from.
    fn load_from(&mut self, source: T) -> Result<(), ParseError>;
}

/// Extract a valid position value from a byte buffer.
/// 
/// # Arguments
/// 
/// * `buf` - Byte buffer.
fn pos_from_bytes(buf: &[u8]) -> Result<u64, ParseError> {
    // validate value size
    if buf.len() != POSITION_SIZE {
        return Err(ParseError::InvalidSize);
    }

    let mut pos_bytes = [0u8; POSITION_SIZE];
    pos_bytes.copy_from_slice(buf);
    Ok(u64::from_be_bytes(pos_bytes))
}

/// Extract a valid position value from a byte buffer.
/// 
/// # Arguments
/// 
/// * `buf` - Byte buffer.
fn pos_into_bytes(pos: u64, buf: &mut [u8]) -> Result<(), ParseError> {
    // validate value size
    if buf.len() != POSITION_SIZE {
        return Err(ParseError::InvalidSize);
    }

    // save position into bytes
    buf.copy_from_slice(&pos.to_be_bytes());

    Ok(())
}
