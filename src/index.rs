pub mod indexer;
pub mod header;
pub mod value;

use std::fmt::{Display, Formatter, Result as FmtResult};
use crate::error::ParseError;
use crate::utils::ByteSized;

/// Position value size.
const POSITION_SIZE: usize = u64::BYTES;

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
