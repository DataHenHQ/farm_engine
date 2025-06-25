use std::io::Error as IoError;
use thiserror::Error;

/// Binary tree index error.
#[derive(Error, Debug)]
pub enum BTreeIndexError {
    #[error("the node doesn't exist")]
    NoInputFields,
    #[error("the left node doesn't exist")]
    NoLeftNode,
    #[error("the right node doesn't exist")]
    NoRightNode,
    #[error("the node doesn't have data")]
    NoData,
    #[error("IO error: {}", .0)]
    IO(IoError)
}

impl From<IoError> for BTreeIndexError {
    fn from(err: IoError) -> Self {
        Self::IO(err)
    }
}

pub type BTreeIndexResult<T> = std::result::Result<T, BTreeIndexError>;