use std::io::Error as IoError;
use thiserror::Error;

use crate::{db::field::error::FieldError, error::ParseError};
use crate::db::index::raw_match::Status;

/// Index error.
#[derive(Error, Debug)]
pub enum IndexError {
    #[error("the input doesn't have any fields")]
    NoFields,
    #[error("invalid field \"{}\"", .0)]
    InvalidField(FieldError),
    #[error("unavailable due status \"{}\"", .0)]
    Unavailable(Status),
    #[error("{}", .0)]
    NonIndexed(String),
    #[error("invalid file magic bytes")]
    BadMagicBytes,
    #[error("index version mismatch, expected {} but found {}", super::VERSION, .0)]
    BadVersion(u32),
    #[error("{}", .0)]
    InputError(InputError),
    #[error("{}", .0)]
    ParseError(ParseError),
    #[error("IO error: {}", .0)]
    IO(IoError),
    #[error("{}", .0)]
    Other(String)
}

impl From<IoError> for IndexError {
    fn from(err: IoError) -> Self {
        Self::IO(err)
    }
}

impl From<ParseError> for IndexError {
    fn from(err: ParseError) -> Self {
        match err {
            ParseError::IO(e) => Self::IO(e),
            _ => Self::ParseError(err)
        }
    }
}

impl From<InputError> for IndexError {
    fn from(err: InputError) -> Self {
        Self::InputError(err)
    }
}

impl From<FieldError> for IndexError {
    fn from(err: FieldError) -> Self {
        Self::InvalidField(err)
    }
}

pub type IndexResult<T> = std::result::Result<T, IndexError>;

#[derive(Error, Debug)]
pub enum InputError {
    #[error("CSV error: {}", .0)]
    CSV(csv::Error),
    #[error("JSON error: {}", .0)]
    JSON(serde_json::Error),
    #[error("Parse error: {}", .0)]
    ParseError(ParseError),
    #[error("Unsupported input type")]
    Unsupported,
    #[error("IO error: {}", .0)]
    IO(IoError),
    #[error("{}", .0)]
    Other(String)
}

impl From<IoError> for InputError {
    fn from(err: IoError) -> Self {
        Self::IO(err)
    }
}

impl From<ParseError> for InputError {
    fn from(err: ParseError) -> Self {
        match err {
            ParseError::IO(e) => Self::IO(e),
            _ => Self::ParseError(err)
        }
    }
}

impl From<csv::Error> for InputError {
    fn from(err: csv::Error) -> Self {
        Self::CSV(err)
    }
}

impl From<serde_json::Error> for InputError {
    fn from(err: serde_json::Error) -> Self {
        Self::JSON(err)
    }
}

pub type InputResult<T> = std::result::Result<T, InputError>;
