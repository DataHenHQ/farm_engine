use std::io::Error as IoError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use thiserror::Error;
use crate::db::table::Status as TableStatus;

/// Parsing error.
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("invalid size")]
    InvalidSize,
    #[error("invalid format")]
    InvalidFormat,
    #[error("invalid byte slice value")]
    InvalidValue,
    #[error("retry limit reached")]
    RetryLimit,
    #[error("{}", .0)]
    ParseString(String),
    #[error("IO error: {}", .0)]
    IO(IoError),
    #[error("{}", .0)]
    Other(String)
}

impl From<String> for ParseError {
    fn from(msg: String) -> Self {
        Self::Other(msg)
    }
}

impl From<&str> for ParseError {
    fn from(msg: &str) -> Self {
        Self::Other(msg.to_string())
    }
}

impl From<Utf8Error> for ParseError {
    fn from(err: Utf8Error) -> Self {
        Self::ParseString(err.to_string())
    }
}

impl From<FromUtf8Error> for ParseError {
    fn from(err: FromUtf8Error) -> Self {
        Self::ParseString(err.to_string())
    }
}

impl From<IoError> for ParseError {
    fn from(err: IoError) -> Self {
        Self::IO(err)
    }
}

pub type ParseResult<T> = std::result::Result<T, ParseError>;


/// Table error.
#[derive(Error, Debug)]
pub enum TableError {
    #[error("the table doesn't have any fields")]
    NoFields,
    #[error("unavailable due status \"{}\"", .0)]
    Unavailable(TableStatus),
    #[error("IO error: {}", .0)]
    IO(IoError)
}

impl From<IoError> for TableError {
    fn from(err: IoError) -> Self {
        Self::IO(err)
    }
}

pub type TableResult<T> = std::result::Result<T, TableError>;
