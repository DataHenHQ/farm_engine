//use thiserror::Error;
//use crate::db::indexer::Status as IndexStatus;
//use crate::db::table::Status as TableStatus;

use std::fmt::Display;

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
    #[error("can't convert bytes to string")]
    ParseString,
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

/// Index error.
#[derive(Error, Debug)]
pub enum IndexError<T> where T: Display {
    #[error("the input doesn't have any fields")]
    NoInputFields,
    #[error("unavailable due status \"{}\"", .0)]
    Unavailable(T)
}

/// Table error.
#[derive(Error, Debug)]
pub enum TableError {
    #[error("the table doesn't have any fields")]
    NoFields,
    #[error("unavailable due status \"{}\"", .0)]
    Unavailable(TableStatus)
}


/// Index error.
#[derive(Error, Debug)]
pub enum DbIndexError {
    #[error("the node doesn't exist")]
    NoInputFields,
    #[error("the left node doesn't exist")]
    NoLeftNode,
    #[error("the right node doesn't exist")]
    NoRightNode,
    #[error("the node doesn't have data")]
    NoData
}

