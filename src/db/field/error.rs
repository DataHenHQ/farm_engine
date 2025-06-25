use std::io::Error as IoError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use thiserror::Error;
use crate::error::ParseError;

#[derive(Error, Debug)]
pub enum FieldError {
    #[error("{}", .0)]
    InvalidFieldType(String),
    #[error("the field doesn't exist")]
    NoField,
    #[error("{}", .0)]
    ParseError(ParseError),
    #[error("IO error: {}", .0)]
    IO(IoError)
}

impl From<ParseError> for FieldError {
    fn from(err: ParseError) -> Self {
        match err {
            ParseError::IO(err) => Self::IO(err),
            _ => Self::ParseError(err)
        }
    }
}

impl From<IoError> for FieldError {
    fn from(err: IoError) -> Self {
        Self::IO(err)
    }
}

pub type FieldResult<T> = std::result::Result<T, FieldError>;


#[derive(Error, Debug)]
pub enum FieldValueError {
    #[error("{}", .0)]
    InvalidType(String),
    #[error("{}", .0)]
    InvalidSize(String),
    #[error("{}", .0)]
    ParseError(ParseError),
    #[error("IO error: {}", .0)]
    IO(IoError)
}

impl From<ParseError> for FieldValueError {
    fn from(err: ParseError) -> Self {
        match err {
            ParseError::IO(err) => Self::IO(err),
            _ => Self::ParseError(err)
        }
    }
}

impl From<IoError> for FieldValueError {
    fn from(err: IoError) -> Self {
        Self::IO(err)
    }
}

impl From<Utf8Error> for FieldValueError {
    fn from(err: Utf8Error) -> Self {
        Self::ParseError(err.into())
    }
}

impl From<FromUtf8Error> for FieldValueError {
    fn from(err: FromUtf8Error) -> Self {
        Self::ParseError(err.into())
    }
}

pub type FieldValueResult<T> = std::result::Result<T, FieldValueError>;


#[derive(Error, Debug)]
pub enum FieldKeyError {
    #[error("{}", .0)]
    NotFound(String),
    #[error("{}", .0)]
    AlreadyExists(String),
    #[error("field name size must be <= {} bytes length", .0)]
    InvalidNameSize(usize),
    #[error("{}", .0)]
    ParseError(ParseError),
    #[error("IO error: {}", .0)]
    IO(IoError)
}

impl From<ParseError> for FieldKeyError {
    fn from(err: ParseError) -> Self {
        match err {
            ParseError::IO(err) => Self::IO(err),
            _ => Self::ParseError(err)
        }
    }
}

impl From<IoError> for FieldKeyError {
    fn from(err: IoError) -> Self {
        Self::IO(err)
    }
}

impl From<Utf8Error> for FieldKeyError {
    fn from(err: Utf8Error) -> Self {
        Self::ParseError(err.into())
    }
}

impl From<FromUtf8Error> for FieldKeyError {
    fn from(err: FromUtf8Error) -> Self {
        Self::ParseError(err.into())
    }
}

pub type FieldKeyResult<T> = std::result::Result<T, FieldKeyError>;

#[derive(Error, Debug)]
pub enum RecordError {
    #[error("field count mismatch the record value count")]
    CountMismatch,
    #[error("{}", .0)]
    SaveError(String),
    #[error("{}", .0)]
    ValueError(FieldValueError),
    #[error("{}", .0)]
    KeyError(FieldKeyError),
    #[error("{}", .0)]
    ParseError(ParseError),
    #[error("IO error: {}", .0)]
    IO(IoError),
    #[error("{}", .0)]
    Other(String)
}

impl From<FieldValueError> for RecordError {
    fn from(err: FieldValueError) -> Self {
        match err {
            FieldValueError::IO(err) => Self::IO(err),
            _ => Self::ValueError(err)
        }
    }
}

impl From<FieldKeyError> for RecordError {
    fn from(err: FieldKeyError) -> Self {
        match err {
            FieldKeyError::IO(err) => Self::IO(err),
            _ => Self::KeyError(err)
        }
    }
}

impl From<ParseError> for RecordError {
    fn from(err: ParseError) -> Self {
        match err {
            ParseError::IO(err) => Self::IO(err),
            _ => Self::ParseError(err)
        }
    }
}

impl From<IoError> for RecordError {
    fn from(err: IoError) -> Self {
        Self::IO(err)
    }
}

impl From<Utf8Error> for RecordError {
    fn from(err: Utf8Error) -> Self {
        Self::ParseError(err.into())
    }
}

impl From<FromUtf8Error> for RecordError {
    fn from(err: FromUtf8Error) -> Self {
        Self::ParseError(err.into())
    }
}

pub type RecordResult<T> = std::result::Result<T, RecordError>;
