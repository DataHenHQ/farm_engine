use std::fmt::{Display, Formatter, Result as FmtResult};

/// Parsing error.
#[derive(Debug)]
pub enum ParseError {
    InvalidSize,
    InvalidFormat,
    InvalidValue,
    Unavailable(super::index::IndexStatus),
    RetryLimit,
    IO(std::io::Error),
    CSV(csv::Error),
    Other(String)
}

impl From<std::io::Error> for ParseError {
    fn from(e: std::io::Error) -> Self {
        Self::IO(e)
    }
}

impl From<csv::Error> for ParseError {
    fn from(e: csv::Error) -> Self {
        Self::CSV(e)
    }
}

impl From<String> for ParseError {
    fn from(e: String) -> Self {
        Self::Other(e)
    }
}

impl From<&str> for ParseError {
    fn from(e: &str) -> Self {
        Self::Other(e.to_string())
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult { 
        write!(f, "{}", match self {
            Self::InvalidSize => "invalid size".to_string(),
            Self::InvalidFormat => "invalid format".to_string(),
            Self::InvalidValue => "invalid value".to_string(),
            Self::Unavailable(s) => format!("unavailable due status \"{}\"", s),
            Self::RetryLimit => "retry limit reached".to_string(),
            Self::IO(e) => e.to_string(),
            Self::CSV(e) => e.to_string(),
            Self::Other(e) => e.to_string()
        })
    }
}