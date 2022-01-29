use thiserror::Error;

/// Parsing error.
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("invalid size")]
    InvalidSize,
    #[error("invalid format")]
    InvalidFormat,
    #[error("invalid byte slice value")]
    InvalidValue,
    #[error("unavailable due status \"{}\"", .0)]
    Unavailable(crate::db::indexer::IndexStatus),
    #[error("retry limit reached")]
    RetryLimit,
    #[error(transparent)]
    IO{
        #[from]
        source: std::io::Error,
    },
    #[error(transparent)]
    CSV{
        #[from]
        source: csv::Error,
    },
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