/// Parsing error.
#[derive(Debug)]
pub enum ParseError {
    InvalidSize,
    InvalidFormat,
    InvalidValue,
    Unavailable(super::index::IndexStatus),
    RetryLimit,
    IO(std::io::Error),
    CSV(csv::Error)
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