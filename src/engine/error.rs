/// Parsing error.
#[derive(Debug)]
pub enum ParseError {
    InvalidSize,
    InvalidFormat,
    InvalidValue,
    Unavailable(super::index::IndexStatus),
    IO(std::io::Error)
}

impl From<std::io::Error> for ParseError {
    fn from(e: std::io::Error) -> Self {
        Self::IO(e)
    }
}