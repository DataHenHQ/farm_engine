use std::fmt::{Display, Formatter, Result as FmtResult};

/// Table healthcheck status.
#[derive(Debug, PartialEq)]
pub enum Status {
    New,
    Good,
    NoFields,
    Corrupted
}

impl Display for Status{
    fn fmt(&self, f: &mut Formatter) -> FmtResult { 
        write!(f, "{}", match self {
            Self::New => "new",
            Self::Good => "good",
            Self::NoFields => "no fields",
            Self::Corrupted => "corrupted"
        })
    }
}