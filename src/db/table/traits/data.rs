use std::io::{Read, Write, Seek};
use anyhow::Result;
use crate::traits::DataTrait;

/// Represents a table data trait.
pub trait DataWithHealthcheckTrait<T>: Read + Write + Seek + DataTrait<T> where T: Read + Write + Seek {
    /// Validates the data file.
    fn healthcheck_binary(&mut self) -> Result<()>;
}