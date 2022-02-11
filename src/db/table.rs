pub mod header;
pub mod record;

use anyhow::{bail, Result};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
use crate::error::ParseError;
use crate::{file_size, generate_hash};
use crate::traits::{ByteSized, LoadFrom, ReadFrom, WriteTo};
use header::Header;
use record::header::{Header as RecordHeader};
use record::value::Value;

/// Table engine version.
pub const VERSION: u32 = 1;

/// Table file extension.
pub const FILE_EXTENSION: &str = "fmtable";

/// Indexer engine.
#[derive(Debug, PartialEq)]
pub struct Table {
    /// Table file path.
    pub path: PathBuf,

    /// Table header.
    pub header: Header,

    // Record header. It contains information about the fields.
    pub record_header: RecordHeader
}

impl Table {
    
}