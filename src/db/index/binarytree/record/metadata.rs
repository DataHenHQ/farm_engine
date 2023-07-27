use std::io::{Read, Write};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use crate::db::field::Record as FieldRecord;
use crate::error::ParseError;
use crate::traits::{ByteSized, WriteAsBytes, WriteTo, LoadFrom};

/// Status flag enumerator.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
pub enum StatusFlag {
    Yes = b'Y' as isize,
    No = b'N' as isize,
    Skip = b'S' as isize,
    None = 0
}

impl StatusFlag {
    /// Return an array with all possible values.
    pub fn as_array() -> [Self; 4] {
        [
            Self::Yes,
            Self::No,
            Self::Skip,
            Self::None
        ]
    }

    /// Returns an array with all possible values as bytes.
    pub fn as_bytes() -> [u8; 4] {
        [
            Self::Yes.into(),
            Self::No.into(),
            Self::Skip.into(),
            Self::None.into()
        ]
    }

    /// Joins an array into a string by using a separator.
    /// 
    /// NOTE: Convert me into Join<Trait> once stable.
    pub fn join<'a>(slice: &[Self], sep: &str) -> String {
        let mut buf: Vec<u8> = Vec::new();
        if slice.len() < 1 {
            return "".to_string();
        }
        let mut iter = slice.iter();
        buf.push(iter.next().unwrap().into());
        for value in slice {
            for char in sep.bytes() {
                buf.push(char);
            }
            buf.push(value.into());
        }
        String::from_utf8_lossy(&buf).to_string()
    }
}

impl std::fmt::Display for StatusFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", match self {
            Self::Yes => "Yes",
            Self::No => "No",
            Self::Skip => "Skip",
            Self::None => ""
        })
    }
}

impl TryFrom<u8> for StatusFlag {
    type Error = ParseError;

    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
        let status_flag = match v {
            b'Y' => Self::Yes,
            b'N' => Self::No,
            b'S' => Self::Skip,
            0 => Self::None,
            _ => return Err(ParseError::InvalidFormat)
        };

        Ok(status_flag)
    }
}

impl From<&StatusFlag> for u8 {
    fn from(v: &StatusFlag) -> Self {
        match v {
            StatusFlag::Yes => b'Y',
            StatusFlag::No => b'N',
            StatusFlag::Skip => b'S',
            StatusFlag::None => 0
        }
    }
}

impl From<StatusFlag> for u8 {
    fn from(v: StatusFlag) -> Self {
        (&v).into()
    }
}

impl ByteSized for StatusFlag {
    const BYTES: usize = 1;
}

impl WriteAsBytes for StatusFlag {
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()> {
        // validate value size
        if buf.len() != Self::BYTES {
            bail!(ParseError::InvalidSize);
        }

        // save value as bytes
        buf[0] = self.into();

        Ok(())
    }
}

impl WriteTo for StatusFlag {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        writer.write_all(&[self.into()])?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Metadata {
    pub status_flag: StatusFlag,
    pub parent: u64,
    pub left_node: u64,
    pub right_node: u64,
    pub height: i64,

    /// Record start location in target
    pub start_pos: u64,

    /// Record end location in target (if apply)
    pub end_pos: u64,

    pub fields: Vec<FieldRecord>
}

impl Metadata {
    /// Table header size in bytes.
    /// 
    /// Byte Format
    /// `<magic_number:11><version:4><record_count:8><name_size:4><name_value:50><uuid:16>`.
    pub const META_BYTES: usize = 82; // TODO: Ale trasteate esto porfa :)

    /// Creates a new record instance.
    pub fn new() -> Self {
        Self{
            status_flag: StatusFlag::None,
            parent: 0,
            left_node: 0,
            right_node: 0,
            height: 0,
            start_pos: 0,
            end_pos: 0,
            fields: Vec::new()
        }
    }

    /// Return the previously calculated byte count to be writed when
    /// converted into bytes.
    pub fn size_as_bytes(&self) -> u64 {
        Self::META_BYTES as u64 + self.fields.size_as_bytes()
    }
}

impl ByteSized for Metadata {
    /// Index header size in bytes.
    /// 
    /// Byte Format
    /// `<magic_number:11><version:4><indexed:1><indexed_count:8><table_nul:1><table_uuid:16>`.
    const BYTES: usize = 30; // TODO: Ale trastea esto porfa :)
}

impl WriteTo for Metadata {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        // write status flag
        self.status_flag.write_to(writer)?;
        // write parent
        self.parent.write_to(writer)?;
        // write left_node
        self.left_node.write_to(writer)?;
        // write right_node
        self.right_node.write_to(writer)?;
        // write height
        self.height.write_to(writer)?;
        // write start_pos
        self.start_pos.write_to(writer)?;
        // write end_pos
        self.end_pos.write_to(writer)?;

        // write fields
        self.fields.write_to(writer)?;

        Ok(())
    }
}

impl LoadFrom for Metadata {
    fn load_from(&mut self, reader: &mut impl Read) -> Result<()> {
        // read data
        let mut carry = 0;
        let mut buf = [0u8; Self::META_BYTES];
        reader.read_exact(&mut buf)?;


        // read status flag
        let status_flag = buf[carry].try_into()?;
        carry += StatusFlag::BYTES;

        // read parent
        let parent = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        // read left_node
        let left_node = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        // read right_node
        let right_node = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        // read height
        let height = i64::from_byte_slice(&buf[carry..carry+i64::BYTES])?;
        carry += i64::BYTES;

        // read start pos
        let start_pos = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        // read end pos
        let end_pos = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        self.fields.load_from(reader)?;
           
        // record index value data
        self.status_flag = status_flag;
        self.parent = parent;
        self.left_node=left_node;
        self.right_node=right_node;
        self.height = height;
        self.start_pos = start_pos;
        self.end_pos = end_pos;

        Ok(())
    }
}

