use std::io::{Read, Write};
use std::convert::TryFrom;
use anyhow::{bail, Result};
use crate::traits::{ByteSized, FromByteSlice, WriteAsBytes, ReadFrom, WriteTo, LoadFrom};
use super::VERSION;
use super::record::header::FieldType;
use super::record::value::Value;

/// File's magic numbervalue size bytes.
pub const MAGIC_NUMBER_SIZE: usize = 11;

/// File's magic number value `datahen_tbl` as bytes.
pub const MAGIC_NUMBER_BYTES: [u8; MAGIC_NUMBER_SIZE] = [100, 97, 116, 97, 104, 101, 110, 95, 116, 98, 108];

/// Table name max length in bytes.
pub const TABLE_NAME_MAX_SIZE: u32 = 50;

/// Table name field.
pub const TABLE_NAME_FIELD: FieldType = FieldType::Str(TABLE_NAME_MAX_SIZE);

//// Describes an Indexer file header.
#[derive(Debug, PartialEq)]
pub struct Header {
    /// Records count.
    pub record_count: u64,

    /// Table name.
    _name: String
}

impl Header {
    /// Creates a new header.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Table name.
    pub fn new(name: &str) -> Result<Self> {
        if !TABLE_NAME_FIELD.is_valid(&Value::Str(name.to_string())) {
            bail!("table name must be shorter than {} bytes", TABLE_NAME_MAX_SIZE);
        }
        Ok(Self{
            record_count: 0,
            _name: name.to_string()
        })
    }

    /// Gets the table name.
    pub fn get_name(&self) -> &str {
        &self._name
    }

    /// Serialize the instance to a fixed byte slice.
    pub fn as_bytes(&self) -> [u8; Self::BYTES] {
        let mut buf = [0u8; Self::BYTES];
        let mut carry = 0;

        // save magic number
        let magic_buf = &mut buf[carry..carry+MAGIC_NUMBER_SIZE];
        magic_buf.copy_from_slice(&MAGIC_NUMBER_BYTES);
        carry += MAGIC_NUMBER_SIZE;

        // save version
        VERSION.write_as_bytes(&mut buf[carry..carry+u32::BYTES]).unwrap();
        carry += u32::BYTES;

        // save record count
        self.record_count.write_as_bytes(&mut buf[carry..carry+u64::BYTES]).unwrap();
        carry += u64::BYTES;

        // save table name
        let name_value = Value::Str(self._name);
        let name_writer = &mut buf[carry..carry+TABLE_NAME_FIELD.value_byte_size()] as &mut [u8];
        TABLE_NAME_FIELD.write_value(&mut name_writer, &name_value).unwrap();

        buf
    }
}

impl ByteSized for Header {
    /// Table header size in bytes.
    /// 
    /// Byte Format
    /// `<magic_number:11><version:4><record_count:8><name_size:4><name_value:50>`.
    const BYTES: usize = 66 + MAGIC_NUMBER_SIZE;
}

impl LoadFrom for Header {
    fn load_from(&mut self, reader: &mut impl Read) -> Result<()> {
        // read data
        let mut carry = 0;
        let mut buf = [0u8; Self::BYTES];
        reader.read_exact(&mut buf)?;

        // read and validate magic number
        if buf[carry..carry+MAGIC_NUMBER_SIZE] != MAGIC_NUMBER_BYTES {
            bail!("invalid file magic number");
        }
        carry += MAGIC_NUMBER_SIZE;

        // read and validate table version
        let version = u32::from_byte_slice(&buf[carry..carry+u32::BYTES])?;
        if version != VERSION {
            bail!("table version mismatch, expected {} buf found {}", VERSION, version);
        }
        carry += u32::BYTES;

        // read record count
        let record_count = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        // read table name
        let mut name_reader = &buf[carry..carry+TABLE_NAME_FIELD.value_byte_size()] as &[u8];
        let name_value = TABLE_NAME_FIELD.read_value(&mut name_reader)?;

        // save values
        self.record_count = record_count;
        self._name = match name_value {
            Value::Str(s) => s,
            _ => bail!("name value should be a string")
        };

        Ok(())
    }
}

impl FromByteSlice for Header {
    fn from_byte_slice(buf: &[u8]) -> Result<Self> {
        let mut header = Self::new("")?;
        let mut reader = buf;
        header.load_from(&mut reader)?;
        Ok(header)
    }
}

impl ReadFrom for Header {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut header = Self::new("")?;
        header.load_from(reader)?;
        Ok(header)
    }
}

impl TryFrom<&[u8]> for Header {
    type Error = anyhow::Error;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        let mut header = Self::new("")?;
        let mut reader = buf;
        header.load_from(&mut reader)?;
        Ok(header)
    }
}

impl WriteTo for Header {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        writer.write_all(&self.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;


    /// Builds an table header as byte slice from the values provided.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Table name.
    /// * `record_count` - Total record count.
    pub fn build_header_bytes(name: &str, record_count: u64) -> [u8; Header::BYTES] {
        Header{
            record_count,
            _name: name.to_string()
        }.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helper::*;

    #[test]
    fn table_name_max_size() {
        assert_eq!(50, TABLE_NAME_MAX_SIZE);
    }

    #[test]
    fn table_name_field() {
        let expected = 50;
        let field_type = TABLE_NAME_FIELD;
        match field_type {
            FieldType::Str(size) => assert_eq!(expected, size),
            t => assert!(false, "expected FieldType::Str({}) but got FieldType::{:?}", expected, t)
        }
    }

    #[test]
    fn new() {
        let expected = Header{
            record_count: 0,
            _name: "hello".to_string()
        };
        match Header::new("hello") {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn new_with_invalid_name() {
        let expected = "table name must be shorter than 50 bytes";
        let invalid_name = String::from_utf8_lossy(vec![b'a'; 51]);
        match Header::new(&invalid_name) {
            Ok(v) => assert!(false, "expected error but got {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn as_bytes() {
        // first test
        let mut expected: [u8; Header::BYTES] = [
            // magic number
            100, 97, 116, 97, 104, 101, 110, 95, 105, 100, 120,
            // version
            0, 0, 0, 2,
            // record count = 2311457452320998632
            32, 19, 242, 78, 103, 5, 196, 232,
            // name size
            0, 0, 0, 8,
            // name value: "my_table"
            109, 121, 95, 116, 97, 98, 108, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0
        ];

        // test header as_bytes function
        let header = Header{
            record_count: 2311457452320998632,
            _name: "my_table".to_string()
        };
        assert_eq!(expected, header.as_bytes());

        // second test
        let expected: [u8; Header::BYTES] = [
            // magic number
            100, 97, 116, 97, 104, 101, 110, 95, 105, 100, 120,
            // version
            0, 0, 0, 2,
            // record count = 4525325654675485867
            62, 205, 47, 180, 235, 228, 244, 171,
            // name size
            0, 0, 0, 9,
            // name value: "hellotbl"
            104, 101, 108, 108, 111, 95, 116, 98, 108, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0
        ];

        // test header as_bytes function
        let header = Header{
            record_count: 4525325654675485867,
            _name: "hello_tbl".to_string()
        };
        assert_eq!(expected, header.as_bytes());
    }

    #[test]
    fn byte_sized() {
        assert_eq!(77, Header::BYTES);
    }

    #[test]
    fn load_from_u8_slice() {
        // first random try
        let mut header = Header{
            record_count: 0,
            _name: "".to_string()
        };
        let expected = Header{
            record_count: 4535435,
            _name: "my_table".to_string()
        };
        let buf = build_header_bytes("my_table", 4535435);
        let mut reader = &buf as &[u8];
        if let Err(e) = header.load_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        };
        assert_eq!(expected, header);

        // second random try
        let mut header = Header{
            record_count: 0,
            _name: "".to_string()
        };
        let expected = Header{
            record_count: 6572646535124,
            _name: "hello_tbl".to_string()
        };
        let buf = build_header_bytes("hello_tbl", 6572646535124);
        let mut reader = &buf as &[u8];
        if let Err(e) = header.load_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        };
        assert_eq!(expected, header);
    }

    #[test]
    fn from_byte_slice() {
        // first random try
        let expected = Header{
            record_count: 2341234,
            _name: "my_table".to_string()
        };
        let buf = build_header_bytes("my_table", 2341234);
        let value = match Header::from_byte_slice(&buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected, value);

        // second random try
        let expected = Header{
            record_count: 9879873495743,
            _name: "hello_tbl".to_string()
        };
        let buf = build_header_bytes("hello_tbl", 9879873495743);
        let value = match Header::from_byte_slice(&buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected, value);
    }

    #[test]
    fn read_from_reader() {
        // first random try
        let expected = Header{
            record_count: 974734838473874,
            _name: "my_table".to_string()
        };
        let buf = build_header_bytes("my_table", 974734838473874);
        let mut reader = &buf as &[u8];
        let value = match Header::read_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected, value);

        // second random try
        let expected = Header{
            record_count: 3434232315645344,
            _name: "hello_tbl".to_string()
        };
        let buf = build_header_bytes("hello_tbl", 3434232315645344);
        let mut reader = &buf as &[u8];
        let value = match Header::read_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected, value);
    }

    #[test]
    fn try_from_u8_slice() {
        // first random try
        let expected = Header{
            record_count: 32412342134234,
            _name: "my_table".to_string()
        };
        let buf = build_header_bytes("my_table", 32412342134234);
        let value = match Header::try_from(&buf[..]) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected, value);

        // second random try
        let expected = Header{
            record_count: 56535423143214,
            _name: "hello_tbl".to_string()
        };
        let buf = build_header_bytes("hello_tbl", 56535423143214);
        let value = match Header::try_from(&buf[..]) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected, value);
    }

    #[test]
    fn write_to_writer() {
        // first random try
        let expected = build_header_bytes("my_table", 788477630402843);
        let header = Header{
            record_count: 788477630402843,
            _name: "my_table".to_string()
        };
        let mut buf = [0u8; Header::BYTES];
        let mut writer = &mut buf as &mut [u8];
        if let Err(e) = header.write_to(&mut writer) {
            assert!(false, "{:?}", e);
            return;
        };
        assert_eq!(expected, buf);

        // second random try
        let expected = build_header_bytes("hello_tbl", 63439320337562938);
        let header = Header{
            record_count: 63439320337562938,
            _name: "hello_tbl".to_string()
        };
        let mut buf = [0u8; Header::BYTES];
        let mut writer = &mut buf as &mut [u8];
        if let Err(e) = header.write_to(&mut writer) {
            assert!(false, "{:?}", e);
            return;
        };
        assert_eq!(expected, buf);
    }
}