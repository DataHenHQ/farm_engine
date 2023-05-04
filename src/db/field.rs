pub mod field_type;
pub mod value;

use serde::{Serialize, Deserialize};
use std::io::{Read, Write};
use anyhow::{bail, Result};
use crate::traits::{ByteSized, ReadFrom, WriteTo};
use self::field_type::FieldType;

/// Represents a field.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Field {
    _name: String,
    _value_type: FieldType
}

impl Field {
    /// Name string max allowed length.
    const MAX_NAME_SIZE: usize = 50;

    /// Create a new field.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name. The name string must be <= [MAX_NAME_SIZE] bytes length.
    /// * `value_type` - Value field type.
    pub fn new(name: &str, value_type: FieldType) -> Result<Self> {
        if name.as_bytes().len() > Self::MAX_NAME_SIZE {
            bail!("field name size must be <= {} bytes length", Self::MAX_NAME_SIZE);
        }
        Ok(Self{
            _name: name.to_string(),
            _value_type: value_type
        })
    }

    /// Returns the field name.
    pub fn get_name(&self) -> &str {
        &self._name
    }

    /// Returns the field type.
    pub fn get_type(&self) -> &FieldType {
        &self._value_type
    }
}

impl ByteSized for Field {
    /// Byte representation: `<name_value_size:4><name_value:50><field_type:5>`.
    const BYTES: usize = 59;
}

impl ReadFrom for Field {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        // read field name value size
        let size = u32::read_from(reader)? as usize;
        if size > Self::MAX_NAME_SIZE {
            bail!("field name size must be <= {} bytes length", Self::MAX_NAME_SIZE);
        }

        // read field name
        let mut buf = [0u8; Self::MAX_NAME_SIZE];
        reader.read_exact(&mut buf)?;
        let name_buf = &buf[..size];
        let name = String::from_utf8(name_buf.to_vec())?;

        // read field value type
        let value_type = FieldType::read_from(reader)?;

        // build field and provide read byte count
        let field = Field::new(&name, value_type)?;
        Ok(field)
    }
}

impl WriteTo for Field {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        // convert name into bytes
        let name_bytes = self._name.as_bytes();

        // write name size
        let size = name_bytes.len();
        if size > Self::MAX_NAME_SIZE {
            bail!("field name size must be <= {} bytes length", Self::MAX_NAME_SIZE);
        }
        let size = size as u32;
        size.write_to(writer)?;

        // write name
        let mut buf = [0u8; Self::MAX_NAME_SIZE];
        let mut buf_writer = &mut buf as &mut [u8];
        buf_writer.write_all(name_bytes)?;
        writer.write_all(&buf)?;

        // write field value type
        self._value_type.write_to(writer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_name_size() {
        assert_eq!(50, Field::MAX_NAME_SIZE);
    }

    #[test]
    fn new_field() {
        let expected = Field{
            _name: "foo".to_string(),
            _value_type: FieldType::I16
        };
        match Field::new("foo", FieldType::I16) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn new_field_with_invalid_name() {
        let expected = "field name size must be <= 50 bytes length";
        match Field::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", FieldType::I16) {
            Ok(v) => assert!(false, "expected error but got {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn get_name() {
        let expected = "abc";
        match Field::new("abc", FieldType::F32) {
            Ok(v) => assert_eq!(expected, v.get_name()),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn get_type() {
        let expected = FieldType::F32;
        match Field::new("abc", FieldType::F32) {
            Ok(v) => assert_eq!(expected, *v.get_type()),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn byte_sized() {
        assert_eq!(59, Field::BYTES);
    }

    #[test]
    fn read_from() {
        let expected = Field{
            _name: "abcde".to_string(),
            _value_type: FieldType::I8
        };
        let buf: [u8; Field::BYTES] = [
            // name value size
            0, 0, 0, 5u8,
            // name
            97u8, 98u8, 99u8, 100u8, 101u8, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            // field type
            2u8, 0, 0, 0, 0
        ];
        let mut reader = &buf as &[u8];
        match Field::read_from(&mut reader) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn write_to_with_valid_name() {
        let expected: [u8; Field::BYTES] = [
            // name value size
            0, 0, 0, 7u8,
            // name
            98u8, 97u8, 114u8, 32u8, 102u8, 111u8, 111u8, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
            // field type
            12u8, 0, 0, 0, 23u8
        ];
        let field = Field{
            _name: "bar foo".to_string(),
            _value_type: FieldType::Str(23)
        };
        let mut buf = [0u8; Field::BYTES];
        let mut writer = &mut buf as &mut [u8];
        match field.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn write_to_with_invalid_name() {
        let expected = "field name size must be <= 50 bytes length";
        let field = Field{
            _name: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            _value_type: FieldType::Str(23)
        };
        let mut buf = [0u8; Field::BYTES];
        let mut writer = &mut buf as &mut [u8];
        match field.write_to(&mut writer) {
            Ok(v) => assert!(false, "expected error but got {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }
}