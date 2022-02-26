use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use anyhow::{bail, Result};
use crate::error::ParseError;
use crate::traits::{ByteSized, FromByteSlice, WriteAsBytes, ReadFrom, WriteTo, LoadFrom};
use super::value::Value;
use super::Record;

/// Represents a field type.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
pub enum FieldType {
    /// Represents a bool type being `type_byte = 1`.
    Bool,
    /// Represents an i8 type being `type_byte = 2`.
    I8,
    /// Represents an i16 type being `type_byte = 3`.
    I16,
    /// Represents an i32 type being `type_byte = 4`.
    I32,
    /// Represents an i64 type being `type_byte = 5`.
    I64,
    /// Represents an u8 type being `type_byte = 6`.
    U8,
    /// Represents an u16 type being `type_byte = 7`.
    U16,
    /// Represents an u32 type being `type_byte = 8`.
    U32,
    /// Represents an u64 type being `type_byte = 9`.
    U64,
    /// Represents a f32 type being `type_byte = 10`.
    F32,
    /// Represents a f64 type being `type_byte = 11`.
    F64,
    /// Represents a string type being `type_byte = 12`.
    Str(u32)
}

impl FieldType {
    /// Min value the field type first byte can take.
    pub const MIN_TYPE_ID: u8 = 1u8;

    /// Max value the field type first byte can take.
    pub const MAX_TYPE_ID: u8 = 12u8;

    /// Gets the byte size of the value described by the field type.
    pub fn value_byte_size(&self) -> usize {
        match self {
            Self::Bool => u8::BYTES,
            Self::I8 => i8::BYTES,
            Self::I16 => i16::BYTES,
            Self::I32 => i32::BYTES,
            Self::I64 => i64::BYTES,
            Self::U8 => u8::BYTES,
            Self::U16 => u16::BYTES,
            Self::U32 => u32::BYTES,
            Self::U64 => u64::BYTES,
            Self::F32 => f32::BYTES,
            Self::F64 => f64::BYTES,
            Self::Str(size) => u32::BYTES + *size as usize
        }
    }

    /// Gets the string max size when [Self::Str].
    pub fn str_size(&self) -> Result<u32> {
        match self {
            Self::Str(size) => Ok(*size),
            _ => bail!("field type is not a string type")
        }
    }

    /// Validate a value against a field type.
    /// 
    /// # Arguments
    /// 
    /// * `value` - Value to validate.
    pub fn is_valid(&self, value: &Value) -> bool {
        // any default value is valid
        if let Value::Default = value {
            return true;
        }

        // validate field type vs value type
        match self {
            FieldType::Bool => if let Value::Bool(_) = value {
                return true;
            },
            FieldType::I8 => if let Value::I8(_) = value {
                return true;
            },
            FieldType::I16 => if let Value::I16(_) = value {
                return true;
            },
            FieldType::I32 => if let Value::I32(_) = value {
                return true;
            },
            FieldType::I64 => if let Value::I64(_) = value {
                return true;
            },
            FieldType::U8 => if let Value::U8(_) = value {
                return true;
            },
            FieldType::U16 => if let Value::U16(_) = value {
                return true;
            },
            FieldType::U32 => if let Value::U32(_) = value {
                return true;
            },
            FieldType::U64 => if let Value::U64(_) = value {
                return true;
            },
            FieldType::F32 => if let Value::F32(_) = value {
                return true;
            },
            FieldType::F64 => if let Value::F64(_) = value {
                return true;
            },
            FieldType::Str(size) => if let Value::Str(s) = value {
                if s.as_bytes().len() > (*size) as usize {
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    /// Reads a value from a reader based on the field type.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    pub fn read_value(&self, reader: &mut impl Read) -> Result<Value> {
        let value: Value = match self {
            Self::Bool => bool::read_from(reader)?.into(),
            Self::I8 => i8::read_from(reader)?.into(),
            Self::I16 => i16::read_from(reader)?.into(),
            Self::I32 => i32::read_from(reader)?.into(),
            Self::I64 => i64::read_from(reader)?.into(),
            Self::U8 => u8::read_from(reader)?.into(),
            Self::U16 => u16::read_from(reader)?.into(),
            Self::U32 => u32::read_from(reader)?.into(),
            Self::U64 => u64::read_from(reader)?.into(),
            Self::F32 => f32::read_from(reader)?.into(),
            Self::F64 => f64::read_from(reader)?.into(),
            Self::Str(size) => {
                let size = (*size) as usize;

                // read the real string size
                let value_size = u32::read_from(reader)? as usize;
                if value_size > size {
                    bail!("string value size can't be bigger than the field size");
                }

                // read the string value
                if size > 0 {
                    let mut buf = vec![0u8; size as usize];
                    reader.read_exact(&mut buf)?;
                    Value::Str(String::from_utf8(buf[..value_size].to_vec())?)
                } else {
                    Value::Str("".to_string())
                }
            }
        };
        Ok(value)
    }

    /// Write a value into a writer based on the field type.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    pub fn write_value(&self, writer: &mut impl Write, value: &Value) -> Result<()> {
        match self {
            Self::Bool => match value {
                Value::Bool(v) => (*v).write_to(writer)?,
                Value::Default => false.write_to(writer)?,
                _ => bail!("value must be a Value::Bool")
            },
            Self::I8 => match value {
                Value::I8(v) => v.write_to(writer)?,
                Value::Default => 0i8.write_to(writer)?,
                _ => bail!("value must be a Value::I8")
            },
            Self::I16 => match value {
                Value::I16(v) => v.write_to(writer)?,
                Value::Default => 0i16.write_to(writer)?,
                _ => bail!("value must be a Value::I16")
            },
            Self::I32 => match value {
                Value::I32(v) => v.write_to(writer)?,
                Value::Default => 0i32.write_to(writer)?,
                _ => bail!("value must be a Value::I32")
            },
            Self::I64 => match value {
                Value::I64(v) => v.write_to(writer)?,
                Value::Default => 0i64.write_to(writer)?,
                _ => bail!("value must be a Value::I64")
            },
            Self::U8 => match value {
                Value::U8(v) => v.write_to(writer)?,
                Value::Default => 0u8.write_to(writer)?,
                _ => bail!("value must be a Value::U8")
            },
            Self::U16 => match value {
                Value::U16(v) => v.write_to(writer)?,
                Value::Default => 0u16.write_to(writer)?,
                _ => bail!("value must be a Value::U16")
            },
            Self::U32 => match value {
                Value::U32(v) => v.write_to(writer)?,
                Value::Default => 0u32.write_to(writer)?,
                _ => bail!("value must be a Value::U32")
            },
            Self::U64 => match value {
                Value::U64(v) => v.write_to(writer)?,
                Value::Default => 0u64.write_to(writer)?,
                _ => bail!("value must be a Value::U64")
            },
            Self::F32 => match value {
                Value::F32(v) => v.write_to(writer)?,
                Value::Default => 0f32.write_to(writer)?,
                _ => bail!("value must be a Value::F32")
            },
            Self::F64 => match value {
                Value::F64(v) => v.write_to(writer)?,
                Value::Default => 0f64.write_to(writer)?,
                _ => bail!("value must be a Value::F64")
            },
            Self::Str(size) => match value {
                Value::Str(v) => {
                    // validate string value
                    let size = *size;
                    let value_buf = v.as_bytes();
                    let value_size = value_buf.len() as u32;
                    if value_size > size {
                        bail!(
                            "string value size ({} bytes) is bigger than field size ({} bytes)",
                            value_size,
                            size
                        );
                    }

                    // write value
                    value_size.write_to(writer)?;
                    writer.write_all(&value_buf)?;
                    if value_size < size {
                        // fill with zeros
                        writer.write_all(&vec![0u8; (size - value_size) as usize])?;
                    }
                },
                Value::Default => {
                    // write default value size and string value
                    0u32.write_to(writer)?;
                    writer.write_all(&vec![0u8; (*size) as usize])?;
                },
                _ => bail!("value must be a Value::Str")
            }
        }
        Ok(())
    }
}

impl ByteSized for FieldType {
    /// Byte representation: `<type:1><value:4>`.
    const BYTES: usize = 5;
}

impl ReadFrom for FieldType {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        // read data
        let mut buf = [0u8; Self::BYTES];
        reader.read_exact(&mut buf)?;
        
        // build field type
        let field_type = match buf[0] {
            1 => Self::Bool,
            2 => Self::I8,
            3 => Self::I16,
            4 => Self::I32,
            5 => Self::I64,
            6 => Self::U8,
            7 => Self::U16,
            8 => Self::U32,
            9 => Self::U64,
            10 => Self::F32,
            11 => Self::F64,
            12 => {
                Self::Str(u32::from_byte_slice(&buf[1..])?)
            },
            _ => bail!(ParseError::InvalidValue)
        };
        Ok(field_type)
    }
}

impl WriteTo for FieldType {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        let mut buf = [0u8; Self::BYTES];
        match self {
            Self::Bool => buf[0] = 1,
            Self::I8 => buf[0] = 2,
            Self::I16 => buf[0] = 3,
            Self::I32 => buf[0] = 4,
            Self::I64 => buf[0] = 5,
            Self::U8 => buf[0] = 6,
            Self::U16 => buf[0] = 7,
            Self::U32 => buf[0] = 8,
            Self::U64 => buf[0] = 9,
            Self::F32 => buf[0] = 10,
            Self::F64 => buf[0] = 11,
            Self::Str(size) => {
                buf[0] = 12;
                size.write_as_bytes(&mut buf[1..])?;
            }
        };
        writer.write_all(&buf)?;
        Ok(())
    }
}

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

/// Represent the record header. Byte format: `<field_count:1><fields:?>`
#[derive(Debug, PartialEq, Clone)]
pub struct Header {
    _list: Vec<Field>,
    _map: HashMap<String, usize>,
    _record_byte_size: u64
}

impl Header {
    /// Create a new instance.
    pub fn new() -> Self {
        Self{
            _list: Vec::new(),
            _map: HashMap::new(),
            _record_byte_size: 0
        }
    }

    /// Add a new field.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    /// * `value_type` - Field value type.
    pub fn add(&mut self, name: &str, value_type: FieldType) -> Result<&Self> {
        let field = Field::new(name, value_type)?;

        // avoid duplicated fields
        if let Some(_) = self._map.get(&field._name) {
            bail!("field \"{}\" already exists within the header", field._name);
        }

        // add field
        self._record_byte_size += field._value_type.value_byte_size() as u64;
        self._list.push(field);
        self._map.insert(name.to_string(), self._list.len()-1);
        
        Ok(self)
    }

    /// Rebuilds the index hashmap.
    fn rebuild_hashmap(&mut self) {
        let mut field_map = HashMap::new();
        let mut record_size = 0u64;
        for (index, field) in self._list.iter().enumerate() {
            field_map.insert(field._name.clone(), index);
            record_size += field._value_type.value_byte_size() as u64;
        }
        self._map = field_map;
        self._record_byte_size = record_size;
    }

    /// Removes and return the field at the index position.
    /// This is currently very inefficient as the map is rebuilt.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Field index to remove.
    pub fn remove(&mut self, index: usize) -> Field {
        let field = self._list.remove(index);
        self.rebuild_hashmap();
        field
    }

    /// Removes and return the field with the specified name.
    /// This is currently very inefficient as the map is rebuilt.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    pub fn remove_by_name(&mut self, name: &str) -> Option<Field> {
        // remove from hash map
        let index = match self._map.get(name) {
            Some(v) => *v,
            None => return None
        };

        // remove from vec
        Some(self.remove(index))
    }

    /// Get a field by name.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    pub fn get(&self, name: &str) -> Option<&Field> {
        let index = match self._map.get(name) {
            Some(v) => *v,
            None => return None
        };
        Some(&self._list[index])
    }

    /// Get a field by name as mutable.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Field> {
        let index = match self._map.get(name) {
            Some(v) => *v,
            None => return None
        };
        Some(&mut self._list[index])
    }

    /// Get a field by it's index.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Field index.
    pub fn get_by_index(&self, index: usize) -> Option<&Field> {
        if self._list.len() > index {
            return Some(&self._list[index]);
        }
        None
    }

    /// Get a field by it's index as mutable.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Field index.
    pub fn get_mut_by_index(&mut self, index: usize) -> Option<&mut Field> {
        if self._list.len() > index {
            return Some(&mut self._list[index]);
        }
        None
    }

    /// Returns the number of fields on the header.
    pub fn len(&self) -> usize {
        self._list.len()
    }

    /// Return the previously calculated byte count to be writed when
    /// the header is converted into bytes.
    pub fn size_as_bytes(&self) -> u64 {
        u32::BYTES as u64 + (Field::BYTES as u64 * self._list.len() as u64)
    }

    /// Returns the record size in bytes.
    pub fn record_byte_size(&self) -> u64 {
        return self._record_byte_size;
    }

    /// Clears the field type list.
    pub fn clear(&mut self) {
        self._list = Vec::new();
        self._map = HashMap::new();
        self._record_byte_size = 0;
    }

    /// Creates a new record instance from the header fields.
    pub fn new_record(&self) -> Result<Record> {
        let mut record = Record::new();

        for field in self._list.iter() {
            record.add(&field._name, Value::Default)?;
        }
        Ok(record)
    }

    /// Reads a record from the reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    pub fn read_record(&self, reader: &mut impl Read) -> Result<Record> {
        let mut record = Record::new();

        for field in self._list.iter() {
            let value = field._value_type.read_value(reader)?;
            record.add(&field._name, value)?;
        }
        Ok(record)
    }

    /// Writes a record into the writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    pub fn write_record(&self, writer: &mut impl Write, record: &Record) -> Result<()> {
        if self._list.len() != record.len() {
            bail!("header field count mismatch the record value count");
        }
        for (index, field) in self._list.iter().enumerate() {
            let value = match record.get_by_index(index) {
                Some(v) => v,
                None => bail!("invalid value index! this should never happen, please check \
                    the record \"len()\" function")
            };
            if let Err(e) = field._value_type.write_value(writer, value) {
                bail!("error saving field \"{}\": {}", &field._name, e);
            }
        }
        Ok(())
    }

    /// Returns an iterator over the header fields.
    pub fn iter(&self) -> std::slice::Iter<Field> {
        self._list.iter()
    }
}

impl LoadFrom for Header {
    fn load_from(&mut self, reader: &mut impl Read) -> Result<()> {
        // read field count
        let field_count = u32::read_from(reader)?;

        // read fields
        let mut record_size = 0u64;
        let mut list = Vec::new();
        let mut map = HashMap::new();
        for i in 0..field_count {
            // read field data and push into the field list
            let field = Field::read_from(reader)?;
            if let Some(_) = map.insert(field._name.clone(), i as usize) {
                bail!("duplicated field \"{}\"", &field._name);
            }
            record_size += field._value_type.value_byte_size() as u64;
            list.push(field);
        }

        // save read field list
        self._list = list;
        self._map = map;
        self._record_byte_size = record_size;
        Ok(())
    }
}

impl ReadFrom for Header {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut header = Self::new();
        header.load_from(reader)?;
        Ok(header)
    }
}

impl WriteTo for Header {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        // write field count
        let field_count = self._list.len() as u32;
        field_count.write_to(writer)?;

        // write fields data
        for field in self._list.iter() {
            field.write_to(writer)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod field_type {
        use super::*;

        #[test]
        fn min_type_id() {
            assert_eq!(1u8, FieldType::MIN_TYPE_ID);
        }

        #[test]
        fn max_type_id() {
            assert_eq!(12u8, FieldType::MAX_TYPE_ID);
        }

        #[test]
        fn value_byte_size() {
            assert_eq!(bool::BYTES, FieldType::Bool.value_byte_size());
            assert_eq!(i8::BYTES, FieldType::I8.value_byte_size());
            assert_eq!(i16::BYTES, FieldType::I16.value_byte_size());
            assert_eq!(i32::BYTES, FieldType::I32.value_byte_size());
            assert_eq!(i64::BYTES, FieldType::I64.value_byte_size());
            assert_eq!(u8::BYTES, FieldType::U8.value_byte_size());
            assert_eq!(u16::BYTES, FieldType::U16.value_byte_size());
            assert_eq!(u32::BYTES, FieldType::U32.value_byte_size());
            assert_eq!(u64::BYTES, FieldType::U64.value_byte_size());
            assert_eq!(f32::BYTES, FieldType::F32.value_byte_size());
            assert_eq!(f64::BYTES, FieldType::F64.value_byte_size());
            assert_eq!(29usize, FieldType::Str(25u32).value_byte_size());
        }

        #[test]
        fn str_size() {
            let expected = 47u32;
            match FieldType::Str(47u32).str_size() {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = 234u32;
            match FieldType::Str(234u32).str_size() {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn bool_is_valid() {
            let field_type = FieldType::Bool;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(true, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn i8_is_valid() {
            let field_type = FieldType::I8;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(true, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn i16_is_valid() {
            let field_type = FieldType::I16;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(true, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn i32_is_valid() {
            let field_type = FieldType::I32;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(true, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn i64_is_valid() {
            let field_type = FieldType::I64;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(true, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn u8_is_valid() {
            let field_type = FieldType::U8;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(true, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn u16_is_valid() {
            let field_type = FieldType::U16;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(true, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn u32_is_valid() {
            let field_type = FieldType::U32;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(true, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn u64_is_valid() {
            let field_type = FieldType::U64;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(true, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn f32_is_valid() {
            let field_type = FieldType::F32;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(true, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn f64_is_valid() {
            let field_type = FieldType::F64;
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(true, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(false, field_type.is_valid(&Value::Str("".to_string())));
        }

        #[test]
        fn str_is_valid() {
            let field_type = FieldType::Str(5);
            assert_eq!(true, field_type.is_valid(&Value::Default));
            assert_eq!(false, field_type.is_valid(&Value::Bool(false)));
            assert_eq!(false, field_type.is_valid(&Value::I8(0)));
            assert_eq!(false, field_type.is_valid(&Value::I16(0)));
            assert_eq!(false, field_type.is_valid(&Value::I32(0)));
            assert_eq!(false, field_type.is_valid(&Value::I64(0)));
            assert_eq!(false, field_type.is_valid(&Value::U8(0)));
            assert_eq!(false, field_type.is_valid(&Value::U16(0)));
            assert_eq!(false, field_type.is_valid(&Value::U32(0)));
            assert_eq!(false, field_type.is_valid(&Value::U64(0)));
            assert_eq!(false, field_type.is_valid(&Value::F32(0f32)));
            assert_eq!(false, field_type.is_valid(&Value::F64(0f64)));
            assert_eq!(true, field_type.is_valid(&Value::Str("abc".to_string())));
            assert_eq!(true, field_type.is_valid(&Value::Str("abcde".to_string())));
            assert_eq!(false, field_type.is_valid(&Value::Str("abcdef".to_string())));
        }

        #[test]
        fn bool_read_value() {
            let expected = Value::Bool(false);
            match FieldType::Bool.read_value(&mut (&[0u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = Value::Bool(true);
            match FieldType::Bool.read_value(&mut (&[1u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i8_read_value() {
            let expected = Value::I8(12i8);
            match FieldType::I8.read_value(&mut (&[12u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = Value::I8(-34i8);
            match FieldType::I8.read_value(&mut (&[222u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i16_read_value() {
            let expected = Value::I16(7948i16);
            match FieldType::I16.read_value(&mut (&[31u8, 12u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = Value::I16(-6388i16);
            match FieldType::I16.read_value(&mut (&[231u8, 12u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i32_read_value() {
            let expected = Value::I32(2064390957i32);
            match FieldType::I32.read_value(&mut (&[123u8, 12u8, 27u8, 45u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = Value::I32(-552854739i32);
            match FieldType::I32.read_value(&mut (&[223u8, 12u8, 27u8, 45u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i64_read_value() {
            let expected = Value::I64(2309250590096973324i64);
            match FieldType::I64.read_value(&mut (&[32u8, 12u8, 27u8, 45u8, 64u8, 23u8, 94u8, 12u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = Value::I64(-1725974676026991092i64);
            match FieldType::I64.read_value(&mut (&[232u8, 12u8, 27u8, 45u8, 64u8, 23u8, 94u8, 12u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u8_read_value() {
            let expected = Value::U8(45u8);
            match FieldType::U8.read_value(&mut (&[45u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u16_read_value() {
            let expected = Value::U16(9494u16);
            match FieldType::U16.read_value(&mut (&[37u8, 22u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u32_read_value() {
            let expected = Value::U32(2065046317u32);
            match FieldType::U32.read_value(&mut (&[123u8, 22u8, 27u8, 45u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u64_read_value() {
            let expected = Value::U64(2312065339864079884u64);
            match FieldType::U64.read_value(&mut (&[32u8, 22u8, 27u8, 45u8, 64u8, 23u8, 94u8, 12u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn f32_read_value() {
            let expected = Value::F32(123434.52343f32);
            match FieldType::F32.read_value(&mut (&[71u8, 241u8, 21u8, 67u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = Value::F32(-43434.52343f32);
            match FieldType::F32.read_value(&mut (&[199u8, 41u8, 170u8, 134u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn f64_read_value() {
            let expected = Value::F64(76434523423424.52343f64);
            match FieldType::F64.read_value(&mut (&[66u8, 209u8, 97u8, 19u8, 39u8, 128u8, 176u8, 33u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = Value::F64(-43434523423424.52343f64);
            match FieldType::F64.read_value(&mut (&[194u8, 195u8, 192u8, 113u8, 171u8, 121u8, 96u8, 67u8] as &[u8])) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_read_value_partial() {
            // test partial
            let expected = Value::Str("abcdefg".to_string());
            let mut reader = &[
                // value size as 7u32
                0u8, 0u8, 0u8, 7u8,
                // string value
                97u8, 98u8, 99u8, 100u8, 101u8, 102u8, 103u8, 0u8, 0u8, 0u8,
                // extra bytes, this shouldn't be read
                10u8, 20u8, 33u8
            ] as &[u8];
            match FieldType::Str(10).read_value(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // check final reader position
            let mut buf = [0u8, 0u8, 0u8];
            let expected = [10u8, 20u8, 33u8];
            match reader.read_exact(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
        }

        #[test]
        fn str_read_value_exact() {
            let expected = Value::Str("abcdefgh".to_string());
            let mut reader = &[
                // value size as 8u32
                0u8, 0u8, 0u8, 8u8,
                // string value
                97u8, 98u8, 99u8, 100u8, 101u8, 102u8, 103u8, 104u8
            ] as &[u8];
            match FieldType::Str(8).read_value(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_read_value_with_garbage() {
            let mut reader = &[
                // value size as 2u32
                0u8, 0u8, 0u8, 2u8,
                // string value with garbage
                97u8, 98u8, 99u8, 100u8, 101u8,
                // extra bytes, this shouldn't be read
                2u8, 34u8
            ] as &[u8];
            let expected = Value::Str("ab".to_string());
            match FieldType::Str(5).read_value(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // check final reader position
            let mut buf = [0u8, 0u8];
            let expected = [2u8, 34u8];
            match reader.read_exact(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_read_value_with_zero_field() {
            let mut reader = &[
                // value size as 032
                0u8, 0u8, 0u8, 0u8,
                // no string value, just some extra bytes that shouldn't be read
                23u8, 54u8
            ] as &[u8];
            let expected = Value::Str("".to_string());
            match FieldType::Str(0).read_value(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // check final reader position
            let mut buf = [0u8, 0u8];
            let expected = [23u8, 54u8];
            match reader.read_exact(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_read_value_empty() {
            let mut reader = &[
                // value size as 0u32
                0u8, 0u8, 0u8, 0u8,
                // string value with some garbage to ignore
                97u8, 98u8, 99u8, 100u8,
                // extra bytes, this shouldn't be read
                54u8, 24u8
            ] as &[u8];
            let expected = Value::Str("".to_string());
            match FieldType::Str(4).read_value(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // check final reader position
            let mut buf = [0u8, 0u8];
            let expected = [54u8, 24u8];
            match reader.read_exact(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_read_value_with_invalid_value_size() {
            let mut reader = &[
                // value size as 7u32, this is invalid given the field size of 2u32
                0u8, 0u8, 0u8, 7u8,
                // string value
                97u8, 98u8,
                // extra bytes, this shouldn't be read
                99u8, 100u8, 101u8, 102u8, 103u8, 0u8, 0u8, 0u8
            ] as &[u8];
            let expected = "string value size can't be bigger than the field size";
            match FieldType::Str(2).read_value(&mut reader) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
        }

        #[test]
        fn bool_write_value() {
            let field_type = FieldType::Bool;
            let expected_err = "value must be a Value::Bool";

            // test default
            let expected = [0u8];
            let mut buf = [0u8; 1];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [0u8];
            let mut buf = [0u8; 1];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = [1u8];
            let mut buf = [0u8; 1];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(true)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            let mut buf = [0u8; 1];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn i8_write_value() {
            let field_type = FieldType::I8;
            let expected_err = "value must be a Value::I8";

            // test default
            let expected = [0u8];
            let mut buf = [0u8; 1];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [32u8];
            let mut buf = [0u8; 1];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(32)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = [233u8];
            let mut buf = [0u8; 1];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(-23)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn i16_write_value() {
            let field_type = FieldType::I16;
            let expected_err = "value must be a Value::I16";

            // test default
            let expected = [0u8, 0u8];
            let mut buf = [0u8; 2];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [12u8, 161u8];
            let mut buf = [0u8; 2];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(3233)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = [165u8, 62u8];
            let mut buf = [0u8; 2];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(-23234)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn i32_write_value() {
            let field_type = FieldType::I32;
            let expected_err = "value must be a Value::I32";

            // test default
            let expected = [0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 4];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [1u8, 237u8, 132u8, 83u8];
            let mut buf = [0u8; 4];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(32343123)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = [254u8, 154u8, 103u8, 87u8];
            let mut buf = [0u8; 4];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(-23435433)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn i64_write_value() {
            let field_type = FieldType::I64;
            let expected_err = "value must be a Value::I64";

            // test default
            let expected = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 8];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [4u8, 121u8, 49u8, 116u8, 167u8, 210u8, 32u8, 239u8];
            let mut buf = [0u8; 8];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(322343225435234543)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = [252u8, 194u8, 155u8, 130u8, 154u8, 40u8, 166u8, 175u8];
            let mut buf = [0u8; 8];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(-233453245435435345)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn u8_write_value() {
            let field_type = FieldType::U8;
            let expected_err = "value must be a Value::U8";

            // test default
            let expected = [0u8];
            let mut buf = [0u8; 1];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [35u8];
            let mut buf = [0u8; 1];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(35)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn u16_write_value() {
            let field_type = FieldType::U16;
            let expected_err = "value must be a Value::U16";

            // test default
            let expected = [0u8, 0u8];
            let mut buf = [0u8; 2];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [91u8, 128u8];
            let mut buf = [0u8; 2];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(23424)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn u32_write_value() {
            let field_type = FieldType::U32;
            let expected_err = "value must be a Value::U32";

            // test default
            let expected = [0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 4];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [21u8, 17u8, 72u8, 244u8];
            let mut buf = [0u8; 4];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(353454324)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn u64_write_value() {
            let field_type = FieldType::U64;
            let expected_err = "value must be a Value::U64";

            // test default
            let expected = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 8];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [5u8, 26u8, 234u8, 174u8, 54u8, 115u8, 174u8, 13u8];
            let mut buf = [0u8; 8];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(367864353542876685)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn f32_write_value() {
            let field_type = FieldType::F32;
            let expected_err = "value must be a Value::F32";

            // test default
            let expected = [0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 4];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [79u8, 82u8, 172u8, 219u8];
            let mut buf = [0u8; 4];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(3534543534.122312f32)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = [207u8, 82u8, 172u8, 219u8];
            let mut buf = [0u8; 4];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(-3534543534.122312f32)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn f64_write_value() {
            let field_type = FieldType::F64;
            let expected_err = "value must be a Value::F64";

            // test default
            let expected = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 8];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test valid writes
            let expected = [66u8, 244u8, 23u8, 30u8, 105u8, 128u8, 114u8, 226u8];
            let mut buf = [0u8; 8];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(353432432543534.122312f64)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = [195u8, 41u8, 29u8, 76u8, 121u8, 94u8, 66u8, 252u8];
            let mut buf = [0u8; 8];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(-3534544354353534.122312f64)) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected_err, e.to_string())
            };
        }

        #[test]
        fn str_write_value_default() {
            let field_type = FieldType::Str(5);

            // test default
            let expected = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 9];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Default) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_write_value_with_valid_values() {
            let field_type = FieldType::Str(5);
    
            // test valid writes
            let expected = [0u8, 0u8, 0u8, 3u8, 97u8, 98u8, 99u8, 0u8, 0u8];
            let mut buf = [0u8; 9];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("abc".to_string())) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
            let expected = [0u8, 0u8, 0u8, 5u8, 97u8, 98u8, 99u8, 100u8, 101u8];
            let mut buf = [0u8; 9];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("abcde".to_string())) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_write_value_with_empty_str() {
            let field_type = FieldType::Str(5);
            let expected = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 9];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_write_value_with_zero_field() {
            let field_type = FieldType::Str(0);
            let expected = [0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 4];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("".to_string())) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_write_value_invalid_value_size() {
            let field_type = FieldType::Str(2);
            let expected = "string value size (3 bytes) is bigger than field size (2 bytes)";
            let mut buf = [0u8; 6];
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Str("abc".to_string())) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
        }

        #[test]
        fn str_write_value_with_other_types() {
            let field_type = FieldType::Str(1);
            let expected = "value must be a Value::Str";
            let mut buf = [0u8; 5];

            // test invalid writes
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::Bool(false)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::I64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U8(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U16(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U32(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::U64(0)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F32(0f32)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
            match field_type.write_value(&mut (&mut buf as &mut [u8]), &Value::F64(0f64)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            };
        }

        #[test]
        fn byte_sized() {
            assert_eq!(5, FieldType::BYTES);
        }

        #[test]
        fn bool_read_from() {
            let mut reader = &[1u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::Bool;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i8_read_from() {
            let mut reader = &[2u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::I8;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i16_read_from() {
            let mut reader = &[3u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::I16;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i32_read_from() {
            let mut reader = &[4u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::I32;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i64_read_from() {
            let mut reader = &[5u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::I64;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u8_read_from() {
            let mut reader = &[6u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::U8;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u16_read_from() {
            let mut reader = &[7u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::U16;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u32_read_from() {
            let mut reader = &[8u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::U32;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u64_read_from() {
            let mut reader = &[9u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::U64;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn f32_read_from() {
            let mut reader = &[10u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::F32;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn f64_read_from() {
            let mut reader = &[11u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::F64;
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_read_from_with_size() {
            let mut reader = &[12u8, 43u8, 23u8, 65u8, 86u8] as &[u8];
            let expected = FieldType::Str(722944342);
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_read_from_with_zero_size() {
            let mut reader = &[12u8, 0u8, 0u8, 0u8, 0u8] as &[u8];
            let expected = FieldType::Str(0);
            match FieldType::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn bool_write_to() {
            let field_type = FieldType::Bool;
            let expected = [1u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i8_write_to() {
            let field_type = FieldType::I8;
            let expected = [2u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i16_write_to() {
            let field_type = FieldType::I16;
            let expected = [3u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i32_write_to() {
            let field_type = FieldType::I32;
            let expected = [4u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn i64_write_to() {
            let field_type = FieldType::I64;
            let expected = [5u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u8_write_to() {
            let field_type = FieldType::U8;
            let expected = [6u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u16_write_to() {
            let field_type = FieldType::U16;
            let expected = [7u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u32_write_to() {
            let field_type = FieldType::U32;
            let expected = [8u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn u64_write_to() {
            let field_type = FieldType::U64;
            let expected = [9u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn f32_write_to() {
            let field_type = FieldType::F32;
            let expected = [10u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn f64_write_to() {
            let field_type = FieldType::F64;
            let expected = [11u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_write_to_with_size() {
            let field_type = FieldType::Str(234655354);
            let expected = [12u8, 13u8, 252u8, 142u8, 122u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }

        #[test]
        fn str_write_to_with_zero_size() {
            let field_type = FieldType::Str(0);
            let expected = [12u8, 0u8, 0u8, 0u8, 0u8];
            let mut buf = [0u8; 5];
            let mut writer = &mut buf as &mut [u8];
            match field_type.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            };
        }
    }

    mod field {
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

    mod header {
        use super::*;

        #[test]
        fn new_header() {
            let expected = Header{
                _list: Vec::new(),
                _map: HashMap::new(),
                _record_byte_size: 0
            };
            let header = Header::new();
            assert_eq!(expected, header);
        }

        #[test]
        fn add_field() {
            let expected_0 = Field{
                _name: "foo".to_string(),
                _value_type: FieldType::F32
            };
            let expected_1 = Field{
                _name: "bar".to_string(),
                _value_type: FieldType::I32
            };
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::I32) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }

            // test list and map
            assert_eq!(2, header._list.len());
            assert_eq!(expected_0, header._list[0]);
            assert_eq!(expected_1, header._list[1]);
            assert_eq!(8, header._record_byte_size);
            match header._map.get("foo") {
                Some(v) => assert_eq!(0, *v),
                None => assert!(false, "expected {:?} but got None", 0)
            }
            match header._map.get("bar") {
                Some(v) => assert_eq!(1, *v),
                None => assert!(false, "expected {:?} but got None", 1)
            }
        }

        #[test]
        fn add_dup_field() {
            let expected = "field \"foo\" already exists within the header";
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            match header.add("foo", FieldType::I32) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }
        }

        #[test]
        fn rebuild_hashmap() {
            let mut header = Header{
                _list: vec!(
                    Field{
                        _name: "abc".to_string(),
                        _value_type: FieldType::U32
                    },
                    Field{
                        _name: "def".to_string(),
                        _value_type: FieldType::Str(45)
                    }
                ),
                _map: HashMap::new(),
                _record_byte_size: 0
            };
            header.rebuild_hashmap();
            assert_eq!(53u64, header._record_byte_size);
            match header._map.get("abc") {
                Some(v) => assert_eq!(0, *v),
                None => assert!(false, "expected {:?} but got None", 0)
            }
            match header._map.get("def") {
                Some(v) => assert_eq!(1, *v),
                None => assert!(false, "expected {:?} but got None", 1)
            }
        }

        #[test]
        fn remove_with_index() {
            let expected = Field{
                _name: "abcde".to_string(),
                _value_type: FieldType::I64
            };
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            assert_eq!(3, header._list.len());
            assert_eq!(3, header._map.len());
            assert_eq!(expected, header._list[1]);
            assert_eq!(20, header._record_byte_size);
            match header._map.get("abcde") {
                Some(v) => assert_eq!(1, *v),
                None => assert!(false, "expected {:?} but got None", 1)
            }

            // remove the header
            let deleted = header.remove(1);
            assert_eq!(expected, deleted);
            assert_eq!(2, header._list.len());
            assert_eq!(2, header._map.len());
            assert_ne!(deleted, header._list[1]);
            assert_eq!(12, header._record_byte_size);
            match header._map.get("abcde") {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            }
        }

        #[test]
        fn remove_by_name() {
            let expected = Field{
                _name: "abcde".to_string(),
                _value_type: FieldType::I64
            };
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("faa", FieldType::F64) {
                assert!(false, "expected to add \"faa\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            assert_eq!(4, header._list.len());
            assert_eq!(4, header._map.len());
            assert_eq!(expected, header._list[2]);
            assert_eq!(28, header._record_byte_size);
            match header._map.get("abcde") {
                Some(v) => assert_eq!(2, *v),
                None => assert!(false, "expected {:?} but got None", 1)
            }

            // remove the header
            let deleted = match header.remove_by_name("abcde") {
                Some(v) => v,
                None => {
                    assert!(false, "expected {:?} but got None", expected);
                    return;
                }
            };
            assert_eq!(expected, deleted);
            assert_eq!(3, header._list.len());
            assert_eq!(3, header._map.len());
            assert_ne!(deleted, header._list[2]);
            assert_eq!(20, header._record_byte_size);
            match header._map.get("abcde") {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            }
        }

        #[test]
        fn remove_by_name_not_found() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("faa", FieldType::F64) {
                assert!(false, "expected to add \"faa\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            assert_eq!(3, header._list.len());
            assert_eq!(3, header._map.len());
            assert_eq!(20, header._record_byte_size);
            match header._map.get("abcde") {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            }

            // try to remove the header
            match header.remove_by_name("abcde") {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            };
            assert_eq!(3, header._list.len());
            assert_eq!(3, header._map.len());
            assert_eq!(20, header._record_byte_size);
        }

        #[test]
        fn get_by_index_existing() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            assert_eq!(3, header._list.len());

            // test search by index
            let expected = Field{
                _name: "abcde".to_string(),
                _value_type: FieldType::I64
            };
            assert_eq!(expected, header._list[1]);
            match header.get_by_index(1) {
                Some(v) => assert_eq!(&expected, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }

            // test search mutable by index
            let mut expected = Field{
                _name: "foo".to_string(),
                _value_type: FieldType::F32
            };
            assert_eq!(expected, header._list[0]);
            match header.get_mut_by_index(0) {
                Some(v) => assert_eq!(&mut expected, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }
        }

        #[test]
        fn get_by_index_not_found() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            assert_eq!(3, header._list.len());

            // test search
            match header.get_by_index(4) {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            }
            match header.get_mut_by_index(5) {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            }
        }

        #[test]
        fn get_existing() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            assert_eq!(3, header._list.len());
            assert_eq!(3, header._map.len());

            // test search by index
            let expected = Field{
                _name: "abcde".to_string(),
                _value_type: FieldType::I64
            };
            assert_eq!(expected, header._list[1]);
            match header.get("abcde") {
                Some(v) => assert_eq!(&expected, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }

            // test search mutable by index
            let mut expected = Field{
                _name: "foo".to_string(),
                _value_type: FieldType::F32
            };
            assert_eq!(expected, header._list[0]);
            match header.get_mut("foo") {
                Some(v) => assert_eq!(&mut expected, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }
        }

        #[test]
        fn get_not_found() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            assert_eq!(3, header._list.len());
            assert_eq!(3, header._map.len());

            // test search
            match header.get("aaa") {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            }
            match header.get_mut("bbb") {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            }
        }

        #[test]
        fn len() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }

            // test length
            assert_eq!(2, header.len());

            // add fields
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }

            // test length
            assert_eq!(3, header.len());

            // delete 2 items
            header.remove(1);
            header.remove_by_name("foo");

            // test length
            assert_eq!(1, header.len());
        }

        #[test]
        fn size_as_bytes() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }

            // test length
            assert_eq!(122, header.size_as_bytes());

            // add fields
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }

            // test length
            assert_eq!(181, header.size_as_bytes());
        }

        #[test]
        fn record_byte_size() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }

            // test length
            assert_eq!(122, header.size_as_bytes());
            assert_eq!(12, header._record_byte_size);

            // add fields
            if let Err(e) = header.add("bar", FieldType::U64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }

            // test length
            assert_eq!(181, header.size_as_bytes());
            assert_eq!(20, header._record_byte_size);
        }

        #[test]
        fn clear() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I64) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }
            assert_eq!(2, header._list.len());
            assert_eq!(12, header._record_byte_size);

            // test clear
            let expected: Vec<Field> = Vec::new();
            header.clear();
            assert_eq!(expected, header._list);
            assert_eq!(0, header._record_byte_size);
        }

        #[test]
        fn new_record() {
            let mut header = Header::new();

            // add fields
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::I64) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }

            // test new record
            let mut expected = Record::new();
            if let Err(e) = expected.add("foo", Value::Default) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = expected.add("bar", Value::Default) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            let record = match header.new_record() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "expected a new record but got error: {:?}", e);
                    return
                }
            };
            assert_eq!(expected, record);
        }

        #[test]
        fn read_record() {
            // create buffer and reader
            let buf = [
                // foo field
                6u8, 74u8, 236u8, 75u8, 242u8, 24u8, 101u8, 197u8,
                // bar field value size
                0, 0, 0, 5u8,
                // bar field value
                104u8, 101u8, 108u8, 108u8, 111u8, 0, 0, 0, 0, 0,
                // abc field
                9u8, 41u8
            ];
            let mut reader = &buf as &[u8];

            // create header
            let mut header = Header::new();
            if let Err(e) = header.add("foo", FieldType::U64) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::Str(10)) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abc", FieldType::I16) {
                assert!(false, "expected to add \"abc\" field but got error: {:?}", e);
                return;
            }

            // create expected record
            let mut expected = Record::new();
            if let Err(e) = expected.add("foo", Value::U64(453434523432543685u64)) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = expected.add("bar", Value::Str("hello".to_string())) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = expected.add("abc", Value::I16(2345i16)) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }

            // test
            match header.read_record(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
        }

        #[test]
        fn write_record() {
            let expected = [
                // foo field
                74u8, 138u8, 96u8, 147u8,
                // bar field value size
                0, 0, 0, 6u8,
                // bar field value
                119u8, 111u8, 114u8, 108u8, 100u8, 33u8, 0, 0, 0, 0, 0, 0,
                // abc field
                48u8, 141u8, 107u8, 57u8, 24u8, 192u8, 156u8, 149u8
            ];

            // create header
            let mut header = Header::new();
            if let Err(e) = header.add("foo", FieldType::F32) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::Str(12)) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abc", FieldType::U64) {
                assert!(false, "expected to add \"abc\" field but got error: {:?}", e);
                return;
            }

            // create record
            let mut record = Record::new();
            if let Err(e) = record.add("foo", Value::F32(4534345.345f32)) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = record.add("bar", Value::Str("world!".to_string())) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = record.add("abc", Value::U64(3498570378509327509u64)) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }

            // test
            let mut buf = [0u8; 28];
            let mut writer = &mut buf as &mut [u8];
            match header.write_record(&mut writer, &record) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
        }

        #[test]
        fn load_from_with_uniq_fields() {
            // expected header
            let mut expected = Header::new();
            if let Err(e) = expected.add("foo", FieldType::F64) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = expected.add("bar", FieldType::Str(45)) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = expected.add("abcde", FieldType::I8) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }

            // test
            let buf = [
                // field count
                0, 0, 0, 3u8,

                // foo field name value size
                0, 0, 0, 3u8,
                // foo field name value
                102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                // foo field type
                11u8, 0, 0, 0, 0,

                // bar field name value size
                0, 0, 0, 3u8,
                // bar field name value
                98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                // bar field type
                12u8, 0, 0, 0, 45u8,

                // abcde field name value size
                0, 0, 0, 5u8,
                // abcde field name value
                97u8, 98u8, 99u8, 100u8, 101u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                // abcde field type
                2u8, 0, 0, 0, 0
            ];
            let mut reader = &buf as &[u8];
            let mut header = Header::new();
            match header.load_from(&mut reader) {
                Ok(()) => assert_eq!(expected, header),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
        }

        #[test]
        fn load_from_with_dup_fields() {
            // expected header
            let expected = "duplicated field \"foo\"";

            // test
            let buf = [
                // field count
                0, 0, 0, 2u8,

                // foo field name value size
                0, 0, 0, 3u8,
                // foo field name value
                102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                // foo field type
                11u8, 0, 0, 0, 0,

                // dup foo field name value size
                0, 0, 0, 3u8,
                // dup foo field name value
                102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                // dup foo field type should be detected even with different types
                1u8, 0, 0, 0, 0
            ];
            let mut reader = &buf as &[u8];
            let mut header = Header::new();
            match header.load_from(&mut reader) {
                Ok(()) => assert!(false, "expected error but got sucess"),
                Err(e) => assert_eq!(expected, e.to_string())
            }
        }

        #[test]
        fn read_from_with_uniq_fields() {
            // expected header
            let mut expected = Header::new();
            if let Err(e) = expected.add("foo", FieldType::U64) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = expected.add("hello", FieldType::Str(656875457u32)) {
                assert!(false, "expected to add \"hello\" field but got error: {:?}", e);
                return;
            }

            // test
            let buf = [
                // field count
                0, 0, 0, 2u8,

                // foo field name value size
                0, 0, 0, 3u8,
                // foo field name value
                102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                // foo field type
                9u8, 0, 0, 0, 0,

                // hello field name value size
                0, 0, 0, 5u8,
                // hello field name value
                104u8, 101u8, 108u8, 108u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0,
                // hello field type
                12u8, 39u8, 39u8, 31u8, 193u8,
            ];
            let mut reader = &buf as &[u8];
            match Header::read_from(&mut reader) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
        }

        #[test]
        fn read_from_with_dup_fields() {
            // expected header
            let expected = "duplicated field \"bar\"";

            // test
            let buf = [
                // field count
                0, 0, 0, 2u8,

                // bar field name value size
                0, 0, 0, 3u8,
                // bar field name value
                98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                // bar field type
                5u8, 0, 0, 0, 0,

                // dup bar field name value size
                0, 0, 0, 3u8,
                // dup bar field name value
                98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                // dup bar field type should be detected even with different types
                3u8, 0, 0, 0, 0
            ];
            let mut reader = &buf as &[u8];
            match Header::read_from(&mut reader) {
                Ok(v) => assert!(false, "expected error but got: {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }
        }

        #[test]
        fn write_to() {
            let expected = [
                // field count
                0, 0, 0, 3u8,

                // foo field name value size
                0, 0, 0, 3u8,
                // foo field name value
                102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                // foo field type
                1u8, 0, 0, 0, 0,

                // abcde field name value size
                0, 0, 0, 5u8,
                // abcde field name value
                97u8, 98u8, 99u8, 100u8, 101u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                // abcde field type
                2u8, 0, 0, 0, 0,

                // bar field name value size
                0, 0, 0, 3u8,
                // bar field name value
                98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
                // bar field type
                12u8, 0, 0, 0, 37u8
            ];

            // create header
            let mut header = Header::new();
            if let Err(e) = header.add("foo", FieldType::Bool) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I8) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::Str(37)) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }

            // test
            let mut buf = [0u8; 181];
            let mut writer = &mut buf as &mut [u8];
            match header.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
        }

        #[test]
        fn iter() {
            // create header
            let mut header = Header::new();
            if let Err(e) = header.add("foo", FieldType::Bool) {
                assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("abcde", FieldType::I8) {
                assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
                return;
            }
            if let Err(e) = header.add("bar", FieldType::Str(37)) {
                assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
                return;
            }

            // test
            let expected = vec!["foo".to_string(), "abcde".to_string(), "bar".to_string()];
            let mut field_names: Vec<String> = Vec::new();
            for field in header.iter() {
                field_names.push(field._name.clone());
            }
            assert_eq!(expected, field_names);
        }
    }
}