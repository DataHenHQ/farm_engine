use std::collections::HashMap;
use std::io::{Read, Write};
use anyhow::{bail, Result};
use crate::error::ParseError;
use crate::traits::{ByteSized, FromByteSlice, WriteAsBytes, ReadFrom, WriteTo};
use super::value::Value;
use super::Record;

/// Represents a field type. Byte format: `<type:1><value:4>`.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
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
    const MIN_TYPE_ID: u8 = 1u8;

    /// Max value the field type first byte can take.
    const MAX_TYPE_ID: u8 = 12u8;

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
                // read the real string size
                let value_size = u32::read_from(reader)? as usize;

                // read the string value
                let mut buf = vec![0u8; *size as usize];
                reader.read_exact(&mut buf[..value_size])?;
                Value::Str(String::from_utf8(buf)?)
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
                Value::Default => 064.write_to(writer)?,
                _ => bail!("value must be a Value::F64")
            },
            Self::Str(size) => match value {
                Value::Str(v) => {
                    // validate string value
                    let size = *size;
                    let value_buf = v.as_bytes();
                    let value_size = value_buf.len() as u32;
                    if value_size > size {
                        bail!("string value is bigger than field size ({} bytes)", size);
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
            12 => Self::Str(u32::from_byte_slice(&buf[1..])?),
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

/// Represents a field. Byte representation: `<name:50><field_type:5>`
#[derive(Debug, PartialEq)]
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
    pub fn get_type(&self) -> FieldType {
        self._value_type
    }
}

impl ByteSized for Field {
    const BYTES: usize = 55;
}

impl ReadFrom for Field {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        // read the field name
        let mut buf = [0u8; Self::MAX_NAME_SIZE];
        reader.read_exact(&mut buf)?;
        let name = String::from_utf8(buf.to_vec())?;

        // read field value type
        let value_type = FieldType::read_from(reader)?;

        // build field and provide read byte count
        let field = Field::new(&name, value_type)?;
        Ok(field)
    }
}

impl WriteTo for Field {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        // write name
        let name_bytes = self._name.as_bytes();
        if name_bytes.len() > Self::MAX_NAME_SIZE {
            bail!("field name size must be less than {} bytes length", Self::MAX_NAME_SIZE);
        }
        let mut buf = [0u8; Self::MAX_NAME_SIZE];
        buf.copy_from_slice(name_bytes);
        writer.write_all(&buf)?;

        // write field value type
        self._value_type.write_to(writer)?;
        Ok(())
    }
}

/// Represent the record header. Byte format: `<field_count:1><fields:?>`
#[derive(Debug, PartialEq)]
pub struct Header {
    _list: Vec<Field>,
    _map: HashMap<String, usize>
}

impl Header {
    /// Create a new instance.
    pub fn new() -> Self {
        Self{
            _list: Vec::new(),
            _map: HashMap::new()
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
        self._list.push(field);
        self._map.insert(name.to_string(), self._list.len()-1);
        
        Ok(self)
    }

    /// Rebuilds the index hashmap.
    fn rebuild_hashmap(&mut self) {
        let mut field_map = HashMap::new();
        for (index, field) in self._list.iter().enumerate() {
            field_map.insert(field._name.clone(), index);
        }
        self._map = field_map;
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

    /// Clears the field type list.
    pub fn clear(&mut self) {
        self._list = Vec::new();
        self._map = HashMap::new();
    }

    /// Creates a new record instance from the header fields.
    pub fn new_record(&self) -> Result<Record> {
        let mut record = Record::new();

        for field in self._list.iter() {
            record.add(field, Value::Default)?;
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
            record.add(field, value)?;
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
            field._value_type.write_value(writer, value)?;
        }
        Ok(())
    }

    /// Read header from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Reader to read from.
    fn read_from(&mut self, reader: &mut impl Read) -> Result<()> {
        // read field count
        let field_count = u8::read_from(reader)?;

        // read fields
        let mut fields = Vec::new();
        for _ in 0..field_count {
            // read field data and push into the field list
            let field = Field::read_from(reader)?;
            fields.push(field);
        }

        // save read field list
        self._list = fields;
        Ok(())
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

impl IntoIterator for Header {
    type Item = Field;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self._list.into_iter()
    }
}