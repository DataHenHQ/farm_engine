pub mod header;

use std::collections::HashMap;
use serde_json::{Value as JSValue, Number as JSNumber};
use anyhow::{bail, Result};
pub use header::Header;
use header::Field;

/// Represents a value.
pub enum Value {
    Default,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),

    /// Represents a string with a max size.
    Str(String)
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Value::I8(v)
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Value::I16(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::I32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::I64(v)
    }
}

impl From<u8> for Value {
    fn from(v: u8) -> Self {
        Value::U8(v)
    }
}

impl From<u16> for Value {
    fn from(v: u16) -> Self {
        Value::U16(v)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::U32(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::U64(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::F32(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::F64(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Str(v.to_string())
    }
}

impl TryFrom<JSValue> for Value {
    type Error = anyhow::Error;

    fn try_from(v: JSValue) -> Result<Self> {
        match v {
            JSValue::Bool(v) => Ok(v.into()),
            JSValue::Number(n) => {
                if n.is_i64() {
                    return Ok(n.as_i64().unwrap().into());
                }
                if n.is_u64() {
                    return Ok(n.as_u64().unwrap().into());
                }
                if n.is_f64() {
                    return Ok(n.as_f64().unwrap().into());
                }
                bail!("unknown number type")
            },
            JSValue::String(s) => Ok(s.as_str().into()),
            JSValue::Null => Ok(Self::Default),
            JSValue::Array(_) => bail!("can't convert from array"),
            JSValue::Object(_) => bail!("can't convert from object")
        }
    }
}

impl From<Value> for JSValue {
    fn from(v: Value) -> Self {
        // convert to serde_json::Value
        match v {
            Value::Bool(v) => Self::Bool(v),
            Value::I8(v) => Self::Number(JSNumber::from(v)),
            Value::I16(v) => Self::Number(JSNumber::from(v)),
            Value::I32(v) => Self::Number(JSNumber::from(v)),
            Value::I64(v) => Self::Number(JSNumber::from(v)),
            Value::U8(v) => Self::Number(JSNumber::from(v)),
            Value::U16(v) => Self::Number(JSNumber::from(v)),
            Value::U32(v) => Self::Number(JSNumber::from(v)),
            Value::U64(v) => Self::Number(JSNumber::from(v)),
            Value::F32(v) => match JSNumber::from_f64(v as f64) {
                Some(jv) => Self::Number(jv),
                None => Self::Null
            },
            Value::F64(v) => match JSNumber::from_f64(v) {
                Some(jv) => Self::Number(jv),
                None => Self::Null
            },
            Value::Str(v) => Self::String(v)
        }
    }
}

/// Represents a data record.
pub struct Record {
    _list: Vec<Value>,
    _map: HashMap<String, usize>
}

impl Record {
    // Creates a new record.
    pub fn new() -> Self {
        Self{
            _list: Vec::new(),
            _map: HashMap::new()
        }
    }

    /// Add a new value.
    /// 
    /// # Arguments
    /// 
    /// * `field` - Field config.
    pub fn add(&mut self, field: &Field, value: Value) -> Result<&Self> {
        let field_name = field.get_name().to_string();

        // avoid duplicated fields
        if let Some(_) = self._map.get(&field_name) {
            bail!("field \"{}\" already exists within the header", field_name);
        }

        // validate value
        if !field.get_type().is_valid(&value) {
            bail!("invalid value, expected {:?}", field.get_type())
        }

        // add field
        self._list.push(value);
        self._map.insert(field_name, self._list.len()-1);
        
        Ok(self)
    }

    /// Set a field value.
    /// 
    /// # Arguments
    /// 
    /// * `field` - Field config.
    /// * `value` - New value.
    pub fn set(&mut self, field: &Field, value: Value) -> Result<()> {
        // make sure field type and value type match
        if !field.get_type().is_valid(&value) {
            bail!("invalid value, expected {:?}", field.get_type())
        }

        // update value
        let index = match self._map.get(field.get_name()) {
            Some(v) => *v,
            None => bail!("can't update: unknown field \"{}\"", field.get_name())
        };
        self._list[index] = value;
        Ok(())
    }

    /// Get a value by name.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        let index = match self._map.get(name) {
            Some(v) => *v,
            None => return None
        };
        Some(&self._list[index])
    }

    /// Get a value by it's index.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Value index.
    pub fn get_by_index(&self, index: usize) -> Option<&Value> {
        if self._list.len() > index {
            return Some(&self._list[index]);
        }
        None
    }

    /// Returns the number of fields on the header.
    pub fn len(&self) -> usize {
        self._list.len()
    }
}