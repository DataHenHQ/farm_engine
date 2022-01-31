use serde_json::{Value as JSValue, Number as JSNumber};
use anyhow::{bail, Result};

/// Represents a value.
#[derive(Debug, PartialEq)]
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

impl From<&Value> for JSValue {
    fn from(value: &Value) -> Self {
        // convert to serde_json::Value
        match value {
            Value::Default => Self::Null,
            Value::Bool(v) => Self::Bool(*v),
            Value::I8(v) => Self::Number(JSNumber::from(*v)),
            Value::I16(v) => Self::Number(JSNumber::from(*v)),
            Value::I32(v) => Self::Number(JSNumber::from(*v)),
            Value::I64(v) => Self::Number(JSNumber::from(*v)),
            Value::U8(v) => Self::Number(JSNumber::from(*v)),
            Value::U16(v) => Self::Number(JSNumber::from(*v)),
            Value::U32(v) => Self::Number(JSNumber::from(*v)),
            Value::U64(v) => Self::Number(JSNumber::from(*v)),
            Value::F32(v) => match JSNumber::from_f64((*v) as f64) {
                Some(jv) => Self::Number(jv),
                None => Self::Null
            },
            Value::F64(v) => match JSNumber::from_f64(*v) {
                Some(jv) => Self::Number(jv),
                None => Self::Null
            },
            Value::Str(v) => Self::String(v.to_string())
        }
    }
}