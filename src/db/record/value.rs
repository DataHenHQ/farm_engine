use serde_json::{Value as JSValue, Number as JSNumber, Map};
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

impl Value {
    /// Try from a JS u64 value.
    /// 
    /// # Arguments
    /// 
    /// * `value` - JS value to convert from.
    pub fn try_from_js_u64(value: JSValue) -> Result<Value> {
        match value {
            JSValue::Number(n) => {
                if !n.is_u64() {
                    bail!("can't convert number into u64");
                }
                Ok(n.as_u64().unwrap().into())
            },
            JSValue::Null => Ok(Self::Default),
            _ => bail!("can't convert from a JS value other than number")
        }
    }
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
    fn from(value: Value) -> Self {
        // convert to serde_json::Value
        match value {
            Value::Default => Self::Null,
            Value::Bool(v) => Self::Bool(v),
            Value::I8(v) => Self::Number(JSNumber::from(v)),
            Value::I16(v) => Self::Number(JSNumber::from(v)),
            Value::I32(v) => Self::Number(JSNumber::from(v)),
            Value::I64(v) => Self::Number(JSNumber::from(v)),
            Value::U8(v) => Self::Number(JSNumber::from(v)),
            Value::U16(v) => Self::Number(JSNumber::from(v)),
            Value::U32(v) => Self::Number(JSNumber::from(v)),
            Value::U64(v) => Self::Number(JSNumber::from(v)),
            Value::F32(v) => match JSNumber::from_f64((v) as f64) {
                Some(jv) => Self::Number(jv),
                None => Self::Null
            },
            Value::F64(v) => match JSNumber::from_f64(v) {
                Some(jv) => Self::Number(jv),
                None => Self::Null
            },
            Value::Str(v) => Self::String(v.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_js_u64_valid() {
        let expected = Value::U64(u64::MAX);
        match Value::try_from_js_u64(JSValue::Number(JSNumber::from(u64::MAX))) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }

        let expected = Value::Default;
        match Value::try_from_js_u64(JSValue::Null) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn try_from_js_u64_invalid() {
        let expected = "can't convert from a JS value other than number";
        match Value::try_from_js_u64(JSValue::Bool(false)) {
            Ok(v) => assert!(false, "expected an error but got: {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
        match Value::try_from_js_u64(JSValue::String("abc".to_string())) {
            Ok(v) => assert!(false, "expected an error but got: {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
        match Value::try_from_js_u64(JSValue::Array(Vec::new())) {
            Ok(v) => assert!(false, "expected an error but got: {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
        match Value::try_from_js_u64(JSValue::Object(Map::new())) {
            Ok(v) => assert!(false, "expected an error but got: {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }

        let expected = "can't convert number into u64";
        match Value::try_from_js_u64(JSValue::Number(JSNumber::from_f64(12.12f64).unwrap())) {
            Ok(v) => assert!(false, "expected an error but got float: {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn from_bool() {
        assert_eq!(Value::Bool(false), Value::from(false));
        assert_eq!(Value::Bool(true), Value::from(true));
    }

    #[test]
    fn from_i8() {
        assert_eq!(Value::I8(12i8), Value::from(12i8));
        assert_eq!(Value::I8(-12i8), Value::from(-12i8));
    }

    #[test]
    fn from_i16() {
        assert_eq!(Value::I16(122i16), Value::from(122i16));
        assert_eq!(Value::I16(-122i16), Value::from(-122i16));
    }

    #[test]
    fn from_i32() {
        assert_eq!(Value::I32(1224i32), Value::from(1224i32));
        assert_eq!(Value::I32(-1224i32), Value::from(-1224i32));
    }

    #[test]
    fn from_i64() {
        assert_eq!(Value::I64(12245i64), Value::from(12245i64));
        assert_eq!(Value::I64(-12245i64), Value::from(-12245i64));
    }

    #[test]
    fn from_u8() {
        assert_eq!(Value::U8(12u8), Value::from(12u8));
    }

    #[test]
    fn from_u16() {
        assert_eq!(Value::U16(122u16), Value::from(122u16));
    }

    #[test]
    fn from_u32() {
        assert_eq!(Value::U32(1224u32), Value::from(1224u32));
    }

    #[test]
    fn from_u64() {
        assert_eq!(Value::U64(12245u64), Value::from(12245u64));
    }

    #[test]
    fn from_f32() {
        assert_eq!(Value::F32(1224.321f32), Value::from(1224.321f32));
        assert_eq!(Value::F32(-1224.321f32), Value::from(-1224.321f32));
    }

    #[test]
    fn from_f64() {
        assert_eq!(Value::F64(12245.321f64), Value::from(12245.321f64));
        assert_eq!(Value::F64(-12245.321f64), Value::from(-12245.321f64));
    }

    #[test]
    fn from_str() {
        assert_eq!(Value::Str("foo".to_string()), Value::from("foo"));
    }

    #[test]
    fn try_from_js_bool() {
        let expected = Value::Bool(false);
        match Value::try_from(JSValue::Bool(false)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }

        let expected = Value::Bool(true);
        match Value::try_from(JSValue::Bool(true)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn try_from_js_i64_number() {
        let expected = Value::I64(43i64);
        match Value::try_from(JSValue::Number(JSNumber::from(43i64))) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }

        let expected = Value::I64(-43i64);
        match Value::try_from(JSValue::Number(JSNumber::from(-43i64))) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }

        let expected = Value::I64(43i64);
        match Value::try_from(JSValue::Number(JSNumber::from(43u64))) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn try_from_js_u64_number() {
        let expected = Value::U64(u64::MAX);
        match Value::try_from(JSValue::Number(JSNumber::from(u64::MAX))) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn try_from_js_f64_number() {
        let expected = Value::F64(45.12f64);
        match Value::try_from(JSValue::Number(JSNumber::from_f64(45.12f64).unwrap())) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn try_from_js_str() {
        let expected = Value::Str("bar".to_string());
        match Value::try_from(JSValue::String("bar".to_string())) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn try_from_js_null() {
        let expected = Value::Default;
        match Value::try_from(JSValue::Null) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn try_from_js_array() {
        let expected = "can't convert from array";
        match Value::try_from(JSValue::Array(Vec::new())) {
            Ok(v) => assert!(false, "expected an error but got: {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn try_from_js_object() {
        let expected = "can't convert from object";
        match Value::try_from(JSValue::Object(Map::new())) {
            Ok(v) => assert!(false, "expected an error but got: {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn js_from_default() {
        assert_eq!(JSValue::Null, JSValue::from(Value::Default));
    }

    #[test]
    fn js_from_bool() {
        assert_eq!(JSValue::Bool(false), JSValue::from(Value::Bool(false)));
        assert_eq!(JSValue::Bool(true), JSValue::from(Value::Bool(true)));
    }

    #[test]
    fn js_from_i8() {
        assert_eq!(JSValue::Number(JSNumber::from(4i8)), JSValue::from(Value::I8(4i8)));
    }

    #[test]
    fn js_from_i16() {
        assert_eq!(JSValue::Number(JSNumber::from(4i16)), JSValue::from(Value::I16(4i16)));
    }

    #[test]
    fn js_from_i32() {
        assert_eq!(JSValue::Number(JSNumber::from(4i32)), JSValue::from(Value::I32(4i32)));
    }

    #[test]
    fn js_from_i64() {
        assert_eq!(JSValue::Number(JSNumber::from(4i64)), JSValue::from(Value::I64(4i64)));
    }

    #[test]
    fn js_from_u8() {
        assert_eq!(JSValue::Number(JSNumber::from(4u8)), JSValue::from(Value::U8(4u8)));
    }

    #[test]
    fn js_from_u16() {
        assert_eq!(JSValue::Number(JSNumber::from(4u16)), JSValue::from(Value::U16(4u16)));
    }

    #[test]
    fn js_from_u32() {
        assert_eq!(JSValue::Number(JSNumber::from(4u32)), JSValue::from(Value::U32(4u32)));
    }

    #[test]
    fn js_from_u64() {
        assert_eq!(JSValue::Number(JSNumber::from(4u64)), JSValue::from(Value::U64(4u64)));
    }

    #[test]
    fn js_from_f32() {
        assert_eq!(JSValue::Number(JSNumber::from_f64(4f64).unwrap()), JSValue::from(Value::F32(4f32)));
    }

    #[test]
    fn js_from_f64() {
        assert_eq!(JSValue::Number(JSNumber::from_f64(4f64).unwrap()), JSValue::from(Value::F64(4f64)));
    }

    #[test]
    fn js_from_str() {
        assert_eq!(JSValue::String("foo".to_string()), JSValue::from(Value::Str("foo".to_string())));
    }
}