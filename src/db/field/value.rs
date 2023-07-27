use serde::ser::{Serialize, Serializer};
use serde_json::{Value as JSValue, Number as JSNumber};
use anyhow::{bail, Result};
use std::cmp::Ordering;

/// Represents a value.
#[derive(Debug, Clone)]
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

impl std::fmt::Display for Value{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { 
        write!(f, "{}", match self {
            Self::Default => "".to_string(),
            Self::Bool(v) => v.to_string(),
            Self::I8(v) => v.to_string(),
            Self::I16(v) => v.to_string(),
            Self::I32(v) => v.to_string(),
            Self::I64(v) => v.to_string(),
            Self::U8(v) => v.to_string(),
            Self::U16(v) => v.to_string(),
            Self::U32(v) => v.to_string(),
            Self::U64(v) => v.to_string(),
            Self::F32(v) => v.to_string(),
            Self::F64(v) => v.to_string(),
            Self::Str(v) => v.to_string()
        })
    }
}

macro_rules! impl_partial_eq_number_to_value {
    ($from:ty, $to:ident) => {
        impl PartialEq<$from> for Value {
            fn eq(&self, other: &$from) -> bool {
                if let Value::$to(v) = self {
                    return v.eq(other);
                }
                let other = (*other as i64);
                match self {
                    Self::I8(v) => (*v as i64)== other,
                    Self::I16(v) => (*v as i64)== other,
                    Self::I32(v) => (*v as i64)== other,
                    Self::I64(v) => *v== other,
                    Self::U8(v) => (*v as i64)== other,
                    Self::U16(v) => (*v as i64)== other,
                    Self::U32(v) => (*v as i64)== other,
                    Self::U64(v) => (*v as i128)== (other as i128),
                    Self::F32(v) => (*v as f64)== (other as f64),
                    Self::F64(v) => (*v as f64)== (other as f64),                    
                    _ => false
                }
            }
        }
    };
}

impl_partial_eq_number_to_value!(i8, I8);
impl_partial_eq_number_to_value!(i16, I16);
impl_partial_eq_number_to_value!(i32, I32);
impl_partial_eq_number_to_value!(i64, I64);
impl_partial_eq_number_to_value!(u8, U8);
impl_partial_eq_number_to_value!(u16, U16);
impl_partial_eq_number_to_value!(u32, U32);
impl_partial_eq_number_to_value!(u64, U64);
impl_partial_eq_number_to_value!(f32, F32);
impl_partial_eq_number_to_value!(f64, F64);

impl PartialEq<String> for Value {
    fn eq(&self, other: &String) -> bool {
        if let Value::Str(v) = self {
            return v.eq(other);
        }
        false
    }
}

impl PartialEq<str> for Value {
    fn eq(&self, other: &str) -> bool {
        if let Value::Str(v) = self {
            return v.eq(other);
        }
        false
    }
}


impl PartialEq<bool> for Value {
    fn eq(&self, other: &bool) -> bool {
        if let Value::Bool(v) = self {
            return v.eq(other);
        }
        false
    }
}


impl PartialEq<Value> for Value{
    fn eq(&self, other: &Value) -> bool {
        match self {
            Self::I8(v) => other.eq(v),
            Self::I16(v) => other.eq(v),
            Self::I32(v) => other.eq(v),
            Self::I64(v) => other.eq(v),
            Self::U8(v) => other.eq(v),
            Self::U16(v) => other.eq(v),
            Self::U32(v) => other.eq(v),
            Self::U64(v) => other.eq(v),
            Self::F32(v) => other.eq(v),
            Self::F64(v) => other.eq(v),
            Self::Str(v) => other.eq(v),
            Self::Bool(v) => other.eq(v),
            Self::Default => {
                if let Self::Default = other {
                    return true;
                }
                false
            },

        }
    }
}

macro_rules! impl_partial_cmp_number_to_value {
    ($from:ty, $to:ident) => {
        impl PartialOrd<$from> for Value {
            fn partial_cmp(&self, other: &$from) -> Option<Ordering>{
                if let Value::$to(v) = self {
                    return v.partial_cmp(other);
                }
                let other = (*other as i64);
                match self {                    
                    Self::I8(v) => (*v as i64).partial_cmp(&other),
                    Self::I16(v) => (*v as i64).partial_cmp(&other),
                    Self::I32(v) => (*v as i64).partial_cmp(&other),
                    Self::I64(v) => (*v).partial_cmp(&other),
                    Self::U8(v) => (*v as i64).partial_cmp(&other),
                    Self::U16(v) => (*v as i64).partial_cmp(&other),
                    Self::U32(v) => (*v as i64).partial_cmp(&other),
                    Self::U64(v) => (*v as i128).partial_cmp(&(other as i128)),
                    Self::F32(v) => (*v as f64).partial_cmp(&(other as f64)),
                    Self::F64(v) => (*v as f64).partial_cmp(&(other as f64)),                      
                    _ => None
                }
            }
        }
    };
}

impl_partial_cmp_number_to_value!(i8, I8);
impl_partial_cmp_number_to_value!(i16, I16);
impl_partial_cmp_number_to_value!(i32, I32);
impl_partial_cmp_number_to_value!(i64, I64);
impl_partial_cmp_number_to_value!(u8, U8);
impl_partial_cmp_number_to_value!(u16, U16);
impl_partial_cmp_number_to_value!(u32, U32);
impl_partial_cmp_number_to_value!(u64, U64);
impl_partial_cmp_number_to_value!(f32, F32);
impl_partial_cmp_number_to_value!(f64, F64);

impl PartialOrd<String> for Value {
    fn partial_cmp(&self, other: &String) -> Option<Ordering> {
        if let Value::Str(v) = self {
            return v.partial_cmp(other);
        }
        None
    }
}

impl PartialOrd<str> for Value {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        if let Value::Str(v) = self {
            return (v as &str).partial_cmp(other);
        }
        None
    }
}


impl PartialOrd<bool> for Value {
    fn partial_cmp(&self, other: &bool) -> Option<Ordering> {
        if let Value::Bool(v) = self {
            return v.partial_cmp(other);
        }
        None
    }
}

impl PartialOrd<Value> for Value{
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match self {
            Self::I8(v) => other.partial_cmp(v),
            Self::I16(v) => other.partial_cmp(v),
            Self::I32(v) => other.partial_cmp(v),
            Self::I64(v) => other.partial_cmp(v),
            Self::U8(v) => other.partial_cmp(v),
            Self::U16(v) => other.partial_cmp(v),
            Self::U32(v) => other.partial_cmp(v),
            Self::U64(v) => other.partial_cmp(v),
            Self::F32(v) => other.partial_cmp(v),
            Self::F64(v) => other.partial_cmp(v),
            Self::Str(v) => other.partial_cmp(v),
            Self::Bool(v) => other.partial_cmp(v),
            Self::Default => {
                if let Self::Default = other {
                    return Some(Ordering::Equal);
                }
                None
            },

        }
    }
}

macro_rules! impl_convert_to_native {
    ($from:ty, $to:ident) => {
        impl From<$from> for Value {
            fn from(v: $from) -> Self {
                Value::$to(v)
            }
        }
        
        impl From<$from> for &Value {
            fn from(v: $from) -> Self {
                &v.into()
            }
        }
    }
}

// implement conversions for all types to native value
impl_convert_to_native!(bool, Bool);
impl_convert_to_native!(i8, I8);
impl_convert_to_native!(i16, I16);
impl_convert_to_native!(i32, I32);
impl_convert_to_native!(i64, I64);
impl_convert_to_native!(u8, U8);
impl_convert_to_native!(u16, U16);
impl_convert_to_native!(u32, U32);
impl_convert_to_native!(u64, U64);
impl_convert_to_native!(f32, F32);
impl_convert_to_native!(f64, F64);

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Str(v.to_string())
    }
}

impl From<&str> for &Value {
    fn from(v: &str) -> Self {
        &v.into()
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

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Default => serializer.serialize_none(),
            Self::Bool(v) => serializer.serialize_bool(*v),
            Self::I8(v) => serializer.serialize_i8(*v),
            Self::I16(v) => serializer.serialize_i16(*v),
            Self::I32(v) => serializer.serialize_i32(*v),
            Self::I64(v) => serializer.serialize_i64(*v),
            Self::U8(v) => serializer.serialize_u8(*v),
            Self::U16(v) => serializer.serialize_u16(*v),
            Self::U32(v) => serializer.serialize_u32(*v),
            Self::U64(v) => serializer.serialize_u64(*v),
            Self::F32(v) => serializer.serialize_f32(*v),
            Self::F64(v) => serializer.serialize_f64(*v),
            Self::Str(v) => serializer.serialize_str(v)
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map as JSMap};

    #[test]
    fn display() {
        assert_eq!("", Value::Default.to_string());
        assert_eq!("true", Value::Bool(true).to_string());
        assert_eq!("false", Value::Bool(false).to_string());
        assert_eq!("5", Value::I8(5i8).to_string());
        assert_eq!("-5", Value::I8(-5i8).to_string());
        assert_eq!("11", Value::I16(11i16).to_string());
        assert_eq!("-11", Value::I16(-11i16).to_string());
        assert_eq!("23", Value::I32(23i32).to_string());
        assert_eq!("-23", Value::I32(-23i32).to_string());
        assert_eq!("76", Value::I64(76i64).to_string());
        assert_eq!("-76", Value::I64(-76i64).to_string());
        assert_eq!("43", Value::U8(43u8).to_string());
        assert_eq!("54", Value::U16(54u16).to_string());
        assert_eq!("87", Value::U32(87u32).to_string());
        assert_eq!("98", Value::U64(98u64).to_string());
        assert_eq!("123.234", Value::F32(123.234f32).to_string());
        assert_eq!("-123.234", Value::F32(-123.234f32).to_string());
        assert_eq!("345.852", Value::F64(345.852).to_string());
        assert_eq!("-345.852", Value::F64(-345.852).to_string());
        assert_eq!("hello", Value::Str("hello".to_string()).to_string());
    }

    #[test]
    fn serialize_default() {
        let expected = "null";
        match serde_json::to_string(&Value::Default) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_i8() {
        let expected = "12";
        match serde_json::to_string(&Value::I8(12i8)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_i16() {
        let expected = "12";
        match serde_json::to_string(&Value::I16(12i16)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_i32() {
        let expected = "12";
        match serde_json::to_string(&Value::I32(12i32)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_i64() {
        let expected = "12";
        match serde_json::to_string(&Value::I64(12i64)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_u8() {
        let expected = "12";
        match serde_json::to_string(&Value::U8(12u8)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_u16() {
        let expected = "12";
        match serde_json::to_string(&Value::U16(12u16)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_u32() {
        let expected = "12";
        match serde_json::to_string(&Value::U32(12u32)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_u64() {
        let expected = "12";
        match serde_json::to_string(&Value::U64(12u64)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_f32() {
        let expected = "12.22";
        match serde_json::to_string(&Value::F32(12.22f32)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_f64() {
        let expected = "12.44";
        match serde_json::to_string(&Value::F64(12.44f64)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn serialize_str() {
        let expected = "\"hello\"";
        match serde_json::to_string(&Value::Str("hello".to_string())) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

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
        match Value::try_from_js_u64(JSValue::Object(JSMap::new())) {
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
        match Value::try_from(JSValue::Object(JSMap::new())) {
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

    #[test]
    fn js_from_ref_default() {
        assert_eq!(JSValue::Null, JSValue::from(&Value::Default));
    }

    #[test]
    fn js_from_ref_bool() {
        assert_eq!(JSValue::Bool(false), JSValue::from(&Value::Bool(false)));
        assert_eq!(JSValue::Bool(true), JSValue::from(&Value::Bool(true)));
    }

    #[test]
    fn js_from_ref_i8() {
        assert_eq!(JSValue::Number(JSNumber::from(4i8)), JSValue::from(&Value::I8(4i8)));
    }

    #[test]
    fn js_from_ref_i16() {
        assert_eq!(JSValue::Number(JSNumber::from(4i16)), JSValue::from(&Value::I16(4i16)));
    }

    #[test]
    fn js_from_ref_i32() {
        assert_eq!(JSValue::Number(JSNumber::from(4i32)), JSValue::from(&Value::I32(4i32)));
    }

    #[test]
    fn js_from_ref_i64() {
        assert_eq!(JSValue::Number(JSNumber::from(4i64)), JSValue::from(&Value::I64(4i64)));
    }

    #[test]
    fn js_from_ref_u8() {
        assert_eq!(JSValue::Number(JSNumber::from(4u8)), JSValue::from(&Value::U8(4u8)));
    }

    #[test]
    fn js_from_ref_u16() {
        assert_eq!(JSValue::Number(JSNumber::from(4u16)), JSValue::from(&Value::U16(4u16)));
    }

    #[test]
    fn js_from_ref_u32() {
        assert_eq!(JSValue::Number(JSNumber::from(4u32)), JSValue::from(&Value::U32(4u32)));
    }

    #[test]
    fn js_from_ref_u64() {
        assert_eq!(JSValue::Number(JSNumber::from(4u64)), JSValue::from(&Value::U64(4u64)));
    }

    #[test]
    fn js_from_ref_f32() {
        assert_eq!(JSValue::Number(JSNumber::from_f64(4f64).unwrap()), JSValue::from(&Value::F32(4f32)));
    }

    #[test]
    fn js_from_ref_f64() {
        assert_eq!(JSValue::Number(JSNumber::from_f64(4f64).unwrap()), JSValue::from(&Value::F64(4f64)));
    }

    #[test]
    fn js_from_ref_str() {
        assert_eq!(JSValue::String("foo".to_string()), JSValue::from(&Value::Str("foo".to_string())));
    }
}