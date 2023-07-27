use std::io::{Read, Write};
use anyhow::{bail, Result};
use uuid::Uuid;
use crate::error::ParseError;

pub trait ByteSized: Sized {
    /// The size of this class instance in bytes.
    const BYTES: usize;
}

macro_rules! impl_byte_sized {
    ($type:ty, $bits:expr) => {
        impl ByteSized for $type {
            const BYTES: usize = ($bits / 8) as usize;
        }
    }
}

// implement `BYTES` constant on numeric types and boolean
impl_byte_sized!(bool, 8);
impl_byte_sized!(u64, u64::BITS);
impl_byte_sized!(u32, u32::BITS);
impl_byte_sized!(u16, u16::BITS);
impl_byte_sized!(u8, u8::BITS);
impl_byte_sized!(i64, i64::BITS);
impl_byte_sized!(i32, i32::BITS);
impl_byte_sized!(i16, i16::BITS);
impl_byte_sized!(i8, i8::BITS);
impl_byte_sized!(f64, 64);
impl_byte_sized!(f32, 32);
impl_byte_sized!(Uuid, 128);

pub trait FromByteSlice: ByteSized {
    /// Creates a value from its representation as bytes from a byte buffer.
    /// 
    /// # Arguments
    /// 
    /// * `buf` - Byte buffer.
    fn from_byte_slice(buf: &[u8]) -> Result<Self>;
}

pub trait ReadFrom: Sized {
    /// Create an instance from a reader contents.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader. 
    fn read_from(reader: &mut impl Read) -> Result<Self>;
}

impl FromByteSlice for bool {
    fn from_byte_slice(buf: &[u8]) -> Result<Self> {
        // validate value size
        if buf.len() != Self::BYTES {
            bail!(ParseError::InvalidSize);
        }

        Ok(match buf[0] {
            0 => false,
            1 => true,
            _ => bail!(ParseError::InvalidValue)
        })
    }
}

impl ReadFrom for bool {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        // read and convert bytes into the type value
        let mut buf = [0u8; Self::BYTES];
        reader.read_exact(&mut buf)?;
        return match buf[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => bail!(ParseError::InvalidValue)
        }
    }
}

macro_rules! impl_from_byte_reader {
    ($type:ty, $fn:ident) => {
        impl FromByteSlice for $type {
            fn from_byte_slice(buf: &[u8]) -> Result<Self> {
                // validate buf size
                if buf.len() != Self::BYTES {
                    bail!(ParseError::InvalidSize);
                }

                // convert bytes into the type value
                let mut bytes = [0u8; Self::BYTES];
                bytes.copy_from_slice(buf);
                Ok(<$type>::$fn(bytes))
            }
        }

        impl ReadFrom for $type {
            fn read_from(reader: &mut impl Read) -> Result<Self> {
                // read and convert bytes into the type value
                let mut buf = [0u8; Self::BYTES];
                reader.read_exact(&mut buf)?;
                Ok(<$type>::$fn(buf))
            }
        }
    }
}

// implement `from_byte_slice` function on numeric types
impl_from_byte_reader!(u64, from_be_bytes);
impl_from_byte_reader!(u32, from_be_bytes);
impl_from_byte_reader!(u16, from_be_bytes);
impl_from_byte_reader!(u8, from_be_bytes);
impl_from_byte_reader!(i64, from_be_bytes);
impl_from_byte_reader!(i32, from_be_bytes);
impl_from_byte_reader!(i16, from_be_bytes);
impl_from_byte_reader!(i8, from_be_bytes);
impl_from_byte_reader!(f64, from_be_bytes);
impl_from_byte_reader!(f32, from_be_bytes);
impl_from_byte_reader!(Uuid, from_bytes);

pub trait WriteAsBytes: ByteSized {
    /// Write the value representation as bytes into a buffer.
    /// 
    /// # Arguments
    /// 
    /// * `buf` - Byte buffer.
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()>;
}

pub trait WriteTo {
    /// Write instance value as bytes into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    fn write_to(&self, writer: &mut impl Write) -> Result<()>;
}

impl WriteAsBytes for bool {
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()> {
        // validate value size
        if buf.len() != Self::BYTES {
            bail!(ParseError::InvalidSize);
        }

        // save value as bytes
        buf[0] = (*self).into();
        Ok(())
    }
}

 
impl WriteTo for bool {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        let buf: [u8; 1] = [(*self).into()];
        writer.write_all(&buf)?;
        Ok(())
    }
}

impl WriteAsBytes for Uuid {
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()> {
        // validate value size
        if buf.len() != Self::BYTES {
            bail!(ParseError::InvalidSize);
        }

        // save value as bytes
        buf.copy_from_slice(self.as_bytes());

        Ok(())
    }
}

impl WriteTo for Uuid {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

macro_rules! impl_write_as_bytes {
    ($t:ty, $fn:ident) => {
        impl WriteAsBytes for $t {
            fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()> {
                // validate value size
                if buf.len() != Self::BYTES {
                    bail!(ParseError::InvalidSize);
                }

                // save value as bytes
                buf.copy_from_slice(&self.$fn());

                Ok(())
            }
        }

        impl WriteTo for $t {
            fn write_to(&self, writer: &mut impl Write) -> Result<()> {
                writer.write_all(&self.$fn())?;
                Ok(())
            }
        }
    };
}

// implement `write_as_bytes` function on numeric types
impl_write_as_bytes!(u64, to_be_bytes);
impl_write_as_bytes!(u32, to_be_bytes);
impl_write_as_bytes!(u16, to_be_bytes);
impl_write_as_bytes!(u8, to_be_bytes);
impl_write_as_bytes!(i64, to_be_bytes);
impl_write_as_bytes!(i32, to_be_bytes);
impl_write_as_bytes!(i16, to_be_bytes);
impl_write_as_bytes!(i8, to_be_bytes);
impl_write_as_bytes!(f64, to_be_bytes);
impl_write_as_bytes!(f32, to_be_bytes);

pub trait LoadFrom {
    /// Loads data into the instance from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    fn load_from(&mut self, reader: &mut impl Read) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_byte_size() {
        assert_eq!(1, bool::BYTES);
        assert_eq!(1, bool::BYTES);
    }
    
    #[test]
    fn u64_byte_size() {
        assert_eq!(8, u64::BYTES);
    }

    #[test]
    fn u32_byte_size() {
        assert_eq!(4, u32::BYTES);
    }

    #[test]
    fn u16_byte_size() {
        assert_eq!(2, u16::BYTES);
    }

    #[test]
    fn u8_byte_size() {
        assert_eq!(1, u8::BYTES);
    }

    #[test]
    fn i64_byte_size() {
        assert_eq!(8, i64::BYTES);
    }

    #[test]
    fn i32_byte_size() {
        assert_eq!(4, i32::BYTES);
    }

    #[test]
    fn i16_byte_size() {
        assert_eq!(2, i16::BYTES);
    }

    #[test]
    fn i8_byte_size() {
        assert_eq!(1, i8::BYTES);
    }

    #[test]
    fn bool_from_byte_slice() {
        let expected = false;
        match bool::from_byte_slice(&[0u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got an error: {:?}", expected, e)
        };
        let expected = true;
        match bool::from_byte_slice(&[1u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got an error: {:?}", expected, e)
        };
        match bool::from_byte_slice(&[3u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidValue but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidValue => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidValue but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidValue but got error: {:?}", ex)
            }
        };
        match bool::from_byte_slice(&[0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn bool_read_from() {
        let expected = false;
        match bool::read_from(&mut (&[0u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got an error: {:?}", expected, e)
        };
        let expected = true;
        match bool::read_from(&mut (&[1u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got an error: {:?}", expected, e)
        };
        match bool::read_from(&mut (&[4u8] as &[u8])) {
            Ok(v) => assert!(false, "expected ParseError::InvalidValue but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidValue => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidValue but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidValue but got error: {:?}", ex)
            }
        };
        let expected = false;
        match bool::read_from(&mut (&[0u8, 0u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got an error: {:?}", expected, e)
        };
        let expected = true;
        match bool::read_from(&mut (&[1u8, 0u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got an error: {:?}", expected, e)
        };
    }

    #[test]
    fn bool_false_on_write_as_bytes() {
        let mut buf = [0u8];
        let expected = [0u8];
        match false.write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn bool_true_on_write_as_bytes() {
        let mut buf = [0u8];
        let expected = [1u8];
        match true.write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn bool_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8];
        match false.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn bool_false_on_write_to() {
        let mut buf = [0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [0u8];
        match false.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn bool_true_on_write_to() {
        let mut buf = [0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [1u8];
        match true.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn bool_buf_size_on_write_to() {
        let mut buf = [0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [0u8, 0u8];
        match false.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [1u8, 0u8];
        match true.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i8_from_byte_slice() {
        let expected = 96i8;
        match i8::from_byte_slice(&[96u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match i8::from_byte_slice(&[0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn i8_read_from() {
        let expected = 101i8;
        match i8::read_from(&mut (&[101u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = -87i8;
        match i8::read_from(&mut (&[169u8, 53u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i8_write_as_bytes() {
        let mut buf = [0u8];
        let expected = [154u8];
        match (-102i8).write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i8_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8];
        match 76i8.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn i8_on_write_to() {
        let mut buf = [0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [24u8];
        match 24i8.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [202u8, 0u8];
        match (-54i8).write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i16_from_byte_slice() {
        let expected = 24599i16;
        match i16::from_byte_slice(&[96u8, 23u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match i16::from_byte_slice(&[0u8, 0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn i16_read_from() {
        let expected = 25932i16;
        match i16::read_from(&mut (&[101u8, 76u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = -5973i16;
        match i16::read_from(&mut (&[232u8, 171u8, 12u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i16_write_as_bytes() {
        let mut buf = [0u8, 0u8];
        let expected = [216u8, 6u8];
        match (-10234i16).write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i16_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8];
        match 7634i16.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn i16_on_write_to() {
        let mut buf = [0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [94u8, 170u8];
        match 24234i16.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [234u8, 165u8, 0u8];
        match (-5467i16).write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i32_from_byte_slice() {
        let expected = 1612144144i32;
        match i32::from_byte_slice(&[96u8, 23u8, 94u8, 16u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match i32::from_byte_slice(&[0u8, 0u8, 0u8, 0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn i32_read_from() {
        let expected = 1699488309i32;
        match i32::read_from(&mut (&[101u8, 76u8, 34u8, 53u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = -391449643i32;
        match i32::read_from(&mut (&[232u8, 170u8, 243u8, 213u8, 98u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i32_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8];
        let expected = [194u8, 254u8, 253u8, 48u8];
        match (-1023476432i32).write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i32_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8];
        match 763123434i32.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn i32_on_write_to() {
        let mut buf = [0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [14u8, 113u8, 187u8, 81u8];
        match 242334545i32.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [252u8, 189u8, 180u8, 28u8, 0u8];
        match (-54676452i32).write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i64_from_byte_slice() {
        let expected = 6924106375311862602i64;
        match i64::from_byte_slice(&[96u8, 23u8, 94u8, 16u8, 23u8, 123u8, 43u8, 74u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match i64::from_byte_slice(&[0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn i64_read_from() {
        let expected = 7299246708500139070i64;
        match i64::read_from(&mut (&[101u8, 76u8, 34u8, 53u8, 84u8, 23u8, 12u8, 62u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = -1681263416360839989i64;
        match i64::read_from(&mut (&[232u8, 170u8, 243u8, 212u8, 157u8, 243u8, 212u8, 203u8, 17u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i64_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let expected = [241u8, 203u8, 225u8, 155u8, 172u8, 106u8, 6u8, 236u8];
        match (-1023476431567845652i64).write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn i64_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        match 7634642314545343i64.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn i64_on_write_to() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [3u8, 92u8, 241u8, 252u8, 56u8, 24u8, 123u8, 126u8];
        match 242334545546345342i64.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [255u8, 61u8, 192u8, 14u8, 71u8, 147u8, 51u8, 22u8, 0u8];
        match (-54676452895673578i64).write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u8_from_byte_slice() {
        let expected = 96u8;
        match u8::from_byte_slice(&[96u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match u8::from_byte_slice(&[0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn u8_read_from() {
        let expected = 101u8;
        match u8::read_from(&mut (&[101u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = 87u8;
        match u8::read_from(&mut (&[87u8, 53u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u8_write_as_bytes() {
        let mut buf = [0u8];
        let expected = [102u8];
        match 102u8.write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u8_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8];
        match 76u8.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn u8_on_write_to() {
        let mut buf = [0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [24u8];
        match 24u8.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [54u8, 0u8];
        match 54u8.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u16_from_byte_slice() {
        let expected = 24599u16;
        match u16::from_byte_slice(&[96u8, 23u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match u16::from_byte_slice(&[0u8, 0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn u16_read_from() {
        let expected = 25932u16;
        match u16::read_from(&mut (&[101u8, 76u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = 5973u16;
        match u16::read_from(&mut (&[23u8, 85u8, 12u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u16_write_as_bytes() {
        let mut buf = [0u8, 0u8];
        let expected = [39u8, 250u8];
        match 10234u16.write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u16_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8];
        match 7634u16.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn u16_on_write_to() {
        let mut buf = [0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [94u8, 170u8];
        match 24234u16.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [21u8, 91u8, 0u8];
        match 5467u16.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u32_from_byte_slice() {
        let expected = 1612144144u32;
        match u32::from_byte_slice(&[96u8, 23u8, 94u8, 16u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match u32::from_byte_slice(&[0u8, 0u8, 0u8, 0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn u32_read_from() {
        let expected = 1699488309u32;
        match u32::read_from(&mut (&[101u8, 76u8, 34u8, 53u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = 391449643u32;
        match u32::read_from(&mut (&[23u8, 85u8, 12u8, 43u8, 98u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u32_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8];
        let expected = [61u8, 1u8, 2u8, 208u8];
        match 1023476432u32.write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u32_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8];
        match 763123434u32.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn u32_on_write_to() {
        let mut buf = [0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [14u8, 113u8, 187u8, 81u8];
        match 242334545u32.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [3u8, 66u8, 75u8, 228u8, 0u8];
        match 54676452u32.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u64_from_byte_slice() {
        let expected = 6924106375311862602u64;
        match u64::from_byte_slice(&[96u8, 23u8, 94u8, 16u8, 23u8, 123u8, 43u8, 74u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match u64::from_byte_slice(&[0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn u64_read_from() {
        let expected = 7299246708500139070u64;
        match u64::read_from(&mut (&[101u8, 76u8, 34u8, 53u8, 84u8, 23u8, 12u8, 62u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = 1681263416360839989u64;
        match u64::read_from(&mut (&[23u8, 85u8, 12u8, 43u8, 98u8, 12u8, 43u8, 53u8, 17u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u64_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let expected = [14u8, 52u8, 30u8, 100u8, 83u8, 149u8, 249u8, 20u8];
        match 1023476431567845652u64.write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn u64_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        match 7634642314545343u64.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn u64_on_write_to() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [3u8, 92u8, 241u8, 252u8, 56u8, 24u8, 123u8, 126u8];
        match 242334545546345342u64.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [7u8, 150u8, 126u8, 118u8, 170u8, 3u8, 60u8, 234u8, 0u8];
        match 546763452895673578u64.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn f32_from_byte_slice() {
        let expected = 16121.44144f32;
        match f32::from_byte_slice(&[70u8, 123u8, 229u8, 196]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match f32::from_byte_slice(&[0u8, 0u8, 0u8, 0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn f32_read_from() {
        let expected = 169948.8309f32;
        match f32::read_from(&mut (&[72u8, 37u8, 247u8, 53u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = -39144.9643f32;
        match f32::read_from(&mut (&[199u8, 24u8, 232u8, 247u8, 30u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn f32_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8];
        let expected = [201u8, 121u8, 223u8, 71u8];
        match (-1023476.432f32).write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn f32_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8];
        match 763123.434f32.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn f32_on_write_to() {
        let mut buf = [0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [72u8, 108u8, 167u8, 163u8];
        match 242334.545f32.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [199u8, 85u8, 148u8, 116u8, 0];
        match (-54676.452f32).write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn f64_from_byte_slice() {
        let expected = 69241063753.11862602f64;
        match f64::from_byte_slice(&[66u8, 48u8, 31u8, 22u8, 201u8, 73u8, 30u8, 94u8]) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        match f64::from_byte_slice(&[0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn f64_read_from() {
        let expected = 729924670.8500139070f64;
        match f64::read_from(&mut (&[65u8, 197u8, 192u8, 226u8, 31u8, 108u8, 205u8, 65u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
        let expected = -168126341636.0839989f64;
        match f64::read_from(&mut (&[194u8, 67u8, 146u8, 142u8, 49u8, 2u8, 10u8, 192u8, 54u8] as &[u8])) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn f64_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let expected = [194u8, 55u8, 212u8, 101u8, 25u8, 20u8, 200u8, 217u8];
        match (-102347643156.7845652f64).write_as_bytes(&mut buf) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }

    #[test]
    fn f64_invalid_buf_size_on_write_as_bytes() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        match 76346423145.45343f64.write_as_bytes(&mut buf) {
            Ok(v) => assert!(false, "expected ParseError::InvalidSize but got {:?}", v),
            Err(e) => match e.downcast() {
                Ok(ex) => match ex {
                    ParseError::InvalidSize => assert!(true),
                    err => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", err)
                },
                Err(ex) => assert!(false, "expected ParseError::InvalidSize but got error: {:?}", ex)
            }
        };
    }

    #[test]
    fn f64_on_write_to() {
        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [66u8, 182u8, 10u8, 74u8, 115u8, 78u8, 10u8, 137u8];
        match 24233454554634.5342f64.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };

        let mut buf = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut writer = &mut buf as &mut [u8];
        let expected = [194u8, 200u8, 221u8, 45u8, 70u8, 181u8, 220u8, 202u8, 0u8];
        match (-54676452895673.578f64).write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        };
    }
}