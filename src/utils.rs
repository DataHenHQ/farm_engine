use std::io::{Read, Write};
use anyhow::{bail, Result};
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
impl_byte_sized!(bool, 1);
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

pub trait FromByteSlice: ByteSized {
    /// Creates a value from its representation as bytes from a byte buffer.
    /// 
    /// # Arguments
    /// 
    /// * `buf` - Byte buffer.
    fn from_byte_slice(buf: &[u8]) -> Result<Self, ParseError>;
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
    fn from_byte_slice(buf: &[u8]) -> Result<Self, ParseError> {
        // validate value size
        if buf.len() != Self::BYTES {
            return Err(ParseError::InvalidSize);
        }

        return match buf[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ParseError::InvalidValue)
        };
    }
}

impl ReadFrom for bool {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        // read and convert bytes into the type value
        let buf = [0u8; Self::BYTES];
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
            fn from_byte_slice(buf: &[u8]) -> Result<Self, ParseError> {
                // validate buf size
                if buf.len() != Self::BYTES {
                    return Err(ParseError::InvalidSize);
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
                let buf = [0u8; Self::BYTES];
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

pub trait WriteAsBytes: ByteSized {
    /// Write the value representation as bytes into a buffer.
    /// 
    /// # Arguments
    /// 
    /// * `buf` - Byte buffer.
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<(), ParseError>;
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
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<(), ParseError> {
        // validate value size
        if buf.len() != Self::BYTES {
            return Err(ParseError::InvalidSize);
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

macro_rules! impl_write_as_bytes {
    ($t:ty, $fn:ident) => {
        impl WriteAsBytes for $t {
            fn write_as_bytes(&self, buf: &mut [u8]) -> Result<(), ParseError> {
                // validate value size
                if buf.len() != Self::BYTES {
                    return Err(ParseError::InvalidSize);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_byte_size() {
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
        assert_eq!(Ok(false), bool::from_byte_slice(&[0u8]));
        assert_eq!(Ok(true), bool::from_byte_slice(&[1u8]));
        assert_eq!(Err(ParseError::InvalidValue), bool::from_byte_slice(&[3u8]));
    }
}