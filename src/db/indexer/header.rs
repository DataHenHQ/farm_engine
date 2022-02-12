use std::io::{Read, Write};
use std::convert::TryFrom;
use anyhow::{bail, Result};
use super::VERSION;
use crate::error::ParseError;
use crate::traits::{ByteSized, FromByteSlice, WriteAsBytes, ReadFrom, WriteTo, LoadFrom};

/// File's magic numbervalue size bytes.
pub const MAGIC_NUMBER_SIZE: usize = 11;

/// File's magic number value `datahen_idx` as bytes.
pub const MAGIC_NUMBER_BYTES: [u8; MAGIC_NUMBER_SIZE] = [100, 97, 116, 97, 104, 101, 110, 95, 105, 100, 120];

/// Unsigned hash value size. Currently using SHA3-256 = key 32 bytes
pub const HASH_SIZE: usize = 32;

/// Signed hash value size.
pub const HASH_U_SIZE: usize = HASH_SIZE + 1;

/// Input supported file types.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum InputType {
    Unknown,
    CSV,
    JSON
}

impl TryFrom<u8> for InputType {
    type Error = ParseError;

    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
        let match_flag = match v {
            0 => Self::Unknown,
            1 => Self::CSV,
            2 => Self::JSON,
            _ => return Err(ParseError::InvalidFormat)
        };

        Ok(match_flag)
    }
}

impl From<&InputType> for u8 {
    fn from(v: &InputType) -> Self {
        match v {
            InputType::Unknown => 0,
            InputType::CSV => 1,
            InputType::JSON => 2
        }
    }
}

impl From<InputType> for u8 {
    fn from(v: InputType) -> Self {
        (&v).into()
    }
}

impl ByteSized for InputType {
    const BYTES: usize = 1;
}

impl WriteAsBytes for InputType {
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()> {
        // validate value size
        if buf.len() != Self::BYTES {
            bail!(ParseError::InvalidSize);
        }

        // save value as bytes
        buf.copy_from_slice(&[self.into()]);

        Ok(())
    }
}

//// Describes an Indexer file header.
#[derive(Debug, PartialEq, Clone)]
pub struct Header {
    /// `true` when the input file has been indexed successfully.
    pub indexed: bool,

    /// Input file hash.
    pub hash: Option<[u8; HASH_SIZE]>,

    /// Indexed records count.
    pub indexed_count: u64,

    /// input file type
    pub input_type: InputType
}

impl Header {
    /// Creates a new header.
    pub fn new() -> Self {
        Self{
            indexed: false,
            hash: None,
            indexed_count: 0,
            input_type: InputType::Unknown
        }
    }

    /// Clone input file hash value.
    /// 
    /// # Arguments
    /// 
    /// * `buf` - Bytes to clone hash from.
    pub fn clone_hash(buf: &[u8]) -> Result<[u8; HASH_SIZE], ParseError> {
        if buf.len() != HASH_SIZE {
            return Err(ParseError::InvalidSize);
        }

        let mut hash = [0u8; HASH_SIZE];
        hash.copy_from_slice(buf);
        Ok(hash)
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

        // save indexed
        self.indexed.write_as_bytes(&mut buf[carry..carry+bool::BYTES]).unwrap();
        carry += bool::BYTES;

        // save indexed record count
        self.indexed_count.write_as_bytes(&mut buf[carry..carry+u64::BYTES]).unwrap();
        carry += u64::BYTES;

        // save input type
        buf[carry] = self.input_type.into();
        carry += InputType::BYTES;

        // save hash flag and value
        if let Some(hash_bytes) = self.hash {
            buf[carry] = 1;
            carry += 1;
            let hash_buf = &mut buf[carry..carry+HASH_SIZE];
            hash_buf.copy_from_slice(&hash_bytes);
        }
        buf
    }
}

impl ByteSized for Header {
    /// Index header size in bytes.
    /// 
    /// Byte Format
    /// `<magic_number:11><version:4><indexed:1><indexed_count:8><input_type:1><hash_valid:1><hash:32>`.
    const BYTES: usize = 47 + MAGIC_NUMBER_SIZE;
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

        // read and validate indexer version
        let version = u32::from_byte_slice(&buf[carry..carry+u32::BYTES])?;
        if version != VERSION {
            bail!("indexer version mismatch, expected {} buf found {}", VERSION, version);
        }
        carry += u32::BYTES;

        // read indexed
        let indexed = bool::from_byte_slice(&buf[carry..carry+1])?;
        carry += bool::BYTES;

        // read indexed record count
        let indexed_count = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        // read input type
        let input_type = buf[carry].try_into()?;
        carry += InputType::BYTES;

        // extract hash
        let hash = if buf[carry] > 0 {
            carry += 1;
            Some(Self::clone_hash(&buf[carry..carry+HASH_SIZE])?)
        } else {
            None
        };

        // save values
        self.indexed = indexed;
        self.indexed_count = indexed_count;
        self.hash = hash;
        self.input_type = input_type;

        Ok(())
    }
}

impl FromByteSlice for Header {
    fn from_byte_slice(buf: &[u8]) -> Result<Self> {
        let mut header = Self::new();
        let mut reader = buf;
        header.load_from(&mut reader)?;
        Ok(header)
    }
}

impl ReadFrom for Header {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut header = Self::new();
        header.load_from(reader)?;
        Ok(header)
    }
}

impl TryFrom<&[u8]> for Header {
    type Error = anyhow::Error;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        let mut header = Self::new();
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
    use rand::Rng;

    /// Generate a random hash value.
    pub fn random_hash() -> [u8; HASH_SIZE] {
        let mut rng = rand::thread_rng();
        let mut buf = [0u8; HASH_SIZE];

        for i in 0..HASH_SIZE {
            buf[i] = rng.gen_range(0..255);
        }
        buf
    }

    /// Builds an index header as byte slice from the values provided.
    /// 
    /// # Arguments
    /// 
    /// * `hash_valid` - `true` if valid hash flag should be true.
    /// * `hash_buf` - Hash byte slice.
    /// * `indexed` - `true` if indexed flag should be true.
    /// * `indexed_count` - Total indexed records.
    pub fn build_header_bytes(hash_valid: bool, hash_buf: &[u8], indexed: bool, indexed_count: u64, input_type: InputType) -> [u8; Header::BYTES] {
        let mut hash = None;
        if hash_valid {
            if hash_buf.len() != HASH_SIZE {
                panic!("invalid hash size, expected {} bytes but got {} bytes", HASH_SIZE, hash_buf.len());
            }
            let mut buf = [0u8; HASH_SIZE];
            buf.copy_from_slice(hash_buf);
            hash = Some(buf);
        }
        Header{
            indexed,
            indexed_count,
            hash,
            input_type
        }.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod input_type {
        use super::*;

        #[test]
        fn try_from_u8() {
            match InputType::try_from(0u8) {
                Ok(v) => assert_eq!(InputType::Unknown, v),
                Err(_) => assert!(false, "should be Ok(InputType::Unknown)")
            }
            match InputType::try_from(1u8) {
                Ok(v) => assert_eq!(InputType::CSV, v),
                Err(_) => assert!(false, "should be Ok(InputType::CSV)")
            }
            match InputType::try_from(2u8) {
                Ok(v) => assert_eq!(InputType::JSON, v),
                Err(_) => assert!(false, "should be Ok(InputType::JSON)")
            }
            match InputType::try_from(3u8) {
                Ok(_) => assert!(false, "should be an Err(ParseError::InvalidFormat)"),
                Err(e) => assert!(
                    if let ParseError::InvalidFormat = e { true } else { false },
                    "should be an Err(ParseError::InvalidFormat)"
                )
            }
        }

        #[test]
        fn into_u8() {
            assert_eq!(0u8, u8::from(InputType::Unknown));
            assert_eq!(1u8, u8::from(InputType::CSV));
            assert_eq!(2u8, u8::from(InputType::JSON));

            assert_eq!(0u8, u8::from(&InputType::Unknown));
            assert_eq!(1u8, u8::from(&InputType::CSV));
            assert_eq!(2u8, u8::from(&InputType::JSON));
        }
    }

    mod header {
        use super::*;
        use test_helper::*;

        #[test]
        fn new() {
            assert_eq!(
                Header{
                    indexed: false,
                    hash: None,
                    indexed_count: 0,
                    input_type: InputType::Unknown
                },
                Header::new()
            );
        }

        #[test]
        fn clone_hash() {
            // first try
            let expected = random_hash();
            match Header::clone_hash(&expected) {
                Ok(v) => assert_eq!(expected, v),
                Err(_) => assert!(false, "clone_hash error out")
            }

            // second try
            let expected = random_hash();
            match Header::clone_hash(&expected) {
                Ok(v) => assert_eq!(expected, v),
                Err(_) => assert!(false, "clone_hash error out")
            }
        }

        #[test]
        fn as_bytes() {
            // first test
            let mut expected = [
                // magic number
                100, 97, 116, 97, 104, 101, 110, 95, 105, 100, 120,
                // version
                0, 0, 0, 2,
                // indexed
                1,
                // indexed count = 2311457452320998633
                32, 19, 242, 78, 103, 5, 196, 233,
                // input type
                1,
                // valid hash
                1,
                // hash value placeholder
                0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
            ];
            let hash_buf = &mut expected[26..26+HASH_SIZE];
            let random_hash_buf = random_hash();
            if hash_buf.len() != HASH_SIZE {
                panic!("invalid hash size, check test \"indexer::header::as_bytes\"");
            }
            hash_buf.copy_from_slice(&random_hash_buf);

            // test header as_bytes function
            let header = Header{
                indexed: true,
                indexed_count: 2311457452320998633,
                hash: Some(random_hash_buf),
                input_type: InputType::CSV
            };
            assert_eq!(expected, header.as_bytes());

            // second test
            let expected = [
                // magic number
                100, 97, 116, 97, 104, 101, 110, 95, 105, 100, 120,
                // version
                0, 0, 0, 2,
                // indexed
                0,
                // indexed count = 4525325654675485867
                62, 205, 47, 180, 235, 228, 244, 171,
                // input type
                2,
                // valid hash
                0,
                // empty hash value
                0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
            ];

            // test header as_bytes function
            let header = Header{
                indexed: false,
                indexed_count: 4525325654675485867,
                hash: None,
                input_type: InputType::JSON,
            };
            assert_eq!(expected, header.as_bytes());
        }

        #[test]
        fn byte_sized() {
            assert_eq!(58, Header::BYTES);
        }

        #[test]
        fn load_from_u8_slice() {
            // first random try
            let mut header = Header{
                indexed: false,
                hash: None,
                indexed_count: 0,
                input_type: InputType::Unknown
            };
            let hash = random_hash();
            let expected = Header{
                indexed: true,
                hash: Some(hash),
                indexed_count: 4535435,
                input_type: InputType::JSON
            };
            let buf = build_header_bytes(true, &hash, true, 4535435, InputType::JSON);
            let mut reader = &buf as &[u8];
            if let Err(e) = header.load_from(&mut reader) {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            };
            assert_eq!(expected, header);

            // second random try
            let mut header = Header{
                indexed: false,
                hash: None,
                indexed_count: 0,
                input_type: InputType::Unknown
            };
            let expected = Header{
                indexed: false,
                hash: None,
                indexed_count: 6572646535124,
                input_type: InputType::JSON
            };
            let buf = build_header_bytes(false, &[], false, 6572646535124, InputType::JSON);
            let mut reader = &buf as &[u8];
            if let Err(e) = header.load_from(&mut reader) {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            };
            assert_eq!(expected, header);
        }

        #[test]
        fn load_from_u8_slice_with_invalid_smaller_buf_size() {
            let mut header = Header{
                indexed: false,
                hash: None,
                indexed_count: 0,
                input_type: InputType::Unknown
            };

            let expected = std::io::ErrorKind::UnexpectedEof;
            let buf = [0u8; Header::BYTES-1];
            let mut reader = &buf as &[u8];
            match header.load_from(&mut reader) {
                Ok(v) => assert!(false, "expected IO error with ErrorKind::UnexpectedEof but got {:x?}", v),
                Err(e) => match e.downcast::<std::io::Error>() {
                    Ok(ex) => assert_eq!(expected, ex.kind()),
                    Err(ex) => assert!(false, "expected IO error with ErrorKind::UnexpectedEof but got error: {:?}", ex)
                }
            }
        }

        #[test]
        fn from_byte_slice() {
            // first random try
            let hash = random_hash();
            let expected = Header{
                indexed: true,
                hash: Some(hash),
                indexed_count: 2341234,
                input_type: InputType::CSV
            };
            let buf = build_header_bytes(true, &hash, true, 2341234, InputType::CSV);
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
                indexed: false,
                hash: None,
                indexed_count: 9879873495743,
                input_type: InputType::Unknown
            };
            let buf = build_header_bytes(false, &[], false, 9879873495743, InputType::Unknown);
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
            let hash = random_hash();
            let expected = Header{
                indexed: false,
                hash: Some(hash),
                indexed_count: 974734838473874,
                input_type: InputType::CSV
            };
            let buf = build_header_bytes(true, &hash, false, 974734838473874, InputType::CSV);
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
                indexed: true,
                hash: None,
                indexed_count: 3434232315645344,
                input_type: InputType::JSON
            };
            let buf = build_header_bytes(false, &[], true, 3434232315645344, InputType::JSON);
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
            let hash = random_hash();
            let expected = Header{
                indexed: false,
                hash: Some(hash),
                indexed_count: 32412342134234,
                input_type: InputType::CSV
            };
            let buf = build_header_bytes(true, &hash, false, 32412342134234, InputType::CSV);
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
                indexed: true,
                hash: None,
                indexed_count: 56535423143214,
                input_type: InputType::JSON
            };
            let buf = build_header_bytes(false, &[], true, 56535423143214, InputType::JSON);
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
            let hash = random_hash();
            let expected = build_header_bytes(true, &hash, false, 788477630402843, InputType::CSV);
            let header = Header{
                indexed: false,
                hash: Some(hash),
                indexed_count: 788477630402843,
                input_type: InputType::CSV
            };
            let mut buf = [0u8; Header::BYTES];
            let mut writer = &mut buf as &mut [u8];
            if let Err(e) = header.write_to(&mut writer) {
                assert!(false, "{:?}", e);
                return;
            };
            assert_eq!(expected, buf);

            // second random try
            let expected = build_header_bytes(false, &[], true, 63439320337562938, InputType::JSON);
            let header = Header{
                indexed: true,
                hash: None,
                indexed_count: 63439320337562938,
                input_type: InputType::JSON
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
}