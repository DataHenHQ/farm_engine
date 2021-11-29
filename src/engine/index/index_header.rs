use std::convert::TryFrom;
use super::{POSITION_SIZE, LoadFrom, pos_from_bytes, pos_into_bytes};
use crate::engine::parse_error::ParseError;

/// Index header line size.
/// 
/// Format:
/// ```
/// <indexed:1><indexed_count:8><hash_valid:1><hash:32>
/// ```
pub const HEADER_LINE_SIZE: usize = 42;

// Unsigned hash value size.
pub const HASH_SIZE: usize = blake3::OUT_LEN;

// Signed hash value size.
pub const HASH_U_SIZE: usize = HASH_SIZE + 1;

/// Describes an Indexer file header.
#[derive(Debug, PartialEq)]
pub struct IndexHeader {
    /// `true` when the input file has been indexed successfully.
    pub indexed: bool,

    // Input file hash
    pub hash: Option<[u8; HASH_SIZE]>,

    // Indexed records count.
    pub indexed_count: u64
}

impl IndexHeader {
    pub fn new() -> Self {
        Self{
            indexed: false,
            hash: None,
            indexed_count: 0
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
}

impl LoadFrom<&[u8]> for IndexHeader {
    fn load_from(&mut self, buf: &[u8]) -> Result<(), ParseError> {
        // validate string size
        if buf.len() != HEADER_LINE_SIZE {
            return Err(ParseError::InvalidSize);
        }

        // extract indexed
        let indexed = match buf[0] {
            0 => false,
            1 => true,
            _ => return Err(ParseError::InvalidValue)
        };

        // extract indexed record count
        let indexed_count = pos_from_bytes(&buf[1..1+POSITION_SIZE])?;

        // extract hash
        let hash = if buf[1+POSITION_SIZE] > 0 {
            Some(Self::clone_hash(&buf[2+POSITION_SIZE..2+POSITION_SIZE+HASH_SIZE])?)
        } else {
            None
        };

        // save values
        self.indexed = indexed;
        self.hash = hash;
        self.indexed_count = indexed_count;

        Ok(())
    }
}

impl TryFrom<&[u8]> for IndexHeader {
    type Error = ParseError;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        let mut header = Self::new();
        header.load_from(buf)?;
        Ok(header)
    }
}

impl From<&IndexHeader> for Vec<u8> {
    fn from(header: &IndexHeader) -> Vec<u8> {
        let mut buf = [0u8; HEADER_LINE_SIZE];

        buf[0] = header.indexed as u8;
        pos_into_bytes(header.indexed_count, &mut buf[1..1+POSITION_SIZE]).unwrap();

        // copy hash as bytes
        if let Some(hash) = header.hash {
            buf[1+POSITION_SIZE] = 1;
            let hash_buf = &mut buf[2+POSITION_SIZE..2+POSITION_SIZE+HASH_SIZE];
            hash_buf.copy_from_slice(&hash);
        }
        
        buf.to_vec()
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

    /// Builds a header byte slice from the values provided.
    /// 
    /// # Arguments
    /// 
    /// * `hash_valid` - `true` if valid hash flag should be true.
    /// * `hash` - Hash byte slice.
    /// * `indexed` - `true` if indexed flag should be true.
    /// * `indexed_count` - Total indexed records.
    pub fn build_header_bytes(hash_valid: bool, hash: &[u8], indexed: bool, indexed_count: u64) -> [u8; HEADER_LINE_SIZE] {
        let mut buf = [0u8; HEADER_LINE_SIZE];
        if indexed {
            buf[0] = 1u8;
        }
        let buf_indexed_count = &mut buf[1..1+POSITION_SIZE];
        buf_indexed_count.copy_from_slice(&indexed_count.to_be_bytes());
        if hash_valid {
            buf[1+POSITION_SIZE] = 1;
        }
        let buf_hash = &mut buf[2+POSITION_SIZE..2+POSITION_SIZE+HASH_SIZE];
        buf_hash.copy_from_slice(&hash);
        buf
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helper::*;

    #[test]
    fn new() {
        assert_eq!(
            IndexHeader{
                indexed: false,
                hash: None,
                indexed_count: 0
            },
            IndexHeader::new()
        );
    }

    #[test]
    fn load_from_u8_slice() {
        let mut value = IndexHeader{
            indexed: false,
            hash: None,
            indexed_count: 0
        };

        // first random try
        let hash = random_hash();
        let expected = IndexHeader{
            indexed: true,
            hash: Some(hash),
            indexed_count: 4535435
        };
        let buf = build_header_bytes(true, &hash, true, 4535435);
        if let Err(_) = value.load_from(&buf[..]) {
            assert!(false, "shouldn't error out");
            return;
        };
        assert_eq!(expected, value);

        // second random try
        let hash = random_hash();
        let expected = IndexHeader{
            indexed: false,
            hash: None,
            indexed_count: 6572646535124
        };
        let buf = build_header_bytes(false, &hash, false, 6572646535124);
        if let Err(_) = value.load_from(&buf[..]) {
            assert!(false, "shouldn't error out");
            return;
        };
        assert_eq!(expected, value);
    }

    #[test]
    fn try_from_u8_slice() {
        // first random try
        let hash = random_hash();
        let expected = IndexHeader{
            indexed: false,
            hash: Some(hash),
            indexed_count: 32412342134234
        };
        let buf = build_header_bytes(true, &hash, false, 32412342134234);
        let value = match IndexHeader::try_from(&buf[..]) {
            Ok(v) => v,
            Err(_) => {
                assert!(false, "shouldn't error out");
                return;
            }
        };
        assert_eq!(expected, value);

        // second random try
        let hash = random_hash();
        let expected = IndexHeader{
            indexed: true,
            hash: None,
            indexed_count: 56535423143214
        };
        let buf = build_header_bytes(false, &hash, true, 56535423143214);
        let value = match IndexHeader::try_from(&buf[..]) {
            Ok(v) => v,
            Err(_) => {
                assert!(false, "shouldn't error out");
                return;
            }
        };
        assert_eq!(expected, value);
    }

    #[test]
    fn into_u8_slice() {
        // first random try
        let buf = build_header_bytes(false, &[0u8; HASH_SIZE], false, 674565465345);
        let expected = buf.to_vec();
        let header = &IndexHeader{
            indexed: false,
            hash: None,
            indexed_count: 674565465345
        };
        let value: Vec<u8> = header.into();
        assert_eq!(expected, value);

        // second random try
        let hash = random_hash();
        let buf = build_header_bytes(true, &hash, true, 87687867546345);
        let expected = buf.to_vec();
        let header = &IndexHeader{
            indexed: true,
            hash: Some(hash),
            indexed_count: 87687867546345
        };
        let value: Vec<u8> = header.into();
        assert_eq!(expected, value);
    }
}