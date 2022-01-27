use std::convert::TryFrom;
use super::{POSITION_SIZE, LoadFrom};
use crate::error::ParseError;
use crate::utils::{FromByteSlice, WriteAsBytes};

/// Index value size in bytes.
/// 
/// Each record has the following format:
/// `<input_start_pos:8><input_end_pos:8><spent_time:8><match:1>`
pub const BYTES: usize = 25;

/// Match flag enumerator.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum MatchFlag {
    Yes = b'Y' as isize,
    No = b'N' as isize,
    Skip = b'S' as isize,
    None = 0
}

impl MatchFlag {
    /// Return an array with all possible values.
    pub fn as_array() -> [Self; 4] {
        [
            Self::Yes,
            Self::No,
            Self::Skip,
            Self::None
        ]
    }

    /// Returns an array with all possible values as bytes.
    pub fn as_bytes() -> [u8; 4] {
        [
            Self::Yes.into(),
            Self::No.into(),
            Self::Skip.into(),
            Self::None.into()
        ]
    }
}

impl TryFrom<u8> for MatchFlag {
    type Error = ParseError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        let match_flag = match v {
            b'Y' => Self::Yes,
            b'N' => Self::No,
            b'S' => Self::Skip,
            0 => Self::None,
            _ => return Err(ParseError::InvalidFormat)
        };

        Ok(match_flag)
    }
}

impl From<MatchFlag> for u8 {
    fn from(v: MatchFlag) -> Self {
        match v {
            MatchFlag::Yes => b'Y',
            MatchFlag::No => b'N',
            MatchFlag::Skip => b'S',
            MatchFlag::None => 0
        }
    }
}

/// Describes an Indexer file value.
#[derive(Debug, PartialEq)]
pub struct Value {
    /// Input file start position for the record.
    pub input_start_pos: u64,

    /// Input file end position for the record.
    pub input_end_pos: u64,

    /// Match flag for the record.
    pub match_flag: MatchFlag,

    /// Spent time to resolve. The time unit must be handle by the dev.
    pub spent_time: u64
}

impl Value {
    pub fn new() -> Self {
        Self{
            input_start_pos: 0,
            input_end_pos: 0,
            spent_time: 0,
            match_flag: MatchFlag::None
        }
    }
}

impl LoadFrom<&[u8]> for Value {
    fn load_from(&mut self, buf: &[u8]) -> Result<(), ParseError> {
        // validate line size
        if buf.len() != BYTES {
            return Err(ParseError::InvalidSize);
        }

        // validate format and values
        let input_start_pos = u64::from_byte_slice(&buf[..POSITION_SIZE])?;
        let input_end_pos = u64::from_byte_slice(&buf[POSITION_SIZE..2*POSITION_SIZE])?;
        let spent_time = u64::from_byte_slice(&buf[2*POSITION_SIZE..3*POSITION_SIZE])?;
        let match_flag = buf[3*POSITION_SIZE].try_into()?;

        self.input_start_pos = input_start_pos;
        self.input_end_pos = input_end_pos;
        self.spent_time = spent_time;
        self.match_flag = match_flag;

        Ok(())
    }
}

impl TryFrom<&[u8]> for Value {
    type Error = ParseError;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        let mut value = Self::new();
        value.load_from(buf)?;
        Ok(value)
    }
}

impl From<&Value> for Vec<u8> {
    fn from(value: &Value) -> Vec<u8> {
        let mut buf = [0u8; BYTES];

        // convert value attributes into bytes and save it on buf
        value.input_start_pos.write_as_bytes(&mut buf[..POSITION_SIZE]).unwrap();
        value.input_end_pos.write_as_bytes(&mut buf[POSITION_SIZE..2*POSITION_SIZE]).unwrap();
        value.spent_time.write_as_bytes(&mut buf[2*POSITION_SIZE..3*POSITION_SIZE]).unwrap();
        buf[3*POSITION_SIZE] = u8::from(value.match_flag);
        
        buf.to_vec()
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;

    pub fn build_value_bytes(input_start_pos: u64, input_end_pos: u64, spent_time: u64, match_flag: u8) -> [u8; BYTES] {
        let mut buf = [0u8; BYTES];
        let buf_input_start_pos = &mut buf[..POSITION_SIZE];
        buf_input_start_pos.copy_from_slice(&input_start_pos.to_be_bytes());
        let buf_input_end_pos = &mut buf[POSITION_SIZE..2*POSITION_SIZE];
        buf_input_end_pos.copy_from_slice(&input_end_pos.to_be_bytes());
        let buf_spent_time = &mut buf[2*POSITION_SIZE..3*POSITION_SIZE];
        buf_spent_time.copy_from_slice(&spent_time.to_be_bytes());
        buf[3*POSITION_SIZE] = match_flag;
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
            Value{
                input_start_pos: 0,
                input_end_pos: 0,
                spent_time: 0,
                match_flag: MatchFlag::None
            },
            Value::new()
        );
    }

    #[test]
    fn load_from_u8_slice() {
        let mut value = Value{
            input_start_pos: 0,
            input_end_pos: 0,
            spent_time: 0,
            match_flag: MatchFlag::None
        };

        // first random try
        let expected = Value{
            input_start_pos: 1400004,
            input_end_pos: 2341234,
            spent_time: 20777332,
            match_flag: MatchFlag::Skip
        };
        let buf = build_value_bytes(1400004, 2341234, 20777332, b'S');
        if let Err(_) = value.load_from(&buf[..]) {
            assert!(false, "shouldn't error out");
            return;
        };
        assert_eq!(expected, value);

        // second random try
        let expected = Value{
            input_start_pos: 445685221,
            input_end_pos: 34656435243,
            spent_time: 8427343298732,
            match_flag: MatchFlag::None
        };
        let buf = build_value_bytes(445685221, 34656435243, 8427343298732, 0);
        if let Err(_) = value.load_from(&buf[..]) {
            assert!(false, "shouldn't error out");
            return;
        };
        assert_eq!(expected, value);
    }

    #[test]
    fn load_from_u8_slice_with_invalid_bigger_size() {
        let mut value = Value{
            input_start_pos: 0,
            input_end_pos: 0,
            spent_time: 0,
            match_flag: MatchFlag::None
        };

        let mut buf: Vec<u8> = vec!();
        for _i in 0..BYTES+1 {
            buf.push(0u8);
        }
        match value.load_from(&buf[..]) {
            Ok(_) => assert!(false, "should have error with ParseError::InvalidSize"),
            Err(e) => assert!(
                if let ParseError::InvalidSize = e { true } else { false },
                "should have be Parser::InvalidSize"
            )
        }
    }

    #[test]
    fn load_from_u8_slice_with_invalid_smaller_size() {
        let mut value = Value{
            input_start_pos: 0,
            input_end_pos: 0,
            spent_time: 0,
            match_flag: MatchFlag::None
        };

        let mut buf: Vec<u8> = vec!();
        for _i in 0..BYTES-1 {
            buf.push(0u8);
        }
        match value.load_from(&buf[..]) {
            Ok(_) => assert!(false, "should have error with ParseError::InvalidSize"),
            Err(e) => assert!(
                if let ParseError::InvalidSize = e { true } else { false },
                "should have be Parser::InvalidSize"
            )
        }
    }

    #[test]
    fn try_from_u8_slice() {
        // first random try
        let expected = Value{
            input_start_pos: 14321432,
            input_end_pos: 456542532,
            spent_time: 5463211,
            match_flag: MatchFlag::No
        };
        let buf = build_value_bytes(14321432, 456542532, 5463211, b'N');
        let value = match Value::try_from(&buf[..]) {
            Ok(v) => v,
            Err(_) => {
                assert!(false, "shouldn't error out");
                return;
            }
        };
        assert_eq!(expected, value);

        // second random try
        let expected = Value{
            input_start_pos: 56745631532,
            input_end_pos: 45245234,
            spent_time: 11896524543541452385,
            match_flag: MatchFlag::Yes
        };
        let buf = build_value_bytes(56745631532, 45245234, 11896524543541452385, b'Y');
        let value = match Value::try_from(&buf[..]) {
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
        let buf = build_value_bytes(32464573645, 2343534543, 29034574985234, b'Y');
        let expected = buf.to_vec();
        let index = &Value{
            input_start_pos: 32464573645,
            input_end_pos: 2343534543,
            spent_time: 29034574985234,
            match_flag: MatchFlag::Yes
        };
        let value: Vec<u8> = index.into();
        assert_eq!(expected, value);

        // second random try
        let buf = build_value_bytes(789865473674, 83454327, 98734951983457, b'N');
        let expected = &buf[..];
        let index = &Value{
            input_start_pos: 789865473674,
            input_end_pos: 83454327,
            spent_time: 98734951983457,
            match_flag: MatchFlag::No
        };
        let value: Vec<u8> = index.into();
        assert_eq!(expected, value);
    }

    mod match_flag {
        use super::*;

        #[test]
        fn try_from_u8() {
            match MatchFlag::try_from(b'Y') {
                Ok(v) => assert_eq!(MatchFlag::Yes, v),
                Err(_) => assert!(false, "should be Ok(MatchFlag::Yes)")
            }
            match MatchFlag::try_from(b'N') {
                Ok(v) => assert_eq!(MatchFlag::No, v),
                Err(_) => assert!(false, "should be Ok(MatchFlag::No)")
            }
            match MatchFlag::try_from(b'S') {
                Ok(v) => assert_eq!(MatchFlag::Skip, v),
                Err(_) => assert!(false, "should be Ok(MatchFlag::Skip)")
            }
            match MatchFlag::try_from(0u8) {
                Ok(v) => assert_eq!(MatchFlag::None, v),
                Err(_) => assert!(false, "should be Ok(MatchFlag::None)")
            }
            match MatchFlag::try_from(b'a') {
                Ok(_) => assert!(false, "should be an Err(ParseError::InvalidFormat)"),
                Err(e) => assert!(
                    if let ParseError::InvalidFormat = e { true } else { false },
                    "should be an Err(ParseError::InvalidFormat)"
                )
            }
        }

        #[test]
        fn into_u8() {
            assert_eq!(b'Y', u8::from(MatchFlag::Yes));
            assert_eq!(b'N', u8::from(MatchFlag::No));
            assert_eq!(b'S', u8::from(MatchFlag::Skip));
            assert_eq!(0u8, u8::from(MatchFlag::None));
        }
    }
}