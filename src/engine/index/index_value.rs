use std::convert::TryFrom;
use super::{POSITION_SIZE, LoadFrom, pos_from_bytes, pos_into_bytes};
use crate::engine::parse_error::ParseError;

/// Index value line size.
/// 
/// Format:
/// ```
/// <input_start_pos:8><input_end_pos:8><output_pos:8><match:1>
/// ```
pub const VALUE_LINE_SIZE: usize = 25;

/// Match flag enumerator.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum MatchFlag {
    Yes = b'Y' as isize,
    No = b'N' as isize,
    Skip = b'S' as isize,
    None = 0
}

impl TryFrom<u8> for MatchFlag {
    type Error = ParseError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        let match_flag = match v {
            b'Y' => MatchFlag::Yes,
            b'N' => MatchFlag::No,
            b'S' => MatchFlag::Skip,
            0 => MatchFlag::None,
            _ => return Err(ParseError::InvalidFormat)
        };

        Ok(match_flag)
    }
}

impl From<&MatchFlag> for u8 {
    fn from(v: &MatchFlag) -> Self {
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
pub struct IndexValue {
    /// Input file start position for the record.
    pub input_start_pos: u64,

    /// Input file end position for the record.
    pub input_end_pos: u64,

    /// Output file position for the record.
    pub output_pos: u64,

    /// Match flag for the record (Y,N,S).
    pub match_flag: MatchFlag
}

impl IndexValue {
    pub fn new() -> Self {
        Self{
            input_start_pos: 0,
            input_end_pos: 0,
            output_pos: 0,
            match_flag: MatchFlag::None
        }
    }
}

impl LoadFrom<&[u8]> for IndexValue {
    fn load_from(&mut self, buf: &[u8]) -> Result<(), ParseError> {
        // validate line size
        if buf.len() != VALUE_LINE_SIZE {
            return Err(ParseError::InvalidSize);
        }

        // validate format and values
        let input_start_pos = pos_from_bytes(&buf[..POSITION_SIZE])?;
        let input_end_pos = pos_from_bytes(&buf[POSITION_SIZE..2*POSITION_SIZE])?;
        let output_pos = pos_from_bytes(&buf[2*POSITION_SIZE..3*POSITION_SIZE])?;
        let match_flag = buf[3*POSITION_SIZE].try_into()?;

        self.input_start_pos = input_start_pos;
        self.input_end_pos = input_end_pos;
        self.output_pos = output_pos;
        self.match_flag = match_flag;

        Ok(())
    }
}

impl TryFrom<&[u8]> for IndexValue {
    type Error = ParseError;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        let mut value = Self::new();
        value.load_from(buf)?;
        Ok(value)
    }
}

impl From<&IndexValue> for Vec<u8> {
    fn from(value: &IndexValue) -> Vec<u8> {
        let mut buf = [0u8; VALUE_LINE_SIZE];

        // convert value attributes into bytes and save it on buf
        pos_into_bytes(value.input_start_pos, &mut buf[..POSITION_SIZE]).unwrap();
        pos_into_bytes(value.input_end_pos, &mut buf[POSITION_SIZE..2*POSITION_SIZE]).unwrap();
        pos_into_bytes(value.output_pos, &mut buf[2*POSITION_SIZE..3*POSITION_SIZE]).unwrap();
        buf[3*POSITION_SIZE] = u8::from(&value.match_flag);
        
        buf.to_vec()
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;

    pub fn build_value_bytes(input_start_pos: u64, input_end_pos: u64, output_pos: u64, match_flag: u8) -> [u8; VALUE_LINE_SIZE] {
        let mut buf = [0u8; VALUE_LINE_SIZE];
        let buf_input_start_pos = &mut buf[..POSITION_SIZE];
        buf_input_start_pos.copy_from_slice(&input_start_pos.to_be_bytes());
        let buf_input_end_pos = &mut buf[POSITION_SIZE..2*POSITION_SIZE];
        buf_input_end_pos.copy_from_slice(&input_end_pos.to_be_bytes());
        let buf_output_pos = &mut buf[2*POSITION_SIZE..3*POSITION_SIZE];
        buf_output_pos.copy_from_slice(&output_pos.to_be_bytes());
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
            IndexValue{
                input_start_pos: 0,
                input_end_pos: 0,
                output_pos: 0,
                match_flag: MatchFlag::None
            },
            IndexValue::new()
        );
    }

    #[test]
    fn load_from_u8_slice() {
        let mut value = IndexValue{
            input_start_pos: 0,
            input_end_pos: 0,
            output_pos: 0,
            match_flag: MatchFlag::None
        };

        // first random try
        let expected = IndexValue{
            input_start_pos: 1400004,
            input_end_pos: 2341234,
            output_pos: 20777332,
            match_flag: MatchFlag::Skip
        };
        let buf = build_value_bytes(1400004, 2341234, 20777332, b'S');
        if let Err(_) = value.load_from(&buf[..]) {
            assert!(false, "shouldn't error out");
            return;
        };
        assert_eq!(expected, value);

        // second random try
        let expected = IndexValue{
            input_start_pos: 445685221,
            input_end_pos: 34656435243,
            output_pos: 8427343298732,
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
        let mut value = IndexValue{
            input_start_pos: 0,
            input_end_pos: 0,
            output_pos: 0,
            match_flag: MatchFlag::None
        };

        let mut buf: Vec<u8> = vec!();
        for _i in 0..VALUE_LINE_SIZE+1 {
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
        let mut value = IndexValue{
            input_start_pos: 0,
            input_end_pos: 0,
            output_pos: 0,
            match_flag: MatchFlag::None
        };

        let mut buf: Vec<u8> = vec!();
        for _i in 0..VALUE_LINE_SIZE-1 {
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
        let expected = IndexValue{
            input_start_pos: 14321432,
            input_end_pos: 456542532,
            output_pos: 5463211,
            match_flag: MatchFlag::No
        };
        let buf = build_value_bytes(14321432, 456542532, 5463211, b'N');
        let value = match IndexValue::try_from(&buf[..]) {
            Ok(v) => v,
            Err(_) => {
                assert!(false, "shouldn't error out");
                return;
            }
        };
        assert_eq!(expected, value);

        // second random try
        let expected = IndexValue{
            input_start_pos: 56745631532,
            input_end_pos: 45245234,
            output_pos: 11896524543541452385,
            match_flag: MatchFlag::Yes
        };
        let buf = build_value_bytes(56745631532, 45245234, 11896524543541452385, b'Y');
        let value = match IndexValue::try_from(&buf[..]) {
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
        let index = &IndexValue{
            input_start_pos: 32464573645,
            input_end_pos: 2343534543,
            output_pos: 29034574985234,
            match_flag: MatchFlag::Yes
        };
        let value: Vec<u8> = index.into();
        assert_eq!(expected, value);

        // second random try
        let buf = build_value_bytes(789865473674, 83454327, 98734951983457, b'N');
        let expected = &buf[..];
        let index = &IndexValue{
            input_start_pos: 789865473674,
            input_end_pos: 83454327,
            output_pos: 98734951983457,
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
            assert_eq!(b'Y', u8::from(&MatchFlag::Yes));
            assert_eq!(b'N', u8::from(&MatchFlag::No));
            assert_eq!(b'S', u8::from(&MatchFlag::Skip));
            assert_eq!(0u8, u8::from(&MatchFlag::None));
        }
    }
}