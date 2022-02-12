use std::io::{Seek, SeekFrom, Read, Write};
use std::convert::TryFrom;
use anyhow::{bail, Result};
use crate::error::ParseError;
use crate::traits::{ByteSized, FromByteSlice, WriteAsBytes, ReadFrom, WriteTo, LoadFrom};

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

impl std::fmt::Display for MatchFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", match self {
            Self::Yes => "Yes",
            Self::No => "No",
            Self::Skip => "Skip",
            Self::None => ""
        })
    }
}

impl TryFrom<u8> for MatchFlag {
    type Error = ParseError;

    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
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

impl From<MatchFlag> for u8 {
    fn from(v: MatchFlag) -> Self {
        (&v).into()
    }
}

impl ByteSized for MatchFlag {
    const BYTES: usize = 1;
}

impl WriteAsBytes for MatchFlag {
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()> {
        // validate value size
        if buf.len() != Self::BYTES {
            bail!(ParseError::InvalidSize);
        }

        // save value as bytes
        buf[0] = self.into();

        Ok(())
    }
}

impl WriteTo for MatchFlag {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        writer.write_all(&[self.into()])?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct Data {
    /// Match flag for the value.
    pub match_flag: MatchFlag,

    /// Spent time to resolve. The time unit must be handle by the dev.
    pub spent_time: u64
}

impl Data {
    /// Creates a new data instance.
    pub fn new() -> Self {
        Data{
            match_flag: MatchFlag::None,
            spent_time: 0
        }
    }
}

impl ByteSized for Data {
    /// Index data size in bytes.
    /// 
    /// Byte format
    /// `<spent_time:8><match:1>`
    const BYTES: usize = 9;
}

impl WriteAsBytes for Data {
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()> {
        // validate value size
        if buf.len() != Self::BYTES {
            bail!(ParseError::InvalidSize);
        }

        // save spent_time
        self.spent_time.write_as_bytes(&mut buf[..u64::BYTES])?;

        // save match flag
        buf[u64::BYTES] = self.match_flag.into();

        Ok(())
    }
}

impl WriteTo for Data {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        // write spent time
        self.spent_time.write_to(writer)?;

        // write match flag
        self.match_flag.write_to(writer)?;
        Ok(())
    }
}

/// Describes an Indexer file value.
#[derive(Debug, PartialEq)]
pub struct Value {
    /// Input file start position for the record.
    pub input_start_pos: u64,

    /// Input file end position for the record.
    pub input_end_pos: u64,

    /// Index data.
    pub data: Data
}

impl Value {
    /// Match flag byte index when as bytes.
    pub const MATCH_FLAG_BYTE_INDEX: usize = 24;

    /// Data byte offset.
    pub const DATA_OFFSET: usize = u64::BYTES*2;

    /// Creates a new value.
    pub fn new() -> Self {
        Self{
            input_start_pos: 0,
            input_end_pos: 0,
            data: Data{
                spent_time: 0,
                match_flag: MatchFlag::None
            }
        }
    }

    /// Serialize the instance to a fixed byte slice.
    pub fn as_bytes(&self) -> [u8; Self::BYTES] {
        let mut buf = [0u8; Self::BYTES];
        let mut carry = 0;

        // save input start position
        self.input_start_pos.write_as_bytes(&mut buf[carry..carry+u64::BYTES]).unwrap();
        carry += u64::BYTES;

        // save input end position
        self.input_end_pos.write_as_bytes(&mut buf[carry..carry+u64::BYTES]).unwrap();
        carry += u64::BYTES;

        // save spent time
        self.data.write_as_bytes(&mut buf[carry..carry+Data::BYTES]).unwrap();
        buf
    }

    /// Read the input bytes from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn read_input_from(&self, reader: &mut (impl Seek + Read)) -> Result<Vec<u8>> {
        let size = self.input_end_pos - self.input_start_pos + 1;
        let mut buf = vec![0u8; size as usize];
        reader.seek(SeekFrom::Start(self.input_start_pos))?;
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

impl ByteSized for Value {
    /// Index value size in bytes.
    /// 
    /// Byte format
    /// `<input_start_pos:8><input_end_pos:8><data:9>`
    const BYTES: usize = 16 + Data::BYTES;
}

impl LoadFrom for Value {
    fn load_from(&mut self, reader: &mut impl Read) -> Result<()> {
        // read data
        let mut carry = 0;
        let mut buf = [0u8; Self::BYTES];
        reader.read_exact(&mut buf)?;

        // read input start pos
        let input_start_pos = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        // read input end pos
        let input_end_pos = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        // read spent type
        let spent_time = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

        // read match flag
        let match_flag = buf[carry].try_into()?;

        // record index value data
        self.input_start_pos = input_start_pos;
        self.input_end_pos = input_end_pos;
        self.data.spent_time = spent_time;
        self.data.match_flag = match_flag;

        Ok(())
    }
}

impl FromByteSlice for Value {
    fn from_byte_slice(buf: &[u8]) -> Result<Self> {
        let mut value = Self::new();
        let mut reader = buf;
        value.load_from(&mut reader)?;
        Ok(value)
    }
}

impl ReadFrom for Value {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut value = Self::new();
        value.load_from(reader)?;
        Ok(value)
    }
}

impl TryFrom<&[u8]> for Value {
    type Error = anyhow::Error;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        let mut value = Self::new();
        let mut reader = buf;
        value.load_from(&mut reader)?;
        Ok(value)
    }
}

impl WriteTo for Value {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        writer.write_all(&self.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;

    /// Build a index value as byte slice from the values provided.
    /// 
    /// # Arguments
    /// 
    /// * `spent_time` - Time spent to resolve the record.
    /// * `match_flag` - Resolve action.
    pub fn build_data_bytes(spent_time: u64, match_flag: u8) -> [u8; Data::BYTES] {
        let mut buf = [0u8; Data::BYTES];
        spent_time.write_as_bytes(&mut buf[0..u64::BYTES]).unwrap();
        buf[u64::BYTES] = match_flag;
        buf
    }

    /// Build a index value as byte slice from the values provided.
    /// 
    /// # Arguments
    /// 
    /// * `input_start_pos` - Start byte position on the original source.
    /// * `input_end_pos` - Start byte position on the original source.
    /// * `spent_time` - Time spent to resolve the record.
    /// * `match_flag` - Resolve action.
    pub fn build_value_bytes(input_start_pos: u64, input_end_pos: u64, spent_time: u64, match_flag: u8) -> [u8; Value::BYTES] {
        Value{
            input_start_pos,
            input_end_pos,
            data: Data{
                spent_time,
                match_flag: MatchFlag::try_from(match_flag).unwrap()
            }
        }.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod match_flag {
        use super::*;

        #[test]
        fn byte_sized() {
            assert_eq!(1, MatchFlag::BYTES)
        }

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

            assert_eq!(b'Y', u8::from(&MatchFlag::Yes));
            assert_eq!(b'N', u8::from(&MatchFlag::No));
            assert_eq!(b'S', u8::from(&MatchFlag::Skip));
            assert_eq!(0u8, u8::from(&MatchFlag::None));
        }

        #[test]
        fn display() {
            assert_eq!("Yes", MatchFlag::Yes.to_string());
            assert_eq!("No", MatchFlag::No.to_string());
            assert_eq!("Skip", MatchFlag::Skip.to_string());
            assert_eq!("", MatchFlag::None.to_string());
        }

        #[test]
        fn write_as_bytes() {
            let mut buf = [0u8];

            // test Yes
            let expected = [b'Y'];
            match MatchFlag::Yes.write_as_bytes(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test No
            let expected = [b'N'];
            match MatchFlag::No.write_as_bytes(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test Skip
            let expected = [b'S'];
            match MatchFlag::Skip.write_as_bytes(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test None
            let expected = [0u8];
            match MatchFlag::None.write_as_bytes(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
        }

        #[test]
        fn write_to() {
            // test Yes
            let expected = [b'Y'];
            let mut buf = [0u8];
            let mut writer = &mut buf as &mut [u8];
            match MatchFlag::Yes.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test No
            let expected = [b'N'];
            let mut buf = [0u8];
            let mut writer = &mut buf as &mut [u8];
            match MatchFlag::No.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test Skip
            let expected = [b'S'];
            let mut buf = [0u8];
            let mut writer = &mut buf as &mut [u8];
            match MatchFlag::Skip.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test None
            let expected = [0u8];
            let mut buf = [0u8];
            let mut writer = &mut buf as &mut [u8];
            match MatchFlag::None.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
        }
    }

    mod data {
        use super::*;
        use super::test_helper::*;

        #[test]
        fn new() {
            let expected = Data{
                match_flag: MatchFlag::None,
                spent_time: 0
            };
            assert_eq!(expected, Data::new())
        }

        #[test]
        fn byte_sized() {
            assert_eq!(9, Data::BYTES)
        }

        #[test]
        fn write_to_writer() {
            // first random try
            let expected = build_data_bytes(29034574985234, b'Y');
            let data = &Data{
                spent_time: 29034574985234,
                match_flag: MatchFlag::Yes
            };
            let mut buf = [0u8; Data::BYTES];
            let mut writer = &mut buf as &mut [u8];
            if let Err(e) = data.write_to(&mut writer) {
                assert!(false, "{:?}", e);
                return;
            };
            assert_eq!(expected, buf);

            // second random try
            let expected = build_data_bytes(98734951983457, b'N');
            let data = &Data{
                spent_time: 98734951983457,
                match_flag: MatchFlag::No
            };
            let mut buf = [0u8; Data::BYTES];
            let mut writer = &mut buf as &mut [u8];
            if let Err(e) = data.write_to(&mut writer) {
                assert!(false, "{:?}", e);
                return;
            };
            assert_eq!(expected, buf);
        }
    }

    mod value {
        use super::*;
        use test_helper::*;

        #[test]
        fn new() {
            assert_eq!(
                Value{
                    input_start_pos: 0,
                    input_end_pos: 0,
                    data: Data{
                        spent_time: 0,
                        match_flag: MatchFlag::None
                    }
                },
                Value::new()
            );
        }

        #[test]
        fn as_bytes() {
            // first test
            let expected = [
                // input start position
                12, 32, 43, 12, 75, 32, 65, 32,
                // input end position
                21, 43, 72, 74, 14, 75, 93, 48,
                // spent time
                34, 62, 94, 37, 48, 54, 38, 59,
                // match flag
                b'Y'
            ];
            let value = Value{
                input_start_pos: 873745659509883168,
                input_end_pos: 1525392381699644720,
                data: Data{
                    spent_time: 2467513159661266491,
                    match_flag: MatchFlag::Yes
                }
            };
            assert_eq!(expected, value.as_bytes());

            // second test
            let expected = [
                // input start position
                45, 38, 63, 17, 74, 20, 101, 67,
                // input end position
                111, 27, 84, 87, 21, 54, 23, 95,
                // spent time
                26, 28, 94, 99, 20, 104, 24, 64,
                // match flag
                b'N'
            ];

            // test value as_bytes function
            let value = Value{
                input_start_pos: 3253357124311606595,
                input_end_pos: 8006085495575943007,
                data: Data{
                    spent_time: 1881482523971164224,
                    match_flag: MatchFlag::No
                }
            };
            assert_eq!(expected, value.as_bytes());
        }

        #[test]
        fn read_input_from() {
            let input_buf = [
                // offset
                0, 0, 0,
                // input bytes
                23u8, 12u8, 25u8, 74u8,
                // extra bytes
                0, 0, 0, 0
            ];
            let rdr = &input_buf as &[u8];
            let mut reader = std::io::Cursor::new(rdr);
            let value = Value{
                input_start_pos: 3,
                input_end_pos: 6,
                data: Data{
                    spent_time: 0,
                    match_flag: MatchFlag::None
                }
            };
            let expected = vec![23u8, 12u8, 25u8, 74u8];
            let buf = match value.read_input_from(&mut reader) {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "expected {:?} but got error: {:?}", expected, e);
                    return;
                }
            };
            
            assert_eq!(expected, buf)
        }

        #[test]
        fn match_flag_byte_index() {
            // test Yes
            let mut value = Value::new();
            value.data.match_flag = MatchFlag::Yes;
            let buf = value.as_bytes();
            assert_eq!(b'Y', buf[Value::MATCH_FLAG_BYTE_INDEX]);

            // test No
            let mut value = Value::new();
            value.data.match_flag = MatchFlag::No;
            let buf = value.as_bytes();
            assert_eq!(b'N', buf[Value::MATCH_FLAG_BYTE_INDEX]);

            // test Skip
            let mut value = Value::new();
            value.data.match_flag = MatchFlag::Skip;
            let buf = value.as_bytes();
            assert_eq!(b'S', buf[Value::MATCH_FLAG_BYTE_INDEX]);

            // test None
            let mut value = Value::new();
            value.data.match_flag = MatchFlag::None;
            let buf = value.as_bytes();
            assert_eq!(0, buf[Value::MATCH_FLAG_BYTE_INDEX]);
        }

        #[test]
        fn byte_sized() {
            assert_eq!(25, Value::BYTES);
        }

        #[test]
        fn load_from_u8_slice() {
            let mut value = Value{
                input_start_pos: 0,
                input_end_pos: 0,
                data: Data{
                    spent_time: 0,
                    match_flag: MatchFlag::None
                }
            };

            // first random try
            let expected = Value{
                input_start_pos: 1400004,
                input_end_pos: 2341234,
                data: Data{
                    spent_time: 20777332,
                    match_flag: MatchFlag::Skip
                }
            };
            let buf = build_value_bytes(1400004, 2341234, 20777332, b'S');
            let mut reader = &buf as &[u8];
            if let Err(e) = value.load_from(&mut reader) {
                assert!(false, "shouldn't error out but got error: {:?}", e);
                return;
            };
            assert_eq!(expected, value);

            // second random try
            let expected = Value{
                input_start_pos: 445685221,
                input_end_pos: 34656435243,
                data: Data{
                    spent_time: 8427343298732,
                    match_flag: MatchFlag::None
                }
            };
            let buf = build_value_bytes(445685221, 34656435243, 8427343298732, 0);
            let mut reader = &buf as &[u8];
            if let Err(e) = value.load_from(&mut reader) {
                assert!(false, "shouldn't error out but got error: {:?}", e);
                return;
            };
            assert_eq!(expected, value);
        }

        #[test]
        fn load_from_u8_slice_with_invalid_smaller_buf_size() {
            let mut value = Value{
                input_start_pos: 0,
                input_end_pos: 0,
                data: Data{
                    spent_time: 0,
                    match_flag: MatchFlag::None
                }
            };

            let expected = std::io::ErrorKind::UnexpectedEof;
            let buf = [0u8; Value::BYTES-1];
            let mut reader = &buf as &[u8];
            match value.load_from(&mut reader) {
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
            let expected = Value{
                input_start_pos: 14321432,
                input_end_pos: 456542532,
                data: Data{
                    spent_time: 5463211,
                    match_flag: MatchFlag::No
                }
            };
            let buf = build_value_bytes(14321432, 456542532, 5463211, b'N');
            let value = match Value::from_byte_slice(&buf) {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "shouldn't error out but got error: {:?}", e);
                    return;
                }
            };
            assert_eq!(expected, value);

            // second random try
            let expected = Value{
                input_start_pos: 56745631532,
                input_end_pos: 45245234,
                data: Data{
                    spent_time: 11896524543541452385,
                    match_flag: MatchFlag::Yes
                }
            };
            let buf = build_value_bytes(56745631532, 45245234, 11896524543541452385, b'Y');
            let value = match Value::from_byte_slice(&buf) {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "shouldn't error out but got error: {:?}", e);
                    return;
                }
            };
            assert_eq!(expected, value);
        }

        #[test]
        fn read_from_reader() {
            // first random try
            let expected = Value{
                input_start_pos: 14321432,
                input_end_pos: 456542532,
                data: Data{
                    spent_time: 5463211,
                    match_flag: MatchFlag::No
                }
            };
            let buf = build_value_bytes(14321432, 456542532, 5463211, b'N');
            let mut reader = &buf as &[u8];
            let value = match Value::read_from(&mut reader) {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "shouldn't error out but got error: {:?}", e);
                    return;
                }
            };
            assert_eq!(expected, value);

            // second random try
            let expected = Value{
                input_start_pos: 56745631532,
                input_end_pos: 45245234,
                data: Data{
                    spent_time: 11896524543541452385,
                    match_flag: MatchFlag::Yes
                }
            };
            let buf = build_value_bytes(56745631532, 45245234, 11896524543541452385, b'Y');
            let mut reader = &buf as &[u8];
            let value = match Value::from_byte_slice(&mut reader) {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "shouldn't error out but got error: {:?}", e);
                    return;
                }
            };
            assert_eq!(expected, value);
        }

        #[test]
        fn try_from_u8_slice() {
            // first random try
            let expected = Value{
                input_start_pos: 14321432,
                input_end_pos: 456542532,
                data: Data{
                    spent_time: 5463211,
                    match_flag: MatchFlag::No
                }
            };
            let buf = build_value_bytes(14321432, 456542532, 5463211, b'N');
            let value = match Value::try_from(&buf[..]) {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "shouldn't error out but got error: {:?}", e);
                    return;
                }
            };
            assert_eq!(expected, value);

            // second random try
            let expected = Value{
                input_start_pos: 56745631532,
                input_end_pos: 45245234,
                data: Data{
                    spent_time: 11896524543541452385,
                    match_flag: MatchFlag::Yes
                }
            };
            let buf = build_value_bytes(56745631532, 45245234, 11896524543541452385, b'Y');
            let value = match Value::try_from(&buf[..]) {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "shouldn't error out but got error: {:?}", e);
                    return;
                }
            };
            assert_eq!(expected, value);
        }

        #[test]
        fn write_to_writer() {
            // first random try
            let expected = build_value_bytes(32464573645, 2343534543, 29034574985234, b'Y');
            let value = &Value{
                input_start_pos: 32464573645,
                input_end_pos: 2343534543,
                data: Data{
                    spent_time: 29034574985234,
                    match_flag: MatchFlag::Yes
                }
            };
            let mut buf = [0u8; Value::BYTES];
            let mut writer = &mut buf as &mut [u8];
            if let Err(e) = value.write_to(&mut writer) {
                assert!(false, "{:?}", e);
                return;
            };
            assert_eq!(expected, buf);

            // second random try
            let expected = build_value_bytes(789865473674, 83454327, 98734951983457, b'N');
            let value = &Value{
                input_start_pos: 789865473674,
                input_end_pos: 83454327,
                data: Data{
                    spent_time: 98734951983457,
                    match_flag: MatchFlag::No
                }
            };
            let mut buf = [0u8; Value::BYTES];
            let mut writer = &mut buf as &mut [u8];
            if let Err(e) = value.write_to(&mut writer) {
                assert!(false, "{:?}", e);
                return;
            };
            assert_eq!(expected, buf);
        }
    }
}