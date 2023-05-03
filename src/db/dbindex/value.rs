use serde::{Serialize, Deserialize};
use std::io::{Seek, SeekFrom, Read, Write};
use std::convert::TryFrom;
use anyhow::{bail, Result};
use crate::error::ParseError;
use crate::traits::{ByteSized, FromByteSlice, WriteAsBytes, ReadFrom, WriteTo, LoadFrom};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Gid{
    value: String
}

impl Gid {
    pub fn get(&self)-> &str{
        &self.value
    }

    pub fn set(&mut self, value: String) -> Result<()>{
        // validate string value
        let value_size = value.as_bytes().len();
        if value_size > Self::BYTES {
            bail!(
                "string value size ({} bytes) is bigger than field size ({} bytes)",
                value_size,
                Self::BYTES
            );
        }
        // write value
        self.value = value;
        Ok(())
    }

    pub fn new(value: &str) -> Self{
        Self{value:value.to_string()}
    }


}

impl std::fmt::Display for Gid {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl ByteSized for Gid {
    const BYTES: usize = 46;
}

impl FromByteSlice for Gid {
    fn from_byte_slice(buf: &[u8]) -> Result<Self> {
        // validate buf size
//println!("{:#?}", String::from_utf8_lossy(buf));
        if buf.len() != Self::BYTES {
            bail!(ParseError::InvalidSize);
        }

        let value_size = u64::from_byte_slice(&buf[..u64::BYTES])? as usize;
        let carry = u64::BYTES;

        if value_size < 1 {
            return Ok(Self{value: "".to_string()})
          }

        // convert bytes into the type value
        let mut bytes = vec![0u8; value_size];
        bytes.copy_from_slice(&buf[carry..carry+value_size]);
        let value = match String::from_utf8(bytes.to_vec()) {
            Ok(v) => v,
            Err(ee) => bail!(ParseError::ParseString) 
        };
        Ok(Self{value})
    }
}

impl ReadFrom for Gid {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        // read and convert bytes into the type value
        let mut buf = [0u8; Self::BYTES];
        reader.read_exact(&mut buf)?;
        let value = match String::from_utf8(buf.to_vec()) {
            Ok(v) => v,
            Err(ee) => bail!(ParseError::ParseString) 
        };
        Ok(Self{value})
    }
}

impl WriteAsBytes for Gid {
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()> {
//println!("{}", buf.len())        ;
        // validate value size
        if buf.len() != Gid::BYTES {
            bail!(ParseError::InvalidSize);
        }

        // save value as bytes
        // validate string value
        let value_buf = self.value.as_bytes();
        let value_size = value_buf.len();
        if value_size > Self::BYTES {
            bail!(
                "string value size ({} bytes) is bigger than gid size ({} bytes)",
                value_size,
                Self::BYTES
            );
        }
//println!("{:#?}",self.value.as_bytes());
        // write value
        let size_buf = &mut buf[..u64::BYTES];
        (value_size as u64).write_as_bytes(size_buf)?;

        if value_buf.len() > 0 {
            let sub_buf = &mut buf[u64::BYTES..u64::BYTES+value_size];
            sub_buf.copy_from_slice(&value_buf);           
        }

        let data_size = value_size+u64::BYTES;
        if data_size < Self::BYTES {
            // fill with zeros
            let mut zero_buf = &mut buf[data_size..];
            zero_buf.copy_from_slice(&vec![0u8; (Self::BYTES - data_size) as usize]);
        }
    
        Ok(())
    }
}

impl WriteTo for Gid {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {  
        // save value as bytes
        // validate string value
        let value_buf = self.value.as_bytes();
        let value_size = value_buf.len();
        if value_size > Self::BYTES {
            bail!(
                "string value size ({} bytes) is bigger than gid size ({} bytes)",
                value_size,
                Self::BYTES
            );
        }
        // write value
        writer.write_all(value_buf)?;
        if value_size < Self::BYTES {
            // fill with zeros
            writer.write_all(&vec![0u8; (Self::BYTES - value_size) as usize])?;
        }    
        Ok(())
    }
}


impl ByteSized for String {
    const BYTES: usize = 38;
}

/// Status flag enumerator.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
pub enum StatusFlag {
    Yes = b'Y' as isize,
    No = b'N' as isize,
    Skip = b'S' as isize,
    None = 0
}

impl StatusFlag {
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

    /// Joins an array into a string by using a separator.
    /// 
    /// NOTE: Convert me into Join<Trait> once stable.
    pub fn join<'a>(slice: &[Self], sep: &str) -> String {
        let mut buf: Vec<u8> = Vec::new();
        if slice.len() < 1 {
            return "".to_string();
        }
        let mut iter = slice.iter();
        buf.push(iter.next().unwrap().into());
        for value in slice {
            for char in sep.bytes() {
                buf.push(char);
            }
            buf.push(value.into());
        }
        String::from_utf8_lossy(&buf).to_string()
    }
}

impl std::fmt::Display for StatusFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", match self {
            Self::Yes => "Yes",
            Self::No => "No",
            Self::Skip => "Skip",
            Self::None => ""
        })
    }
}

impl TryFrom<u8> for StatusFlag {
    type Error = ParseError;

    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
        let status_flag = match v {
            b'Y' => Self::Yes,
            b'N' => Self::No,
            b'S' => Self::Skip,
            0 => Self::None,
            _ => return Err(ParseError::InvalidFormat)
        };

        Ok(status_flag)
    }
}

impl From<&StatusFlag> for u8 {
    fn from(v: &StatusFlag) -> Self {
        match v {
            StatusFlag::Yes => b'Y',
            StatusFlag::No => b'N',
            StatusFlag::Skip => b'S',
            StatusFlag::None => 0
        }
    }
}

impl From<StatusFlag> for u8 {
    fn from(v: StatusFlag) -> Self {
        (&v).into()
    }
}

impl ByteSized for StatusFlag {
    const BYTES: usize = 1;
}

impl WriteAsBytes for StatusFlag {
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

impl WriteTo for StatusFlag {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        writer.write_all(&[self.into()])?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Data {
    /// Status flag for the value.
    pub status_flag: StatusFlag,

    /// Spent time to resolve. The time unit must be handle by the dev.
    pub spent_time: u64
    , pub parent: u64   // --> Ale
    , pub left_node: u64   // --> Ale 
    , pub right_node: u64   // --> Ale 
    , pub gid: Gid   // --> Ale
    , pub height:i64

}

impl Data {
    /// Creates a new data instance.
    pub fn new() -> Self {
        Data{
            status_flag: StatusFlag::None,
            spent_time: 0,
            parent: 0,   // --> Ale 
            left_node: 0,   // --> Ale
            right_node: 0   // --> Ale 
            ,gid: Gid::new("")   // --> Ale 
            ,height: 0   // --> Ale 
        }
    }
}

impl ByteSized for Data {
    /// Index data size in bytes.
    /// 
    /// Byte format
    /// `<status_flag:1><spent_time:8><parent:8><left_node:8><right_node:8><height:1><gid:46>`
    const BYTES: usize = 87;//49; // --> Ale const BYTES: usize = 33;    // --> Ale const BYTES: usize = 9;
}

impl WriteAsBytes for Data {
    fn write_as_bytes(&self, buf: &mut [u8]) -> Result<()> {
        // validate value size
        if buf.len() != Self::BYTES {
            bail!(ParseError::InvalidSize);
        }

let mut carry = 0;//u64::BYTES;
//println!("{}","aaaaaa");
//println!("{:?}",&buf[..]);
//println!("{:?}",&buf[carry]);        
//println!("{:?} SP",&buf[carry..carry+u64::BYTES]);

        // save spent_time
        self.spent_time.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;  // --> Ale  self.spent_time.write_as_bytes(&mut buf[..u64::BYTES])?;
        
        carry += u64::BYTES;
                
        // save status flag
        buf[carry] = self.status_flag.into();

        carry += StatusFlag::BYTES;


//println!("{:?} P",&buf[carry..carry+u64::BYTES]);
        // save parent
        // este es el que funciona 
        //self.parent.write_as_bytes(&mut buf[u64::BYTES+1..])?;  // --> Ale  self.spent_time.write_as_bytes(&mut buf[..u64::BYTES])?;
        self.parent.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;
//println!("{:?} L",&buf[carry..carry+u64::BYTES]);

        // save left_node
        self.left_node.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;
//println!("{:?} R",&buf[carry..carry+u64::BYTES]);

        // save right_node
        self.right_node.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

//println!("{:?} H",&buf[carry..carry+u64::BYTES]);
        // save height
        self.height.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

//println!("{:?} G",&buf[carry..carry+u64::BYTES]);
        // save gid
        self.gid.write_as_bytes(&mut buf[carry..carry+Gid::BYTES])?;
         

        

        /* --> Ale se cambio con tato para que el orden fuera diferente
        
        let mut carry = 0;//u64::BYTES;
//println!("{}","aaaaaa");
//println!("{:?}",&buf[..]);
//println!("{:?}",&buf[carry]);

        // save status flag
        buf[carry] = self.status_flag.into();

        carry += StatusFlag::BYTES;

//println!("{:?} SP",&buf[carry..carry+u64::BYTES]);
        // save spent_time
        self.spent_time.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;  // --> Ale  self.spent_time.write_as_bytes(&mut buf[..u64::BYTES])?;
        
        carry += u64::BYTES;

//println!("{:?} P",&buf[carry..carry+u64::BYTES]);
        // save parent
        // este es el que funciona 
        //self.parent.write_as_bytes(&mut buf[u64::BYTES+1..])?;  // --> Ale  self.spent_time.write_as_bytes(&mut buf[..u64::BYTES])?;
        self.parent.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;
//println!("{:?} L",&buf[carry..carry+u64::BYTES]);

        // save left_node
        self.left_node.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;
//println!("{:?} R",&buf[carry..carry+u64::BYTES]);

        // save right_node
        self.right_node.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

//println!("{:?} H",&buf[carry..carry+u64::BYTES]);
        // save height
        self.height.write_as_bytes(&mut buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;

//println!("{:?} G",&buf[carry..carry+u64::BYTES]);
        // save gid
        self.gid.write_as_bytes(&mut buf[carry..carry+Gid::BYTES])?;
         */
        Ok(())
    }
}

impl WriteTo for Data {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        // write spent time
        self.spent_time.write_to(writer)?;
        // write status flag
        self.status_flag.write_to(writer)?;
        // write parent
        self.parent.write_to(writer)?;  // --> Ale
        // write left_node
        self.left_node.write_to(writer)?;  // --> Ale
        // write right_node
        self.right_node.write_to(writer)?;  // --> Ale
        // write height
        self.height.write_to(writer)?;  // --> Ale
        // write gid
        self.gid.write_to(writer)?;  // --> Ale

        Ok(())
    }
}

/// Describes an Indexer file value.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Value {
    /// Input file start position for the record.
    pub input_start_pos: u64,

    /// Input file end position for the record.
    pub input_end_pos: u64,

    /// Index data.
    pub data: Data
}

impl Value {
    /// Status flag byte index when as bytes.
    pub const STATUS_FLAG_BYTE_INDEX: usize = 24;

    /// Data byte offset.
    pub const DATA_OFFSET: usize = u64::BYTES*2;

    /// Creates a new value.
    pub fn new() -> Self {
        Self{
            input_start_pos: 0,
            input_end_pos: 0,
            data: Data{
                status_flag: StatusFlag::None
                ,spent_time: 0
                ,parent: 0 // --> Ale
                ,left_node:0 // --> Ale
                ,right_node:0 // --> Ale
                ,height:0 // --> Ale
                ,gid:Gid::new("") // --> Ale
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
//println!(" BUF:{:?}",&buf[..]);
//println!(" start:{:?}",&buf[carry]);
        // read input start pos
        let input_start_pos = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;
//println!(" end:{:?}",&buf[carry]);
        // read input end pos
        let input_end_pos = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;  
//println!(" spent:{:?}",&buf[carry..carry+u64::BYTES]);
        // read spent type
        let spent_time = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?;
        carry += u64::BYTES;
//println!(" status:{:?}",&buf[carry]);
        // read status flag
        let status_flag = buf[carry].try_into()?;
        carry += StatusFlag::BYTES;


//println!(" parent:{:?}",&buf[carry..carry+u64::BYTES]);
        // read parent  // --> Ale
        let parent = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?; // --> Ale
        carry += u64::BYTES;
//println!(" left:{:?}",&buf[carry..carry+u64::BYTES]);
        // read left_node  // --> Ale
        //let left_node = u64::from_byte_slice(&buf[carry..])?; // --> Ale                
        let left_node = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?; // --> Ale
        carry += u64::BYTES;
//println!(" right: {:?}",&buf[carry..carry+u64::BYTES]);
        // read right_node  // --> Ale
        let right_node = u64::from_byte_slice(&buf[carry..carry+u64::BYTES])?; // --> Ale
        carry += u64::BYTES;
//println!(" height: {:?}",&buf[carry..carry+i64::BYTES]);
        // read height  // --> Ale
        let height = i64::from_byte_slice(&buf[carry..carry+i64::BYTES])?; // --> Ale
        carry += i64::BYTES;
//println!(" gid: {:?}",&buf[carry..]);
        // read gid  // --> Ale
        let gid = Gid::from_byte_slice(&buf[carry..])?; // --> Ale
           
        // record index value data
        self.input_start_pos = input_start_pos;
        self.input_end_pos = input_end_pos;
        self.data.status_flag = status_flag;
        self.data.spent_time = spent_time;
        self.data.parent = parent; // --> Ale
        self.data.left_node=left_node;
        self.data.right_node=right_node;
        self.data.height = height;
        self.data.gid=gid;

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
    use std::os::unix::process::parent_id;

    use super::*;

    /// Build a index value as byte slice from the values provided.
    /// 
    /// # Arguments
    /// 
    /// * `spent_time` - Time spent to resolve the record.
    /// * `status_flag` - Resolve action.
    /// * `parent` - Resolve action.  // --> Ale
    pub fn build_data_bytes(spent_time: u64, status_flag: u8,parent: u64,left_node: u64,right_node: u64,height:i64,gid: Gid) -> [u8; Data::BYTES] {
        let mut buf = [0u8; Data::BYTES];
        let mut carry = 0;
        spent_time.write_as_bytes(&mut buf[carry..carry+u64::BYTES]).unwrap();
        carry += u64::BYTES;
        buf[carry] = status_flag;
        carry += StatusFlag::BYTES;
        parent.write_as_bytes(&mut buf[carry..carry+u64::BYTES]).unwrap();         
        carry += u64::BYTES;
        left_node.write_as_bytes(&mut buf[carry..carry+u64::BYTES]).unwrap();
        carry += u64::BYTES;
        right_node.write_as_bytes(&mut buf[carry..carry+u64::BYTES]).unwrap();
        carry += u64::BYTES;
        height.write_as_bytes(&mut buf[carry..carry+i64::BYTES]).unwrap();
        carry += i64::BYTES;
        gid.write_as_bytes(&mut buf[carry..carry+Gid::BYTES]).unwrap();
         // --> Ale, esto no estaba en las version funcional
        buf
    }

    /// Build a index value as byte slice from the values provided.
    /// 
    /// # Arguments
    /// 
    /// * `input_start_pos` - Start byte position on the original source.
    /// * `input_end_pos` - Start byte position on the original source.
    /// * `spent_time` - Time spent to resolve the record.
    /// * `status_flag` - Resolve action.
    pub fn build_value_bytes(input_start_pos: u64, input_end_pos: u64, spent_time: u64, status_flag: u8, parent: u64, left_node: u64, right_node: u64, gid: Gid, height:i64) -> [u8; Value::BYTES] {
        Value{
            input_start_pos,
            input_end_pos,
            data: Data{
                spent_time,
                status_flag: StatusFlag::try_from(status_flag).unwrap()
                ,parent // --> Ale
                ,left_node // --> Ale
                ,right_node // --> Ale
                ,gid // --> Ale
                ,height // --> Ale
            }
        }.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod status_flag {
        use super::*;

        #[test]
        fn byte_sized() {
            assert_eq!(1, StatusFlag::BYTES)
        }

        #[test]
        fn try_from_u8() {
            match StatusFlag::try_from(b'Y') {
                Ok(v) => assert_eq!(StatusFlag::Yes, v),
                Err(_) => assert!(false, "should be Ok(StatusFlag::Yes)")
            }
            match StatusFlag::try_from(b'N') {
                Ok(v) => assert_eq!(StatusFlag::No, v),
                Err(_) => assert!(false, "should be Ok(StatusFlag::No)")
            }
            match StatusFlag::try_from(b'S') {
                Ok(v) => assert_eq!(StatusFlag::Skip, v),
                Err(_) => assert!(false, "should be Ok(StatusFlag::Skip)")
            }
            match StatusFlag::try_from(0u8) {
                Ok(v) => assert_eq!(StatusFlag::None, v),
                Err(_) => assert!(false, "should be Ok(StatusFlag::None)")
            }
            match StatusFlag::try_from(b'a') {
                Ok(_) => assert!(false, "should be an Err(ParseError::InvalidFormat)"),
                Err(e) => assert!(
                    if let ParseError::InvalidFormat = e { true } else { false },
                    "should be an Err(ParseError::InvalidFormat)"
                )
            }
        }

        #[test]
        fn into_u8() {
            assert_eq!(b'Y', u8::from(StatusFlag::Yes));
            assert_eq!(b'N', u8::from(StatusFlag::No));
            assert_eq!(b'S', u8::from(StatusFlag::Skip));
            assert_eq!(0u8, u8::from(StatusFlag::None));

            assert_eq!(b'Y', u8::from(&StatusFlag::Yes));
            assert_eq!(b'N', u8::from(&StatusFlag::No));
            assert_eq!(b'S', u8::from(&StatusFlag::Skip));
            assert_eq!(0u8, u8::from(&StatusFlag::None));
        }

        #[test]
        fn display() {
            assert_eq!("Yes", StatusFlag::Yes.to_string());
            assert_eq!("No", StatusFlag::No.to_string());
            assert_eq!("Skip", StatusFlag::Skip.to_string());
            assert_eq!("", StatusFlag::None.to_string());
        }

        #[test]
        fn write_as_bytes() {
            let mut buf = [0u8];

            // test Yes
            let expected = [b'Y'];
            match StatusFlag::Yes.write_as_bytes(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test No
            let expected = [b'N'];
            match StatusFlag::No.write_as_bytes(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test Skip
            let expected = [b'S'];
            match StatusFlag::Skip.write_as_bytes(&mut buf) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test None
            let expected = [0u8];
            match StatusFlag::None.write_as_bytes(&mut buf) {
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
            match StatusFlag::Yes.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test No
            let expected = [b'N'];
            let mut buf = [0u8];
            let mut writer = &mut buf as &mut [u8];
            match StatusFlag::No.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test Skip
            let expected = [b'S'];
            let mut buf = [0u8];
            let mut writer = &mut buf as &mut [u8];
            match StatusFlag::Skip.write_to(&mut writer) {
                Ok(()) => assert_eq!(expected, buf),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test None
            let expected = [0u8];
            let mut buf = [0u8];
            let mut writer = &mut buf as &mut [u8];
            match StatusFlag::None.write_to(&mut writer) {
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
                status_flag: StatusFlag::None,
                spent_time: 0
                ,parent: 0 // --> Ale
                ,left_node:0 // --> Ale
                ,right_node:0 // --> Ale
                ,gid: Gid::new("") // --> Ale
                ,height:0 // --> Ale
            };
            assert_eq!(expected, Data::new())
        }

        #[test]
        fn byte_sized() {
            assert_eq!(87, Data::BYTES)  // --> Ale assert_eq!(9, Data::BYTES)
        }

        #[test]
        fn write_to_writer() {
            // first random try
            let expected = build_data_bytes(29034574985234, b'Y',23, 0,0,0,Gid::new(""));
            let data = &Data{
                spent_time: 29034574985234,
                status_flag: StatusFlag::Yes
                ,parent: 23 // --> Ale
                ,left_node:0 // --> Ale
                ,right_node:0 // --> Ale
                ,gid: Gid::new("") // --> Ale
                ,height:0 // --> Ale
            };
            let mut buf = [0u8; Data::BYTES];
            let mut writer = &mut buf as &mut [u8];
            if let Err(e) = data.write_to(&mut writer) {
                assert!(false, "{:?}", e);
                return;
            };
            assert_eq!(expected, buf);

            // second random try
            let expected = build_data_bytes(98734951983457, b'N',24,0,0,0,Gid::new(""));
            let data = &Data{
                spent_time: 98734951983457,
                status_flag: StatusFlag::No
                ,parent: 24 // --> Ale
                ,left_node:0 // --> Ale
                ,right_node:0 // --> Ale
                ,gid: Gid::new("") // --> Ale
                ,height:0 // --> Ale
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
                        status_flag: StatusFlag::None
                        ,parent: 0 // --> Ale
                        ,left_node:0 // --> Ale
                        ,right_node:0 // --> Ale
                        ,gid: Gid::new("") // --> Ale
                        ,height:0 // --> Ale
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
                34, 62, 94, 37, 48, 54, 38, 59   ,             
                // status flag
                b'Y'
                // parent  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale   ,0, 0, 0, 0, 0, 0, 0, 150u8
                // left_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // right_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // height
                ,0, 0, 0, 0, 0, 0, 0,0   // --> Ale
                // gid  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0,3,50
                , 53, 52, 0, 0, 0, 0,0 ,0, 0, 0, 0, 0, 0, 0,0 ,0, 0, 0, 0, 0, 0, 0,0 ,0, 0, 0, 0, 0, 0
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
            ];
            println!("{:?}", Gid::new("254"));
            let value = Value{
                input_start_pos: 873745659509883168,
                input_end_pos: 1525392381699644720,
                data: Data{
                    spent_time: 2467513159661266491,
                    status_flag: StatusFlag::Yes
                    ,parent: 0 // --> Ale
                    ,left_node:0 // --> Ale
                    ,right_node:0 // --> Ale
                    ,gid:Gid::new("254") // --> Ale
                    ,height:0 // --> Ale
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
                // status flag
                b'N'
                // parent  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale   --- compararlo
                // left_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // right_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // height
                ,0, 0, 0, 0, 0, 0, 0,0   // --> Ale
                // gid  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0,20, 
                49, 56, 52, 52, 54, 55, 52, 52, 
                48, 55, 51, 55, 48, 57, 53, 53, 
                49, 54, 49, 52, 0, 0, 0, 0, 
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0   // --> Ale
            ];

            // test value as_bytes function
            let value = Value{
                input_start_pos: 3253357124311606595,
                input_end_pos: 8006085495575943007,
                data: Data{
                    spent_time: 1881482523971164224,
                    status_flag: StatusFlag::No
                    ,parent: 0 // --> Ale
                    ,left_node:0 // --> Ale
                    ,right_node:0 // --> Ale
                    ,height:0 // --> Ale
                    ,gid: Gid::new("18446744073709551614") // --> Ale
                    
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
                    status_flag: StatusFlag::None
                    ,parent: 0 // --> Ale
                    ,left_node:0 // --> Ale
                    ,right_node:0 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
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
        fn status_flag_byte_index() {
            // test Yes
            let mut value = Value::new();
            value.data.status_flag = StatusFlag::Yes;
            let buf = value.as_bytes();
            assert_eq!(b'Y', buf[Value::STATUS_FLAG_BYTE_INDEX]);

            // test No
            let mut value = Value::new();
            value.data.status_flag = StatusFlag::No;
            let buf = value.as_bytes();
            assert_eq!(b'N', buf[Value::STATUS_FLAG_BYTE_INDEX]);

            // test Skip
            let mut value = Value::new();
            value.data.status_flag = StatusFlag::Skip;
            let buf = value.as_bytes();
            assert_eq!(b'S', buf[Value::STATUS_FLAG_BYTE_INDEX]);

            // test None
            let mut value = Value::new();
            value.data.status_flag = StatusFlag::None;
            let buf = value.as_bytes();
            assert_eq!(0, buf[Value::STATUS_FLAG_BYTE_INDEX]);
        }

        #[test]
        fn byte_sized() {
            assert_eq!(103, Value::BYTES);   // --> Ale assert_eq!(25, Value::BYTES);
        }

        #[test]
        fn load_from_u8_slice() {
            let mut value = Value{
                input_start_pos: 0,
                input_end_pos: 0,
                data: Data{
                    spent_time: 0,
                    status_flag: StatusFlag::None
                    ,parent: 0 // --> Ale
                    ,left_node:0 // --> Ale
                    ,right_node:0 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
                }
            };

            // first random try
            let expected = Value{
                input_start_pos: 1400004,
                input_end_pos: 2341234,
                data: Data{
                    spent_time: 20777332,
                    status_flag: StatusFlag::Skip
                    ,parent: 0 // --> Ale
                    ,left_node:23 // --> Ale
                    ,right_node:0 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            let buf = build_value_bytes(1400004, 2341234, 20777332, b'S',0,23,0,Gid::new(""),0);
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
                    status_flag: StatusFlag::None
                    ,parent: 0 // --> Ale
                    ,left_node:0 // --> Ale
                    ,right_node:0 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            let buf = build_value_bytes(445685221, 34656435243, 8427343298732, 0,0,0,0,Gid::new(""),0);
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
                    status_flag: StatusFlag::None
                    ,parent: 0 // --> Ale
                    ,left_node:0 // --> Ale
                    ,right_node:0 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
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
                    status_flag: StatusFlag::No
                    ,parent: 44 // --> Ale
                    ,left_node:0 // --> Ale
                    ,right_node:0 // --> Ale
                    ,gid:Gid::new("1") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            let buf = build_value_bytes(14321432, 456542532, 5463211, b'N',44,0,0,Gid::new("1"),0);
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
                    status_flag: StatusFlag::Yes
                    ,parent: 0 // --> Ale
                    ,left_node:0 // --> Ale
                    ,right_node:33 // --> Ale
                    ,gid:Gid::new("7") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            let buf = build_value_bytes(56745631532, 45245234, 11896524543541452385, b'Y',0,0,33,Gid::new("7"),0);
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
                    status_flag: StatusFlag::No
                    ,parent: 0 // --> Ale
                    ,left_node:11 // --> Ale
                    ,right_node:22 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            let buf = build_value_bytes(14321432, 456542532, 5463211, b'N',0,11,22,Gid::new(""),0);
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
                    status_flag: StatusFlag::Yes
                    ,parent: 0 // --> Ale
                    ,left_node:0 // --> Ale
                    ,right_node:0 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            let buf = build_value_bytes(56745631532, 45245234, 11896524543541452385, b'Y',0,0,0,Gid::new(""),0);
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
                    status_flag: StatusFlag::No
                    ,parent: 1123 // --> Ale
                    ,left_node:4456 // --> Ale
                    ,right_node:7789 // --> Ale
                    ,gid:Gid::new("15") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            let buf = build_value_bytes(14321432, 456542532, 5463211, b'N',1123,4456,7789,Gid::new("15"),0);
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
                    status_flag: StatusFlag::Yes
                    ,parent: 7894 // --> Ale
                    ,left_node:4561 // --> Ale
                    ,right_node:1230 // --> Ale
                    ,gid:Gid::new("456") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            let buf = build_value_bytes(56745631532, 45245234, 11896524543541452385, b'Y',7894,4561,1230,Gid::new("456"),0);
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
            let expected = build_value_bytes(32464573645, 2343534543, 29034574985234, b'Y',1234,4567,789,Gid::new("50"),0);
            let value = &Value{
                input_start_pos: 32464573645,
                input_end_pos: 2343534543,
                data: Data{
                    spent_time: 29034574985234,
                    status_flag: StatusFlag::Yes
                    ,parent: 1234 // --> Ale
                    ,left_node:4567 // --> Ale
                    ,right_node:789 // --> Ale
                    ,gid:Gid::new("50") // --> Ale
                    ,height:0 // --> Ale
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
            let expected = build_value_bytes(789865473674, 83454327, 98734951983457, b'N',1234,4567,1478,Gid::new(""),0);
            let value = &Value{
                input_start_pos: 789865473674,
                input_end_pos: 83454327,
                data: Data{
                    spent_time: 98734951983457,
                    status_flag: StatusFlag::No
                    ,parent: 1234 // --> Ale
                    ,left_node:4567 // --> Ale
                    ,right_node:1478 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
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