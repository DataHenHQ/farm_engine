use std::io::{Read, Write};
use std::convert::TryFrom;
use anyhow::Result;
use uuid::Uuid;
use crate::traits::{ByteSized, ReadFrom, WriteTo, LoadFrom};
use crate::db::field::Header as RecordHeader;
use super::Meta;

//// Describes a table file header.
#[derive(Debug, PartialEq, Clone)]
pub struct Header {
    pub meta: Meta,

    // Record header. It contains information about the fields.
    pub record: RecordHeader
}

impl Header {
    /// Creates a new header.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Table name.
    pub fn new(name: &str, uuid: Option<Uuid>) -> Result<Self> {
        Ok(Self{
            meta: Meta::new(name, uuid)?,
            record: RecordHeader::new()
        })
    }

    /// Return the previously calculated byte count to be writed when
    /// the header is converted into bytes.
    pub fn size_as_bytes(&self) -> u64 {
        Meta::BYTES as u64 + self.record.size_as_bytes()
    }
}

impl LoadFrom for Header {
    fn load_from(&mut self, reader: &mut impl Read) -> Result<()> {
        self.meta.load_from(reader)?;
        self.record.load_from(reader)
    }
}

impl ReadFrom for Header {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut header = Self::new("", Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?;
        header.load_from(reader)?;
        Ok(header)
    }
}

impl TryFrom<&[u8]> for Header {
    type Error = anyhow::Error;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        let mut header = Self::new("", Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?;
        let mut reader = buf;
        header.load_from(&mut reader)?;
        Ok(header)
    }
}

impl WriteTo for Header {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        self.meta.write_to(writer)?;
        self.record.write_to(writer)
    }
}

#[cfg(test)]
pub mod test_helper {
    use crate::db::field::FieldType;

    use super::*;

    /// Builds a table random uuid.
    pub fn table_uuid() -> Uuid {
        Uuid::new_v4()
    }

    /// Builds an table header as byte slice from the values provided.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Table name.
    /// * `record_count` - Total record count.
    pub fn build_header_bytes(name: &str, uuid: Option<Uuid>, record_count: u64, records: Vec<(String, FieldType)>) -> Vec<u8> {
        let mut header = Header{
            meta: Meta::new(name, uuid).unwrap(),
            record: RecordHeader::new()
        };
        header.meta.record_count = record_count;
        for (name, field_type) in records {
            header.record.add(name.as_str(), field_type).unwrap();
        }
        let mut buf: Vec<u8> = Vec::new();
        header.meta.write_to(&mut buf).unwrap();
        header.record.write_to(&mut buf).unwrap();
        buf
    }
}

#[cfg(test)]
mod tests {
    use crate::db::field::FieldType;

    use super::*;
    use test_helper::*;

    #[test]
    fn new() {
        let uuid = table_uuid();
        let expected = Header{
            meta: Meta::new("hello", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        match Header::new("hello", Some(uuid)) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn as_bytes() {
        // first test
        let expected: Vec<u8> = vec![
            // magic number
            100, 97, 116, 97, 104, 101, 110, 95, 116, 98, 108,
            // version
            0, 0, 0, 2,
            // record count = 2311457452320998632
            32, 19, 242, 78, 103, 5, 196, 232,
            // name size
            0, 0, 0, 8,
            // name value: "my_table"
            109, 121, 95, 116, 97, 98, 108, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            // uuid: "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8"
            161, 162, 163, 164, 177, 178, 193, 194, 209, 210, 211, 212, 213, 214, 215, 216,

            // field count
            0, 0, 0, 1u8,

            // foo field name value size
            0, 0, 0, 3u8,
            // foo field name value
            102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // foo field type
            11u8, 0, 0, 0, 0,
        ];

        // test header as_bytes function
        let mut header = Header{
            meta: Meta::new(
                "my_table",
                Some(Uuid::parse_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap())
            ).unwrap(),
            record: RecordHeader::new()
        };
        header.meta.record_count = 2311457452320998632;
        header.record.add("foo", FieldType::F64).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        match header.write_to(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected header instance but got error: {:?}", e)
            }
        };
        assert_eq!(expected, buf);

        // second test
        let expected: Vec<u8> = vec![
            // magic number
            100, 97, 116, 97, 104, 101, 110, 95, 116, 98, 108,
            // version
            0, 0, 0, 2,
            // record count = 4525325654675485867
            62, 205, 47, 180, 235, 228, 244, 171,
            // name size
            0, 0, 0, 9,
            // name value: "hellotbl"
            104, 101, 108, 108, 111, 95, 116, 98, 108, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            // uuid: "b1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8"
            177, 162, 163, 164, 177, 178, 193, 194, 209, 210, 211, 212, 213, 214, 215, 216,

            // field count
            0, 0, 0, 1u8,

            // bar field name value size
            0, 0, 0, 3u8,
            // bar field name value
            98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // bar field type
            12u8, 0, 0, 0, 45u8
        ];

        // test header as_bytes function
        let mut header = Header{
            meta: Meta::new(
                "hello_tbl",
                Some(Uuid::parse_str("b1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap())
            ).unwrap(),
            record: RecordHeader::new()
        };
        header.meta.record_count = 4525325654675485867;
        header.record.add("bar", FieldType::Str(45)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        match header.write_to(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected header instance but got error: {:?}", e)
            }
        };
        assert_eq!(expected, buf);
    }

    #[test]
    fn size_as_bytes() {
        let mut header = match Header::new("bar", Some(Uuid::new_v4())) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected header instance but got error: {:?}", e);
                return;
            }
        };
        header.record.add("foo", FieldType::U32).unwrap();
        assert_eq!(156, header.size_as_bytes());
    }

    #[test]
    fn load_from_u8_slice() {
        // first random try
        let uuid = table_uuid();
        let mut header = Header{
            meta: Meta::new("", None).unwrap(),
            record: RecordHeader::new()
        };
        let mut expected = Header{
            meta: Meta::new("my_table", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        expected.meta.record_count = 4535435;
        expected.record.add("bar", FieldType::U8).unwrap();
        let buf = build_header_bytes("my_table", Some(uuid), 4535435, vec![("bar".to_string(), FieldType::U8)]);
        let mut reader = &buf as &[u8];
        if let Err(e) = header.load_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        };
        assert_eq!(expected.meta, header.meta);
        assert_eq!(expected.record.len(), header.record.len());
        assert_eq!(1, header.record.len());
        let field = header.record.get_by_index(0).unwrap();
        let expected_field = expected.record.get_by_index(0).unwrap();
        assert_eq!(expected_field.get_name(), field.get_name());
        assert_eq!(expected_field.get_type(), field.get_type());
        assert_eq!(expected, header);

        // second random try
        let uuid = table_uuid();
        let mut header = Header{
            meta: Meta::new("", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        let mut expected = Header{
            meta: Meta::new("hello_tbl", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        expected.meta.record_count = 6572646535124;
        expected.record.add("abc", FieldType::U32).unwrap();
        let buf = build_header_bytes("hello_tbl", Some(uuid), 6572646535124, vec![("abc".to_string(), FieldType::U32)]);
        let mut reader = &buf as &[u8];
        if let Err(e) = header.load_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
            return;
        };
        assert_eq!(expected.meta, header.meta);
        assert_eq!(expected.record.len(), header.record.len());
        assert_eq!(1, header.record.len());
        let field = header.record.get_by_index(0).unwrap();
        let expected_field = expected.record.get_by_index(0).unwrap();
        assert_eq!(expected_field.get_name(), field.get_name());
        assert_eq!(expected_field.get_type(), field.get_type());
        assert_eq!(expected, header);
    }

    #[test]
    fn read_from_reader() {
        // first random try
        let uuid = table_uuid();
        let mut expected = Header{
            meta: Meta::new("my_table", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        expected.meta.record_count = 2341234;
        expected.record.add("fff", FieldType::Bool).unwrap();
        let buf = build_header_bytes("my_table", Some(uuid), 2341234, vec![("fff".to_string(), FieldType::Bool)]);
        let mut reader = &buf as &[u8];
        let header = match Header::read_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected.meta, header.meta);
        assert_eq!(expected.record.len(), header.record.len());
        assert_eq!(1, header.record.len());
        let field = header.record.get_by_index(0).unwrap();
        let expected_field = expected.record.get_by_index(0).unwrap();
        assert_eq!(expected_field.get_name(), field.get_name());
        assert_eq!(expected_field.get_type(), field.get_type());
        assert_eq!(expected, header);

        // second random try
        let uuid = table_uuid();
        let mut expected = Header{
            meta: Meta::new("hello_tbl", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        expected.meta.record_count = 9879873495743;
        expected.record.add("aaa", FieldType::F64).unwrap();
        expected.record.add("bbb", FieldType::I8).unwrap();
        let buf = build_header_bytes(
            "hello_tbl",
            Some(uuid),
            9879873495743,
            vec![("aaa".to_string(), FieldType::F64), ("bbb".to_string(), FieldType::I8)]
        );
        let mut reader = &buf as &[u8];
        let header = match Header::read_from(&mut reader) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected.meta, header.meta);
        assert_eq!(expected.record.len(), header.record.len());
        assert_eq!(2, header.record.len());
        let field = header.record.get_by_index(0).unwrap();
        let expected_field = expected.record.get_by_index(0).unwrap();
        assert_eq!(expected_field.get_name(), field.get_name());
        assert_eq!(expected_field.get_type(), field.get_type());
        let field = header.record.get_by_index(1).unwrap();
        let expected_field = expected.record.get_by_index(1).unwrap();
        assert_eq!(expected_field.get_name(), field.get_name());
        assert_eq!(expected_field.get_type(), field.get_type());
        assert_eq!(expected, header);
    }

    #[test]
    fn try_from_u8_slice() {
        // first random try
        let uuid = table_uuid();
        let mut expected = Header{
            meta: Meta::new("my_table", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        expected.meta.record_count = 32412342134234;
        let buf = build_header_bytes("my_table", Some(uuid), 32412342134234, vec![]);
        let header = match Header::try_from(&buf[..]) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected.meta, header.meta);
        assert_eq!(expected.record.len(), header.record.len());
        assert_eq!(0, header.record.len());
        assert_eq!(expected, header);

        // second random try
        let uuid = table_uuid();
        let mut expected = Header{
            meta: Meta::new("hello_tbl", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        expected.meta.record_count = 56535423143214;
        expected.record.add("ccc", FieldType::I16).unwrap();
        let buf = build_header_bytes("hello_tbl", Some(uuid), 56535423143214, vec![("ccc".to_string(), FieldType::I16)]);
        let header = match Header::try_from(&buf[..]) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected success but got error: {:?}", e);
                return;
            }
        };
        assert_eq!(expected.meta, header.meta);
        assert_eq!(expected.record.len(), header.record.len());
        assert_eq!(1, header.record.len());
        let field = header.record.get_by_index(0).unwrap();
        let expected_field = expected.record.get_by_index(0).unwrap();
        assert_eq!(expected_field.get_name(), field.get_name());
        assert_eq!(expected_field.get_type(), field.get_type());
        assert_eq!(expected, header);
    }

    #[test]
    fn write_to_writer() {
        // first random try
        let uuid = table_uuid();
        let expected = build_header_bytes("my_table", Some(uuid), 788477630402843, vec![("ddd".to_string(), FieldType::I32)]);
        let mut header = Header{
            meta: Meta::new("my_table", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        header.meta.record_count = 788477630402843;
        header.record.add("ddd", FieldType::I32).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        if let Err(e) = header.write_to(&mut buf) {
            assert!(false, "{:?}", e);
            return;
        };
        assert_eq!(expected, buf);

        // second random try
        let uuid = table_uuid();
        let expected = build_header_bytes("hello_tbl", Some(uuid), 63439320337562938, vec![]);
        let mut header = Header{
            meta: Meta::new("hello_tbl", Some(uuid)).unwrap(),
            record: RecordHeader::new()
        };
        header.meta.record_count = 63439320337562938;
        let mut buf: Vec<u8> = Vec::new();
        if let Err(e) = header.write_to(&mut buf) {
            assert!(false, "{:?}", e);
            return;
        };
        assert_eq!(expected, buf);
    }
}