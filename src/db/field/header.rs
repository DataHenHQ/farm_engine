use anyhow::{bail, Result};
use indexmap::IndexMap;
use std::io::{Read, Write};
use crate::traits::{ByteSized, ReadFrom, WriteTo, LoadFrom};
use super::FieldType;
use super::Value;
use super::Field;
use super::Record;

/// Represent the record header. Byte format: `<field_count:1><fields:?>`
#[derive(Debug, PartialEq, Clone)]
pub struct Header {
    _fields: IndexMap<String, Field>,
    _record_byte_size: u64
}

impl Header {
    /// Create a new instance.
    pub fn new() -> Self {
        Self{
            _fields: IndexMap::new(),
            _record_byte_size: 0
        }
    }

    /// Add a new field.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    /// * `value_type` - Field value type.
    pub fn add(&mut self, name: &str, value_type: FieldType) -> Result<&Self> {
        let field = Field::new(name, value_type)?;

        // avoid duplicated fields
        if let Some(_) = self._fields.get(field.get_name()) {
            bail!("field \"{}\" already exists within the header", field.get_name());
        }

        // add field
        self._record_byte_size += field.get_type().value_byte_size() as u64;
        self._fields.insert(name.to_string(), field);
        
        Ok(self)
    }

    /// Rebuilds the index hashmap.
    fn rebuild_hashmap(&mut self) {
        let mut record_size = 0u64;
        for (_, field) in self._fields.iter() {
            record_size += field.get_type().value_byte_size() as u64;
        }
        self._record_byte_size = record_size;
    }

    /// Removes and return the field at the index position.
    /// This is currently very inefficient as the map is rebuilt.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Field index to remove.
    pub fn remove(&mut self, index: usize) -> Option<Field> {
        let (_, field) = self._fields.shift_remove_index(index)?;
        self.rebuild_hashmap();
        Some(field)
    }

    /// Removes and return the field with the specified name.
    /// This is currently very inefficient as the map is rebuilt.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    pub fn remove_by_name(&mut self, name: &str) -> Option<Field> {
        let field = self._fields.remove(name)?;
        self.rebuild_hashmap();
        Some(field)
    }

    /// Get a field by name.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    pub fn get(&self, name: &str) -> Option<&Field> {
        self._fields.get(name)
    }

    /// Get a field by name as mutable.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Field> {
        self._fields.get_mut(name)
    }

    /// Get a field by it's index.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Field index.
    pub fn get_by_index(&self, index: usize) -> Option<&Field> {
        let (_, field) = self._fields.get_index(index)?;
        Some(field)
    }

    /// Get a field by it's index as mutable.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Field index.
    pub fn get_mut_by_index(&mut self, index: usize) -> Option<&mut Field> {
        let (_, field) = self._fields.get_index_mut(index)?;
        Some(field)
    }

    /// Returns the number of fields on the header.
    pub fn len(&self) -> usize {
        self._fields.len()
    }

    /// Return the previously calculated byte count to be writed when
    /// the header is converted into bytes.
    pub fn size_as_bytes(&self) -> u64 {
        u32::BYTES as u64 + (Field::BYTES as u64 * self._fields.len() as u64)
    }

    /// Returns the record size in bytes.
    pub fn record_byte_size(&self) -> u64 {
        return self._record_byte_size;
    }

    /// Clears the field type list.
    pub fn clear(&mut self) {
        self._fields = IndexMap::new();
        self._record_byte_size = 0;
    }

    /// Creates a new record instance from the header fields.
    pub fn new_record(&self) -> Result<Record> {
        let mut record = Record::new();

        for (key, _) in self._fields.iter() {
            record.add(&key, Value::Default)?;
        }
        Ok(record)
    }

    /// Reads a record from the reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    pub fn read_record(&self, reader: &mut impl Read) -> Result<Record> {
        let mut record = Record::new();

        for (key, field) in self._fields.iter() {
            let value = field.get_type().read_value(reader)?;
            record.add(&key, value)?;
        }
        Ok(record)
    }

    /// Writes a record into the writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    pub fn write_record(&self, writer: &mut impl Write, record: &Record) -> Result<()> {
        if self._fields.len() != record.len() {
            bail!("header field count mismatch the record value count");
        }
        for (index, (key, field)) in self._fields.iter().enumerate() {
            let value = match record.get_by_index(index) {
                Some(v) => v,
                None => bail!("invalid value index! this should never happen, please check \
                    the record \"len()\" function")
            };
            if let Err(e) = field.get_type().write_value(writer, value) {
                bail!("error saving field \"{}\": {}", &key, e);
            }
        }
        Ok(())
    }

    /// Returns an iterator over the header fields.
    pub fn iter(&self) -> indexmap::map::Iter<String, Field> {
        self._fields.iter()
    }
}

impl LoadFrom for Header {
    fn load_from(&mut self, reader: &mut impl Read) -> Result<()> {
        // read field count
        let field_count = u32::read_from(reader)?;

        // read fields
        let mut record_size = 0u64;
        let mut fields = IndexMap::new();
        for _ in 0..field_count {
            // read field data and push into the field list
            let field = Field::read_from(reader)?;
            let name = field.get_name().to_string();
            record_size += field.get_type().value_byte_size() as u64;
            if let Some(_) = fields.insert(field.get_name().to_string(), field) {
                bail!("duplicated field \"{}\"", &name);
            }
        }

        // save read field list
        self._fields = fields;
        self._record_byte_size = record_size;
        Ok(())
    }
}

impl ReadFrom for Header {
    fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut header = Self::new();
        header.load_from(reader)?;
        Ok(header)
    }
}

impl WriteTo for Header {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        // write field count
        let field_count = self._fields.len() as u32;
        field_count.write_to(writer)?;

        // write fields data
        for (_, field) in self._fields.iter() {
            field.write_to(writer)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_header() {
        let expected = Header{
            _fields: IndexMap::new(),
            _record_byte_size: 0
        };
        let header = Header::new();
        assert_eq!(expected, header);
    }

    #[test]
    fn add_field() {
        let expected_0 = Field::new("foo",FieldType::F32).unwrap();
        let expected_1 = Field::new("bar", FieldType::I32).unwrap();
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::I32) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }

        // test list and map
        assert_eq!(2, header._fields.len());
        assert_eq!(expected_0, header._fields[0]);
        assert_eq!(expected_1, header._fields[1]);
        assert_eq!(8, header._record_byte_size);
        match header._fields.get("foo") {
            Some(v) => assert_eq!(expected_0, *v),
            None => assert!(false, "expected {:?} but got None", 0)
        }
        match header._fields.get("bar") {
            Some(v) => assert_eq!(expected_1, *v),
            None => assert!(false, "expected {:?} but got None", 1)
        }
    }

    #[test]
    fn add_dup_field() {
        let expected = "field \"foo\" already exists within the header";
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        match header.add("foo", FieldType::I32) {
            Ok(v) => assert!(false, "expected error but got {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn rebuild_hashmap() {
        let mut header = Header{
            _fields: IndexMap::new(),
            _record_byte_size: 0
        };
        header._fields.insert("abc".to_string(), Field::new("abc", FieldType::U32).unwrap());
        header._fields.insert("def".to_string(), Field::new("def", FieldType::Str(45)).unwrap());
        header.rebuild_hashmap();
        assert_eq!(53u64, header._record_byte_size);
    }

    #[test]
    fn remove_with_index() {
        let expected = Field::new("abcde", FieldType::I64).unwrap();
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        assert_eq!(3, header._fields.len());
        assert_eq!(expected, header._fields[1]);
        assert_eq!(20, header._record_byte_size);
        match header._fields.get("abcde") {
            Some(v) => assert_eq!(expected, *v),
            None => assert!(false, "expected {:?} but got None", expected)
        }

        // remove the header
        match header.remove(1) {
            Some(v) => assert_eq!(expected, v),
            None => assert!(false, "expected {:?} but got None", expected)
        };
        assert_eq!(2, header._fields.len());
        assert_eq!(12, header._record_byte_size);
        match header._fields.get("abcde") {
            Some(v) => assert!(false, "expected None but got {:?}", v),
            None => assert!(true, "")
        }
    }

    #[test]
    fn remove_by_name() {
        let expected: Field = Field::new("abcde", FieldType::I64).unwrap();
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("faa", FieldType::F64) {
            assert!(false, "expected to add \"faa\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        assert_eq!(4, header._fields.len());
        assert_eq!(28, header._record_byte_size);
        match header._fields.get("abcde") {
            Some(v) => assert_eq!(expected, *v),
            None => assert!(false, "expected {:?} but got None", 1)
        }

        // remove the header
        let deleted = match header.remove_by_name("abcde") {
            Some(v) => v,
            None => {
                assert!(false, "expected {:?} but got None", expected);
                return;
            }
        };
        assert_eq!(expected, deleted);
        assert_eq!(3, header._fields.len());
        assert_eq!(20, header._record_byte_size);
        match header._fields.get("abcde") {
            Some(v) => assert!(false, "expected None but got {:?}", v),
            None => assert!(true, "")
        }
    }

    #[test]
    fn remove_by_name_not_found() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("faa", FieldType::F64) {
            assert!(false, "expected to add \"faa\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        assert_eq!(3, header._fields.len());
        assert_eq!(20, header._record_byte_size);
        match header._fields.get("abcde") {
            Some(v) => assert!(false, "expected None but got {:?}", v),
            None => assert!(true, "")
        }

        // try to remove the header
        match header.remove_by_name("abcde") {
            Some(v) => assert!(false, "expected None but got {:?}", v),
            None => assert!(true, "")
        };
        assert_eq!(3, header._fields.len());
        assert_eq!(20, header._record_byte_size);
    }

    #[test]
    fn get_by_index_existing() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        assert_eq!(3, header._fields.len());

        // test search by index
        let expected = Field::new("abcde", FieldType::I64).unwrap();
        assert_eq!(expected, header._fields[1]);
        match header.get_by_index(1) {
            Some(v) => assert_eq!(&expected, v),
            None => assert!(false, "expected {:?} but got None", expected)
        }

        // test search mutable by index
        let mut expected = Field::new("foo", FieldType::F32).unwrap();
        assert_eq!(expected, header._fields[0]);
        match header.get_mut_by_index(0) {
            Some(v) => assert_eq!(&mut expected, v),
            None => assert!(false, "expected {:?} but got None", expected)
        }
    }

    #[test]
    fn get_by_index_not_found() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        assert_eq!(3, header._fields.len());

        // test search
        match header.get_by_index(4) {
            Some(v) => assert!(false, "expected None but got {:?}", v),
            None => assert!(true, "")
        }
        match header.get_mut_by_index(5) {
            Some(v) => assert!(false, "expected None but got {:?}", v),
            None => assert!(true, "")
        }
    }

    #[test]
    fn get_existing() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        assert_eq!(3, header._fields.len());

        // test search by index
        let expected = Field::new("abcde", FieldType::I64).unwrap();
        assert_eq!(expected, header._fields[1]);
        match header.get("abcde") {
            Some(v) => assert_eq!(&expected, v),
            None => assert!(false, "expected {:?} but got None", expected)
        }

        // test search mutable by index
        let mut expected = Field::new("foo", FieldType::F32).unwrap();
        assert_eq!(expected, header._fields[0]);
        match header.get_mut("foo") {
            Some(v) => assert_eq!(&mut expected, v),
            None => assert!(false, "expected {:?} but got None", expected)
        }
    }

    #[test]
    fn get_not_found() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        assert_eq!(3, header._fields.len());

        // test search
        match header.get("aaa") {
            Some(v) => assert!(false, "expected None but got {:?}", v),
            None => assert!(true, "")
        }
        match header.get_mut("bbb") {
            Some(v) => assert!(false, "expected None but got {:?}", v),
            None => assert!(true, "")
        }
    }

    #[test]
    fn len() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }

        // test length
        assert_eq!(2, header.len());

        // add fields
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }

        // test length
        assert_eq!(3, header.len());

        // delete 2 items
        header.remove(1);
        header.remove_by_name("foo");

        // test length
        assert_eq!(1, header.len());
    }

    #[test]
    fn size_as_bytes() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }

        // test length
        assert_eq!(122, header.size_as_bytes());

        // add fields
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }

        // test length
        assert_eq!(181, header.size_as_bytes());
    }

    #[test]
    fn record_byte_size() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }

        // test length
        assert_eq!(122, header.size_as_bytes());
        assert_eq!(12, header._record_byte_size);

        // add fields
        if let Err(e) = header.add("bar", FieldType::U64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }

        // test length
        assert_eq!(181, header.size_as_bytes());
        assert_eq!(20, header._record_byte_size);
    }

    #[test]
    fn clear() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I64) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }
        assert_eq!(2, header._fields.len());
        assert_eq!(12, header._record_byte_size);

        // test clear
        let expected: IndexMap<String, Field> = IndexMap::new();
        header.clear();
        assert_eq!(expected, header._fields);
        assert_eq!(0, header._record_byte_size);
    }

    #[test]
    fn new_record() {
        let mut header = Header::new();

        // add fields
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::I64) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }

        // test new record
        let mut expected = Record::new();
        if let Err(e) = expected.add("foo", Value::Default) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = expected.add("bar", Value::Default) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        let record = match header.new_record() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new record but got error: {:?}", e);
                return
            }
        };
        assert_eq!(expected, record);
    }

    #[test]
    fn read_record() {
        // create buffer and reader
        let buf = [
            // foo field
            6u8, 74u8, 236u8, 75u8, 242u8, 24u8, 101u8, 197u8,
            // bar field value size
            0, 0, 0, 5u8,
            // bar field value
            104u8, 101u8, 108u8, 108u8, 111u8, 0, 0, 0, 0, 0,
            // abc field
            9u8, 41u8
        ];
        let mut reader = &buf as &[u8];

        // create header
        let mut header = Header::new();
        if let Err(e) = header.add("foo", FieldType::U64) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::Str(10)) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abc", FieldType::I16) {
            assert!(false, "expected to add \"abc\" field but got error: {:?}", e);
            return;
        }

        // create expected record
        let mut expected = Record::new();
        if let Err(e) = expected.add("foo", Value::U64(453434523432543685u64)) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = expected.add("bar", Value::Str("hello".to_string())) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = expected.add("abc", Value::I16(2345i16)) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }

        // test
        match header.read_record(&mut reader) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn write_record() {
        let expected = [
            // foo field
            74u8, 138u8, 96u8, 147u8,
            // bar field value size
            0, 0, 0, 6u8,
            // bar field value
            119u8, 111u8, 114u8, 108u8, 100u8, 33u8, 0, 0, 0, 0, 0, 0,
            // abc field
            48u8, 141u8, 107u8, 57u8, 24u8, 192u8, 156u8, 149u8
        ];

        // create header
        let mut header = Header::new();
        if let Err(e) = header.add("foo", FieldType::F32) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::Str(12)) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abc", FieldType::U64) {
            assert!(false, "expected to add \"abc\" field but got error: {:?}", e);
            return;
        }

        // create record
        let mut record = Record::new();
        if let Err(e) = record.add("foo", Value::F32(4534345.345f32)) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = record.add("bar", Value::Str("world!".to_string())) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = record.add("abc", Value::U64(3498570378509327509u64)) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }

        // test
        let mut buf = [0u8; 28];
        let mut writer = &mut buf as &mut [u8];
        match header.write_record(&mut writer, &record) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn load_from_with_uniq_fields() {
        // expected header
        let mut expected = Header::new();
        if let Err(e) = expected.add("foo", FieldType::F64) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = expected.add("bar", FieldType::Str(45)) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = expected.add("abcde", FieldType::I8) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }

        // test
        let buf = [
            // field count
            0, 0, 0, 3u8,

            // foo field name value size
            0, 0, 0, 3u8,
            // foo field name value
            102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // foo field type
            11u8, 0, 0, 0, 0,

            // bar field name value size
            0, 0, 0, 3u8,
            // bar field name value
            98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // bar field type
            12u8, 0, 0, 0, 45u8,

            // abcde field name value size
            0, 0, 0, 5u8,
            // abcde field name value
            97u8, 98u8, 99u8, 100u8, 101u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
            // abcde field type
            2u8, 0, 0, 0, 0
        ];
        let mut reader = &buf as &[u8];
        let mut header = Header::new();
        match header.load_from(&mut reader) {
            Ok(()) => assert_eq!(expected, header),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn load_from_with_dup_fields() {
        // expected header
        let expected = "duplicated field \"foo\"";

        // test
        let buf = [
            // field count
            0, 0, 0, 2u8,

            // foo field name value size
            0, 0, 0, 3u8,
            // foo field name value
            102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // foo field type
            11u8, 0, 0, 0, 0,

            // dup foo field name value size
            0, 0, 0, 3u8,
            // dup foo field name value
            102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // dup foo field type should be detected even with different types
            1u8, 0, 0, 0, 0
        ];
        let mut reader = &buf as &[u8];
        let mut header = Header::new();
        match header.load_from(&mut reader) {
            Ok(()) => assert!(false, "expected error but got sucess"),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn read_from_with_uniq_fields() {
        // expected header
        let mut expected = Header::new();
        if let Err(e) = expected.add("foo", FieldType::U64) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = expected.add("hello", FieldType::Str(656875457u32)) {
            assert!(false, "expected to add \"hello\" field but got error: {:?}", e);
            return;
        }

        // test
        let buf = [
            // field count
            0, 0, 0, 2u8,

            // foo field name value size
            0, 0, 0, 3u8,
            // foo field name value
            102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // foo field type
            9u8, 0, 0, 0, 0,

            // hello field name value size
            0, 0, 0, 5u8,
            // hello field name value
            104u8, 101u8, 108u8, 108u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            // hello field type
            12u8, 39u8, 39u8, 31u8, 193u8,
        ];
        let mut reader = &buf as &[u8];
        match Header::read_from(&mut reader) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn read_from_with_dup_fields() {
        // expected header
        let expected = "duplicated field \"bar\"";

        // test
        let buf = [
            // field count
            0, 0, 0, 2u8,

            // bar field name value size
            0, 0, 0, 3u8,
            // bar field name value
            98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // bar field type
            5u8, 0, 0, 0, 0,

            // dup bar field name value size
            0, 0, 0, 3u8,
            // dup bar field name value
            98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // dup bar field type should be detected even with different types
            3u8, 0, 0, 0, 0
        ];
        let mut reader = &buf as &[u8];
        match Header::read_from(&mut reader) {
            Ok(v) => assert!(false, "expected error but got: {:?}", v),
            Err(e) => assert_eq!(expected, e.to_string())
        }
    }

    #[test]
    fn write_to() {
        let expected = [
            // field count
            0, 0, 0, 3u8,

            // foo field name value size
            0, 0, 0, 3u8,
            // foo field name value
            102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // foo field type
            1u8, 0, 0, 0, 0,

            // abcde field name value size
            0, 0, 0, 5u8,
            // abcde field name value
            97u8, 98u8, 99u8, 100u8, 101u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
            // abcde field type
            2u8, 0, 0, 0, 0,

            // bar field name value size
            0, 0, 0, 3u8,
            // bar field name value
            98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
            // bar field type
            12u8, 0, 0, 0, 37u8
        ];

        // create header
        let mut header = Header::new();
        if let Err(e) = header.add("foo", FieldType::Bool) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I8) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::Str(37)) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }

        // test
        let mut buf = [0u8; 181];
        let mut writer = &mut buf as &mut [u8];
        match header.write_to(&mut writer) {
            Ok(()) => assert_eq!(expected, buf),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn iter() {
        // create header
        let mut header = Header::new();
        if let Err(e) = header.add("foo", FieldType::Bool) {
            assert!(false, "expected to add \"foo\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("abcde", FieldType::I8) {
            assert!(false, "expected to add \"abcde\" field but got error: {:?}", e);
            return;
        }
        if let Err(e) = header.add("bar", FieldType::Str(37)) {
            assert!(false, "expected to add \"bar\" field but got error: {:?}", e);
            return;
        }

        // test
        let expected = vec!["foo".to_string(), "abcde".to_string(), "bar".to_string()];
        let mut field_names: Vec<String> = Vec::new();
        for (key, field) in header.iter() {
            assert_eq!(key, field.get_name());
            field_names.push(key.to_string());
        }
        assert_eq!(expected, field_names);
    }
}