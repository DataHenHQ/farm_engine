pub mod header;
pub mod value;

use serde::ser::{Serialize, Serializer, SerializeMap};
use std::collections::HashMap;
use anyhow::{bail, Result};
pub use header::Header;
pub use value::Value;

/// Represents a data record.
#[derive(Debug, PartialEq)]
pub struct Record {
    _list: Vec<(String, Value)>,
    _map: HashMap<String, usize>
}

impl Record {
    // Creates a new record.
    pub fn new() -> Self {
        Self{
            _list: Vec::new(),
            _map: HashMap::new()
        }
    }

    /// Add a new value.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    pub fn add(&mut self, name: &str, value: Value) -> Result<&Self> {
        // avoid duplicated fields
        if let Some(_) = self._map.get(name) {
            bail!("field \"{}\" already exists within the record", name);
        }

        // add field
        let item = (name.to_string(), value);
        self._list.push(item);
        self._map.insert(name.to_string(), self._list.len()-1);
        
        Ok(self)
    }

    /// Set a field value by field name.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    /// * `value` - New value.
    pub fn set(&mut self, name: &str, value: Value) -> Result<()> {
        // update value
        let index = match self._map.get(name) {
            Some(v) => *v,
            None => bail!("can't update: unknown field \"{}\"", name)
        };
        self._list[index].1 = value;
        Ok(())
    }

    /// Set a field value by field index.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Field index.
    /// * `value` - New value.
    pub fn set_by_index(&mut self, index: usize, value: Value) {
        self._list[index].1 = value;
    }

    /// Get a value by name.
    /// 
    /// # Arguments
    /// 
    /// * `name` - Field name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        let index = match self._map.get(name) {
            Some(v) => *v,
            None => return None
        };
        Some(&self._list[index].1)
    }

    /// Get a value by it's index.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Value index.
    pub fn get_by_index(&self, index: usize) -> Option<&Value> {
        if self._list.len() > index {
            return Some(&self._list[index].1);
        }
        None
    }

    /// Returns the number of fields on the header.
    pub fn len(&self) -> usize {
        self._list.len()
    }

    /// Returns a column iterator.
    pub fn iter(&self) -> std::slice::Iter<(String, Value)> {
        self._list.iter()
    }
}

impl Serialize for Record {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_map(Some(self._list.len()))?;
        for (k, v) in self._list.iter() {
            s.serialize_entry(k, v)?;
        }
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod record {
        use super::*;

        #[test]
        fn new_record() {
            let expected = Record{
                _list: Vec::new(),
                _map: HashMap::new()
            };
            let record = Record::new();
            assert_eq!(expected, record);
        }

        #[test]
        fn serialize() {
            let expected = r#"{"foo":111,"bar":222,"abc":"AAA","my_flag":true}"#.to_string();
            let mut record = Record::new();
            record.add("foo", 111i16.into()).unwrap();
            record.add("bar", 222i64.into()).unwrap();
            record.add("abc", "AAA".into()).unwrap();
            record.add("my_flag", true.into()).unwrap();
            match serde_json::to_string(&record) {
                Ok(s) => assert_eq!(expected, s),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
        }

        #[test]
        fn add_field() {
            let mut record = Record::new();

            // add first field
            let expected = ("foo".to_string(), Value::F32(23f32));
            if let Err(e) = record.add(&"foo", Value::F32(23f32)) {
                assert!(false, "expected to add {:?} value to \"foo\" field but got error: {:?}", expected, e);
                return;
            }
            assert_eq!(expected, record._list[0]);
            match record._map.get("foo") {
                Some(v) => assert_eq!(0, *v),
                None => assert!(false, "expected {:?} but got None", 0)
            }

            // add first field
            let expected = ("bar".to_string(), Value::I64(765i64));
            if let Err(e) = record.add("bar", Value::I64(765i64)) {
                assert!(false, "expected to add {:?} value to \"bar\" field but got error: {:?}", expected, e);
                return;
            }
            assert_eq!(expected, record._list[1]);
            match record._map.get("bar") {
                Some(v) => assert_eq!(1, *v),
                None => assert!(false, "expected {:?} but got None", 0)
            }
        }

        #[test]
        fn add_dup_field() {
            let expected = "field \"foo\" already exists within the record";
            let mut record = Record::new();

            // add fields
            let value = Value::Bool(true);
            if let Err(e) = record.add("foo", Value::Bool(true)) {
                assert!(false, "expected to add {:?} value to \"foo\" field but got error: {:?}", value, e);
                return;
            }
            match record.add("foo", Value::Bool(true)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }
        }

        #[test]
        fn set_existing_field_value() {
            let mut record = Record::new();

            // add field values
            if let Err(e) = record.add("foo", Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add("abcde", Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            if let Err(e) = record.add("bar", Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }

            // check the inserted values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(("foo".to_string(), Value::F32(23.12f32)), record._list[0]);
            assert_eq!(("abcde".to_string(), Value::I64(12i64)), record._list[1]);
            assert_eq!(("bar".to_string(), Value::U64(34u64)), record._list[2]);

            // update values
            if let Err(e) = record.set("foo", Value::F32(657.54f32)) {
                assert!(false, "expected to set {:?} value to \"foo\" field but got error: {:?}", Value::F32(657.54f32), e);
                return;
            }
            if let Err(e) = record.set("abcde", Value::I64(956i64)) {
                assert!(false, "expected to set {:?} value to \"abcde\" field but got error: {:?}", Value::I64(956i64), e);
                return;
            }
            if let Err(e) = record.set("bar", Value::U64(45596u64)) {
                assert!(false, "expected to set {:?} value to \"bar\" field but got error: {:?}", Value::U64(45596u64), e);
                return;
            }

            // check the new values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(("foo".to_string(), Value::F32(657.54f32)), record._list[0]);
            assert_eq!(("abcde".to_string(), Value::I64(956i64)), record._list[1]);
            assert_eq!(("bar".to_string(), Value::U64(45596u64)), record._list[2]);
        }

        #[test]
        fn set_invalid_field() {
            let mut record = Record::new();

            // add field values
            if let Err(e) = record.add("foo", Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add("abcde", Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            if let Err(e) = record.add("bar", Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }

            // check the inserted values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(("foo".to_string(), Value::F32(23.12f32)), record._list[0]);
            assert_eq!(("abcde".to_string(), Value::I64(12i64)), record._list[1]);
            assert_eq!(("bar".to_string(), Value::U64(34u64)), record._list[2]);

            // update values
            let expected = "can't update: unknown field \"aaa\"";
            match record.set("aaa", Value::U64(20u64)) {
                Ok(()) => assert!(false, "expected an error but got success"),
                Err(e) => assert_eq!(expected, e.to_string())
            }

            // check the new values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(("foo".to_string(), Value::F32(23.12f32)), record._list[0]);
            assert_eq!(("abcde".to_string(), Value::I64(12i64)), record._list[1]);
            assert_eq!(("bar".to_string(), Value::U64(34u64)), record._list[2]);
        }

        #[test]
        fn set_by_index_existing_field_value() {
            let mut record = Record::new();

            // add field values
            if let Err(e) = record.add("foo", Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add("abcde", Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            if let Err(e) = record.add("bar", Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }

            // check the inserted values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(("foo".to_string(), Value::F32(23.12f32)), record._list[0]);
            assert_eq!(("abcde".to_string(), Value::I64(12i64)), record._list[1]);
            assert_eq!(("bar".to_string(), Value::U64(34u64)), record._list[2]);

            // update values
            record.set_by_index(0, Value::F32(657.54f32));
            record.set_by_index(1, Value::I64(956i64));
            record.set_by_index(2, Value::U64(45596u64));

            // check the new values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(("foo".to_string(), Value::F32(657.54f32)), record._list[0]);
            assert_eq!(("abcde".to_string(), Value::I64(956i64)), record._list[1]);
            assert_eq!(("bar".to_string(), Value::U64(45596u64)), record._list[2]);
        }

        #[test]
        fn get_by_index_existing() {
            let mut record = Record::new();

            // add field values
            if let Err(e) = record.add("foo", Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add("abcde", Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            if let Err(e) = record.add("bar", Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }
            assert_eq!(3, record._list.len());

            // first test search by index
            let expected = ("abcde".to_string(), Value::I64(12i64));
            assert_eq!(expected, record._list[1]);
            match record.get_by_index(1) {
                Some(v) => assert_eq!(&expected.1, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }

            // second test search by index
            let expected = ("foo".to_string(), Value::F32(23.12f32));
            assert_eq!(expected, record._list[0]);
            match record.get_by_index(0) {
                Some(v) => assert_eq!(&expected.1, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }
        }

        #[test]
        fn get_by_index_not_found() {
            let mut record = Record::new();

            // add field values
            if let Err(e) = record.add("foo", Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add("abcde", Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            if let Err(e) = record.add("bar", Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }
            assert_eq!(3, record._list.len());

            // test search
            match record.get_by_index(4) {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            }
        }

        #[test]
        fn get_existing() {
            let mut record = Record::new();

            // add field values
            if let Err(e) = record.add("foo", Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add("abcde", Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            if let Err(e) = record.add("bar", Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());

            // first test search by index
            let expected = ("abcde".to_string(), Value::I64(12i64));
            assert_eq!(expected, record._list[1]);
            match record.get("abcde") {
                Some(v) => assert_eq!(&expected.1, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }

            // second test search by index
            let mut expected = ("bar".to_string(), Value::U64(34u64));
            assert_eq!(expected, record._list[2]);
            match record.get("bar") {
                Some(v) => assert_eq!(&mut expected.1, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }
        }

        #[test]
        fn get_not_found() {
            let mut record = Record::new();

            // add field values
            if let Err(e) = record.add("foo", Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add("abcde", Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            if let Err(e) = record.add("bar", Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());

            // test search
            match record.get("aaa") {
                Some(v) => assert!(false, "expected None but got {:?}", v),
                None => assert!(true, "")
            }
        }

        #[test]
        fn len() {
            let mut record = Record::new();

            // add field values
            if let Err(e) = record.add("foo", Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add("abcde", Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }

            // test length
            assert_eq!(2, record.len());

            // add field value
            if let Err(e) = record.add("bar", Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }

            // test length
            assert_eq!(3, record.len());
        }

        #[test]
        fn iter() {
            let expected_list = vec![
                ("foo".to_string(), Value::F32(12f32)),
                ("bar".to_string(), Value::Str("hello".to_string())),
                ("abc".to_string(), Value::U16(32u16))
            ];
            let mut expected_map = HashMap::new();
            expected_map.insert("foo".to_string(), 0usize);
            expected_map.insert("bar".to_string(), 1usize);
            expected_map.insert("abc".to_string(), 2usize);
            let mut record = Record::new();

            // add field values
            if let Err(e) = record.add("foo", Value::F32(12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add("bar", Value::Str("hello".to_string())) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::Str("hello".to_string()), e);
                return;
            }
            if let Err(e) = record.add("abc", Value::U16(32u16)) {
                assert!(false, "expected to add {:?} value to  \"abc\" field but got error: {:?}", Value::U16(32u16), e);
                return;
            }

            // test
            let mut list = Vec::new();
            let mut map = HashMap::new();
            let mut i: usize = 0;
            for (s, v) in record.iter() {
                list.push((s.to_string(), v.clone()));
                map.insert(s.to_string(), i);
                i += 1;
            }
            assert_eq!(expected_list, record._list);
            assert_eq!(expected_map, record._map);
            assert_eq!(expected_list, list);
            assert_eq!(expected_map, map);
        }
    }
}