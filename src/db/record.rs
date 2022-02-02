pub mod header;
pub mod value;

use std::collections::HashMap;
use anyhow::{bail, Result};
pub use header::Header;
pub use value::Value;
use header::Field;

/// Represents a data record.
#[derive(Debug, PartialEq)]
pub struct Record {
    _list: Vec<Value>,
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
    /// * `field` - Field config.
    pub fn add(&mut self, field: &Field, value: Value) -> Result<&Self> {
        let field_name = field.get_name().to_string();

        // avoid duplicated fields
        if let Some(_) = self._map.get(&field_name) {
            bail!("field \"{}\" already exists within the record", field_name);
        }

        // validate value
        if !field.get_type().is_valid(&value) {
            bail!("invalid value, expected Value::{:?}", field.get_type())
        }

        // add field
        self._list.push(value);
        self._map.insert(field_name, self._list.len()-1);
        
        Ok(self)
    }

    /// Set a field value.
    /// 
    /// # Arguments
    /// 
    /// * `field` - Field config.
    /// * `value` - New value.
    pub fn set(&mut self, field: &Field, value: Value) -> Result<()> {
        // make sure field type and value type match
        if !field.get_type().is_valid(&value) {
            bail!("invalid value, expected Value::{:?}", field.get_type())
        }

        // update value
        let index = match self._map.get(field.get_name()) {
            Some(v) => *v,
            None => bail!("can't update: unknown field \"{}\"", field.get_name())
        };
        self._list[index] = value;
        Ok(())
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
        Some(&self._list[index])
    }

    /// Get a value by it's index.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Value index.
    pub fn get_by_index(&self, index: usize) -> Option<&Value> {
        if self._list.len() > index {
            return Some(&self._list[index]);
        }
        None
    }

    /// Returns the number of fields on the header.
    pub fn len(&self) -> usize {
        self._list.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod record {
        use super::*;
        use header::FieldType;

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
        fn add_field() {
            let mut record = Record::new();

            // add first field
            let expected = Value::F32(23f32);
            let field = Field::new("foo", FieldType::F32).unwrap();
            if let Err(e) = record.add(&field, Value::F32(23f32)) {
                assert!(false, "expected to add {:?} value to \"foo\" field but got error: {:?}", expected, e);
                return;
            }
            assert_eq!(expected, record._list[0]);
            match record._map.get("foo") {
                Some(v) => assert_eq!(0, *v),
                None => assert!(false, "expected {:?} but got None", 0)
            }

            // add first field
            let expected = Value::I64(765i64);
            let field = Field::new("bar", FieldType::I64).unwrap();
            if let Err(e) = record.add(&field, Value::I64(765i64)) {
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
            let field = Field::new("foo", FieldType::Bool).unwrap();
            let value = Value::Bool(true);
            if let Err(e) = record.add(&field, Value::Bool(true)) {
                assert!(false, "expected to add {:?} value to \"foo\" field but got error: {:?}", value, e);
                return;
            }
            match record.add(&field, Value::Bool(true)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }
        }

        #[test]
        fn add_invalid_field_value() {
            let expected = "invalid value, expected Value::Bool";
            let mut record = Record::new();

            // add fields
            let field = Field::new("abc", FieldType::Bool).unwrap();
            match record.add(&field, Value::I8(4i8)) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }
        }

        #[test]
        fn set_existing_field_value() {
            let mut record = Record::new();

            let fields = [
                Field::new("foo", FieldType::F32).unwrap(),
                Field::new("abcde", FieldType::I64).unwrap(),
                Field::new("bar", FieldType::U64).unwrap()
            ];

            // add field values
            if let Err(e) = record.add(&fields[0], Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add(&fields[1], Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            if let Err(e) = record.add(&fields[2], Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }

            // check the inserted values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(Value::F32(23.12f32), record._list[0]);
            assert_eq!(Value::I64(12i64), record._list[1]);
            assert_eq!(Value::U64(34u64), record._list[2]);

            // update values
            if let Err(e) = record.set(&fields[0], Value::F32(657.54f32)) {
                assert!(false, "expected to set {:?} value to \"foo\" field but got error: {:?}", Value::F32(657.54f32), e);
                return;
            }
            if let Err(e) = record.set(&fields[1], Value::I64(956i64)) {
                assert!(false, "expected to set {:?} value to \"abcde\" field but got error: {:?}", Value::I64(956i64), e);
                return;
            }
            if let Err(e) = record.set(&fields[2], Value::U64(45596u64)) {
                assert!(false, "expected to set {:?} value to \"bar\" field but got error: {:?}", Value::U64(45596u64), e);
                return;
            }

            // check the new values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(Value::F32(657.54f32), record._list[0]);
            assert_eq!(Value::I64(956i64), record._list[1]);
            assert_eq!(Value::U64(45596u64), record._list[2]);
        }

        #[test]
        fn set_invalid_field_value() {
            let expected = "invalid value, expected Value::Str(20)";
            let mut record = Record::new();
            let field = Field::new("foo", FieldType::Str(20)).unwrap();

            // add field
            if let Err(e) = record.add(&field, Value::Str("hello".to_string())) {
                assert!(false, "expected to add {:?} value to \"foo\" field but got error: {:?}", expected, e);
                return;
            }

            // check the inserted value
            assert_eq!(1, record._list.len());
            assert_eq!(1, record._map.len());
            assert_eq!(Value::Str("hello".to_string()), record._list[0]);

            // set invalid value
            match record.set(&field, Value::I8(4i8)) {
                Ok(()) => assert!(false, "expected error but got success"),
                Err(e) => assert_eq!(expected, e.to_string())
            }

            // check the inserted value
            assert_eq!(1, record._list.len());
            assert_eq!(1, record._map.len());
            assert_eq!(Value::Str("hello".to_string()), record._list[0]);
        }

        #[test]
        fn set_invalid_field() {
            let mut record = Record::new();

            let fields = [
                Field::new("foo", FieldType::F32).unwrap(),
                Field::new("abcde", FieldType::I64).unwrap(),
                Field::new("bar", FieldType::U64).unwrap()
            ];

            // add field values
            if let Err(e) = record.add(&fields[0], Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            if let Err(e) = record.add(&fields[1], Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            if let Err(e) = record.add(&fields[2], Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }

            // check the inserted values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(Value::F32(23.12f32), record._list[0]);
            assert_eq!(Value::I64(12i64), record._list[1]);
            assert_eq!(Value::U64(34u64), record._list[2]);

            // update values
            let expected = "can't update: unknown field \"aaa\"";
            let field = Field::new("aaa", FieldType::U64).unwrap();
            match record.set(&field, Value::U64(20u64)) {
                Ok(()) => assert!(false, "expected an error but got success"),
                Err(e) => assert_eq!(expected, e.to_string())
            }

            // check the new values
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());
            assert_eq!(Value::F32(23.12f32), record._list[0]);
            assert_eq!(Value::I64(12i64), record._list[1]);
            assert_eq!(Value::U64(34u64), record._list[2]);
        }

        #[test]
        fn get_by_index_existing() {
            let mut record = Record::new();

            // add field values
            let field = Field::new("foo", FieldType::F32).unwrap();
            if let Err(e) = record.add(&field, Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            let field = Field::new("abcde", FieldType::I64).unwrap();
            if let Err(e) = record.add(&field, Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            let field = Field::new("bar", FieldType::U64).unwrap();
            if let Err(e) = record.add(&field, Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }
            assert_eq!(3, record._list.len());

            // first test search by index
            let expected = Value::I64(12i64);
            assert_eq!(expected, record._list[1]);
            match record.get_by_index(1) {
                Some(v) => assert_eq!(&expected, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }

            // second test search by index
            let expected = Value::F32(23.12f32);
            assert_eq!(expected, record._list[0]);
            match record.get_by_index(0) {
                Some(v) => assert_eq!(&expected, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }
        }

        #[test]
        fn get_by_index_not_found() {
            let mut record = Record::new();

            // add field values
            let field = Field::new("foo", FieldType::F32).unwrap();
            if let Err(e) = record.add(&field, Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            let field = Field::new("abcde", FieldType::I64).unwrap();
            if let Err(e) = record.add(&field, Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            let field = Field::new("bar", FieldType::U64).unwrap();
            if let Err(e) = record.add(&field, Value::U64(34u64)) {
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
            let field = Field::new("foo", FieldType::F32).unwrap();
            if let Err(e) = record.add(&field, Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            let field = Field::new("abcde", FieldType::I64).unwrap();
            if let Err(e) = record.add(&field, Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            let field = Field::new("bar", FieldType::U64).unwrap();
            if let Err(e) = record.add(&field, Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }
            assert_eq!(3, record._list.len());
            assert_eq!(3, record._map.len());

            // first test search by index
            let expected = Value::I64(12i64);
            assert_eq!(expected, record._list[1]);
            match record.get("abcde") {
                Some(v) => assert_eq!(&expected, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }

            // second test search by index
            let mut expected = Value::U64(34u64);
            assert_eq!(expected, record._list[2]);
            match record.get("bar") {
                Some(v) => assert_eq!(&mut expected, v),
                None => assert!(false, "expected {:?} but got None", expected)
            }
        }

        #[test]
        fn get_not_found() {
            let mut record = Record::new();

            // add field values
            let field = Field::new("foo", FieldType::F32).unwrap();
            if let Err(e) = record.add(&field, Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            let field = Field::new("abcde", FieldType::I64).unwrap();
            if let Err(e) = record.add(&field, Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }
            let field = Field::new("bar", FieldType::U64).unwrap();
            if let Err(e) = record.add(&field, Value::U64(34u64)) {
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
            let field = Field::new("foo", FieldType::F32).unwrap();
            if let Err(e) = record.add(&field, Value::F32(23.12f32)) {
                assert!(false, "expected to add {:?} value to  \"foo\" field but got error: {:?}", Value::F32(23.12f32), e);
                return;
            }
            let field = Field::new("abcde", FieldType::I64).unwrap();
            if let Err(e) = record.add(&field, Value::I64(12i64)) {
                assert!(false, "expected to add {:?} value to  \"abcde\" field but got error: {:?}", Value::I64(12i64), e);
                return;
            }

            // test length
            assert_eq!(2, record.len());

            // add field value
            let field = Field::new("bar", FieldType::U64).unwrap();
            if let Err(e) = record.add(&field, Value::U64(34u64)) {
                assert!(false, "expected to add {:?} value to  \"bar\" field but got error: {:?}", Value::U64(34u64), e);
                return;
            }

            // test length
            assert_eq!(3, record.len());
        }
    }
}