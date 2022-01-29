pub mod header;
pub mod value;

use std::collections::HashMap;
use anyhow::{bail, Result};
pub use header::Header;
pub use value::Value;
use header::Field;

/// Represents a data record.
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
            bail!("field \"{}\" already exists within the header", field_name);
        }

        // validate value
        if !field.get_type().is_valid(&value) {
            bail!("invalid value, expected {:?}", field.get_type())
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
            bail!("invalid value, expected {:?}", field.get_type())
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