pub mod metadata;

use std::io::{Read, Write};
use anyhow::{bail, Result};
use serde::Serialize;
use sha3::digest::typenum::private::IsGreaterPrivate;
use crate::db::field::{Record as FieldRecord, Header as FieldHeader};
use crate::error::{ParseError, IndexError};
use crate::traits::{ByteSized, WriteAsBytes, WriteTo, LoadFrom};
use metadata::Metadata;

#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct Record {
    /// Index record metadata
    pub metadata: Metadata,

    /// Indexed fields
    pub fields: FieldRecord,
}

impl Record {
    /// Creates a new record instance.
    pub fn new() -> Self {
        Self{
            metadata: Metadata::new(),
            fields: FieldRecord::new(),
        }
    }

//TODO --> Ale Arreglar el codigo, es para fields individuales, no para un indice grupal
    pub fn gt(&self, record: &Self) -> Result<bool> {
        for (key, value) in self.fields.iter() {
            match record.fields.get(key) {
                Some(v) => if !(value < v) {
                    return Ok(false)
                },
                None => bail!(IndexError::InvalidField(key))
            }
        }
        Ok(true)
    }

    pub fn lt(&self, record: &Self) -> Result<bool> {
        for (key, value) in self.fields.iter() {
            match record.fields.get(key) {
                Some(v) => if !(value > v){
                    return Ok(false)
                },
                None => bail!(IndexError::InvalidField(key))
            }
        }
        Ok(true)
    }

    pub fn equ(&self, record: &Self) -> Result<bool> {
        for (key, value) in self.fields.iter() {
            match record.fields.get(key) {
                Some(v) => if !(value == v) {
                    return Ok(false)
                },
                    //    return Ok(false)
                    //},
                None => bail!(IndexError::InvalidField(key))
            }
        }
        Ok(true)
    }

    pub fn eq_full_index(&self, record: &Self) -> Result<bool> {
        let length = record.fields.len();

        let mut counter = 1;

        if (length > 0) {
            let key1 = record.fields.get_by_index(counter);

            for (key, value) in self.fields.iter() {
                match record.fields.get(key) {
                    Some(v) => if !(value == v) {
                        return Ok(false)
                    },
                    None => bail!(IndexError::InvalidField(key))
                }
            }
        }

        Ok(true)
    }

    /// Return the previously calculated byte count to be writed when
    /// converted into bytes.
    pub fn size_as_bytes(field_header: &FieldHeader) -> u64 {
        Metadata::BYTES as u64 + field_header.record_byte_size()
    }

    pub fn write_to(&self, field_header: &FieldHeader, writer: &mut impl Write) -> Result<()> {
        // write metadata
        self.metadata.write_to(writer)?;

        // write fields
        field_header.write_record(writer, &self.fields)?;

        Ok(())
    }

    pub fn load_from(&mut self, field_header: &FieldHeader, reader: &mut impl Read) -> Result<()> {
        // read metadata
        self.metadata.load_from(reader)?;

        // read fields
        self.fields = field_header.read_record(reader)?;

        Ok(())
    }
}

