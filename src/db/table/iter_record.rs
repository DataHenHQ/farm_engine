use crate::db::{field::Record, table::traits::TableTrait};
use std::io::{Read, Seek};
use crate::db::table::Table;

pub struct IterRecord<'reader, 'table, R: Read + Seek> {
    pub(crate) reader: &'reader mut R,
    pub(crate) table: &'table Table,
    pub(crate) min: u64,
    pub(crate) max: u64
}

impl<'reader, 'table, R: Read + Seek> Iterator for IterRecord<'reader, 'table, R> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.min > self.max {
            return None;
        }
        let record = match self.table.unsafe_record_from(self.reader) {
            Ok(v) => v,
            Err(_) => return None
        };
        self.min += 1;
        Some(record)
    }
}
