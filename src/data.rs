use std::io::{Read, Write, Seek};

use crate::traits::DataTrait;

/// Represents a table data containing the table binary.
#[derive(Debug, PartialEq, Clone)]
pub struct Data<T: Read + Write + Seek> {
    data: T,
    need_flush: bool
}

impl<T: Read + Write + Seek> DataTrait<T, ()> for Data<T> {
    fn into_data(self) -> T {
        self.data
    }

    fn new(data: T, need_flush: bool) -> Self {
        Self{
            data,
            need_flush
        }
    }

    fn data_ref(&self) -> &T {
        &self.data
    }

    fn data_mut(&mut self) -> &mut T {
        &mut self.data
    }

    fn healthcheck_binary(&mut self) -> Result<(), ()> {
        Ok(())
    }
}

impl<T: Read + Write + Seek> Read for Data<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.data.read(buf)
    }
}

impl<T: Read + Write + Seek> Write for Data<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.data.write(buf)?;
        self.need_flush = true;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.data.flush()?;
        self.need_flush = false;
        Ok(())
    }
}

impl<T: Read + Write + Seek> Seek for Data<T> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        if self.need_flush {
            self.flush()?;
        }
        self.data.seek(pos)
    }
}