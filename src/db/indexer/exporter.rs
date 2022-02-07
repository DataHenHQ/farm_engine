use anyhow::{Result};
use std::io::{Write};
use serde_json::{Map as JSMap, Value as JSValue, Number as JSNumber};
use super::Data;

/// Represent a field to be exported.
#[derive(Debug, PartialEq)]
pub enum ExportField {
    Input(String),
    Record(String),
    SpentTime,
    MatchFlag
}

/// Exporter supported file types.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum ExportFileType {
    CSV,
    JSON
}

/// Exporter writer trait useful when handling multiple input file types.
pub trait ExporterWriter {
    /// Write plain text.
    /// 
    /// # Arguments
    /// 
    /// * `text` - Plain text to write.
    fn write(&mut self, text: &str) -> Result<()>;

    /// Write headers.
    /// 
    /// # Arguments
    /// 
    /// * `headers` - Header names slice.
    fn write_headers(&mut self, headers: &[String]) -> Result<()>;

    /// Write the fields' data into the writer.
    /// 
    /// # Arguments
    /// 
    /// * `fields` - Fields to export.
    /// * `input_data` - Input data to filter.
    /// * `indexer_data` - Indexer data to filter.
    fn write_data(&mut self, fields: &[ExportField], input_data: JSMap<String, JSValue>, indexer_data: Data) -> Result<()>;
}

impl<T: ExporterWriter> ExporterWriter for &'_ T {
    fn write(&mut self, text: &str) -> Result<()> {
        (**self).write(text)
    }

    fn write_headers(&mut self, headers: &[String]) -> Result<()> {
        (**self).write_headers(headers)
    }

    fn write_data(&mut self, fields: &[ExportField], input_data: JSMap<String, JSValue>, indexer_data: Data) -> Result<()> {
        (**self).write_data(fields, input_data, indexer_data)
    }
}

pub struct ExporterCSVWriter<W: Write> {
    writer: csv::Writer<W>
}

impl<W: Write> ExporterCSVWriter<W> {
    /// Filter all fields value into a String array.
    /// 
    /// # Arguments
    /// 
    /// * `fields` - Export fields.
    /// * `input_data` - Input data to filter.
    /// * `indexer_data` - Indexer data to filter.
    fn filter_data(fields: &[ExportField], input_data: JSMap<String, JSValue>, indexer_data: Data) -> Vec<String> {
        let mut data = Vec::new();
        for field in fields {
            let value = match field {
                ExportField::SpentTime => indexer_data.index.spent_time.to_string(),
                ExportField::MatchFlag => indexer_data.index.match_flag.to_string(),
                ExportField::Input(s) => match input_data.get(s) {
                    Some(v) => v.to_string(),
                    None => "".to_string()
                },
                ExportField::Record(s) => match indexer_data.record.get(s) {
                    Some(v) => v.to_string(),
                    None => "".to_string()
                }
            };
            data.push(value);
        }
        data
    }
}

impl<W: Write> ExporterWriter for ExporterCSVWriter<W> {
    fn write(&mut self, text: &str) -> Result<()> {
        unimplemented!()
    }

    fn write_headers(&mut self, headers: &[String]) -> Result<()> {
        self.writer.write_record(headers)?;
        Ok(())
    }

    fn write_data(&mut self, fields: &[ExportField], input_data: JSMap<String, JSValue>, indexer_data: Data) -> Result<()> {
        let data = Self::filter_data(fields, input_data, indexer_data);
        self.writer.write_record(&data)?;
        Ok(())
    }
}

pub struct ExporterJSONWriter<'w, W: 'w> {
    writer: &'w mut W
}

impl<'w, W: Write> ExporterJSONWriter<'w, W> {
    /// Filter all fields value into an JSValue array.
    /// 
    /// # Arguments
    /// 
    /// * `fields` - Export fields.
    /// * `input_data` - Input data to filter.
    /// * `indexer_data` - Indexer data to filter.
    fn filter_data(fields: &[ExportField], input_data: JSMap<String, JSValue>, indexer_data: Data) -> JSMap<String, JSValue> {
        let mut data = JSMap::new();
        for field in fields {
            match field {
                ExportField::SpentTime => {
                    let value = JSValue::Number(JSNumber::from(indexer_data.index.spent_time));
                    data["spent_time"] = value;
                },
                ExportField::MatchFlag => {
                    let value = JSValue::String(indexer_data.index.match_flag.to_string());
                    data["matched"] = value;
                },
                ExportField::Input(s) => {
                    let value = match input_data.get(s) {
                        Some(v) => v.clone(),
                        None => JSValue::Null
                    };
                    data[s] = value;
                },
                ExportField::Record(s) => {
                    let value = match indexer_data.record.get(s) {
                        Some(v) => v.into(),
                        None => JSValue::Null
                    };
                    data[s] = value;
                }
            };
        }
        data
    }
}

impl<'w, W: Write> ExporterWriter for ExporterJSONWriter<'w, W> {
    fn write(&mut self, text: &str) -> Result<()> {
        self.writer.write_all(text.as_bytes())?;
        Ok(())
    }

    fn write_headers(&mut self, _: &[String]) -> Result<()> {
        Ok(())
    }

    fn write_data(&mut self, fields: &[ExportField], input_data: JSMap<String, JSValue>, indexer_data: Data) -> Result<()> {
        let data = Self::filter_data(fields, input_data, indexer_data);
        serde_json::to_writer(&mut self.writer, &data)?;
        Ok(())
    }
}