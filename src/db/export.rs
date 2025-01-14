use anyhow::{Result, bail};
use serde::{Serialize, Deserialize};
use serde_json::{Map as JSMap, Value as JSValue, Number as JSNumber};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write, BufWriter};
use std::path::PathBuf;
use crate::traits::ReadFrom;
use super::indexer::Indexer;
use super::indexer::header::InputType;
use super::indexer::value::{Value as IndexValue, MatchFlag};
use super::table::record::Record;
use super::source::Source;

/// MatchFlag masked value.
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct MatchFlagMask {
    pub yes: Option<String>,
    pub no: Option<String>,
    pub skip: Option<String>,
    pub none: Option<String>
}

/// Represent a field to be exported.
#[derive(Debug, PartialEq, Clone)]
pub enum ExportField {
    AllInput{overrides: Option<HashMap<String, ExportField>>},
    AllRecord{overrides: Option<HashMap<String, ExportField>>},
    Input{label: Option<String>, name: String},
    Record{label: Option<String>, name: String},
    /// Spent time with moved decimal point.
    SpentTime{label: Option<String>, decimal: f64},
    MatchFlag{label: Option<String>, mask: Option<MatchFlagMask>},
    None{label: String}
}

/// Exporter supported file types.
#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
pub enum ExportFileType {
    CSV,
    JSON
}

/// Exporter data.
#[derive(Debug, PartialEq)]
pub struct ExportData {
    input_headers: Vec<String>,
    input: JSMap<String, JSValue>,
    index: IndexValue,
    record: Record
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
    /// * `value` - Indexer data to filter.
    fn write_data(&mut self, fields: &[ExportField], source: ExportData, is_first: bool) -> Result<()>;

    /// Write end.
    fn write_end(&mut self) -> Result<()>;

    /// Calculate the spent time value
    /// 
    /// # Arguments
    /// 
    /// * `decimal` - Decimal points to move to the left.
    /// * `source` - Source data.
    fn calc_spent_time(decimal: f64, source: &ExportData) -> f64 {
        source.index.data.spent_time as f64 / 10f64.powf(decimal)
    }

    /// Calculate the match flag value
    /// 
    /// # Arguments
    /// 
    /// * `mask` - Match flag mask to override its string value representation.
    /// * `source` - Source data.
    fn calc_match_flag(mask: &Option<MatchFlagMask>, source: &ExportData) -> String {
        match mask {
            Some(v) => match source.index.data.match_flag {
                MatchFlag::Yes => match &v.yes {
                    Some(s) => s.to_string(),
                    None => MatchFlag::Yes.to_string()
                },
                MatchFlag::No => match &v.no {
                    Some(s) => s.to_string(),
                    None => MatchFlag::No.to_string()
                },
                MatchFlag::Skip => match &v.skip {
                    Some(s) => s.to_string(),
                    None => MatchFlag::Skip.to_string()
                },
                MatchFlag::None => match &v.none {
                    Some(s) => s.to_string(),
                    None => MatchFlag::None.to_string()
                }
            },
            None => source.index.data.match_flag.to_string()
        }
    }
}

impl<T: ExporterWriter> ExporterWriter for &'_ mut T {
    fn write(&mut self, text: &str) -> Result<()> {
        (**self).write(text)
    }

    fn write_headers(&mut self, headers: &[String]) -> Result<()> {
        (**self).write_headers(headers)
    }

    fn write_data(&mut self, fields: &[ExportField], source: ExportData, is_first: bool) -> Result<()> {
        (**self).write_data(fields, source, is_first)
    }

    fn write_end(&mut self) -> Result<()> {
        (**self).write_end()
    }
}

struct ExporterCSVWriter<W: Write> {
    pub writer: csv::Writer<W>
}

impl<W: Write> ExporterCSVWriter<W> {
    /// Filter a single field into a String vector.
    /// 
    /// # Arguments
    /// 
    /// * `field` - Export field.
    /// * `data` - String vector to store values into.
    /// * `source` - Source data to filter.
    fn filter_single(field: &ExportField, data: &mut Vec<String>, source: &ExportData) {
        let value =  match field {
            ExportField::SpentTime{label: _, decimal} => Self::calc_spent_time(*decimal, source).to_string(),
            ExportField::MatchFlag{label: _, mask} => Self::calc_match_flag(mask, source),
            ExportField::Input{label: _, name} => match source.input.get(name) {
                Some(v) => match v {
                    JSValue::String(s) => s.to_string(),
                    jsv => jsv.to_string()
                },
                None => "".to_string()
            },
            ExportField::Record{label: _, name} => match source.record.get(name) {
                Some(v) => v.to_string(),
                None => "".to_string()
            },
            ExportField::AllInput{overrides} => {
                for s in source.input_headers.iter() {
                    // apply field override
                    if let Some(map) = overrides {
                        if let Some(new_field) = map.get(s) {
                            Self::filter_single(new_field, data, source);
                            continue
                        }
                    }

                    // add field value
                    let val = match source.input.get(s) {
                        Some(v) => match v {
                            JSValue::String(s) => s.to_string(),
                            jsv => jsv.to_string()
                        },
                        None => "".to_string()
                    };
                    data.push(val);
                }
                return
            },
            ExportField::AllRecord{overrides} => {
                for (s, v) in source.record.iter() {
                    // apply field override
                    if let Some(map) = overrides {
                        if let Some(new_field) = map.get(s) {
                            Self::filter_single(new_field, data, source);
                            continue
                        }
                    }

                    // add field value
                    data.push(v.to_string());
                }
                return
            }
            ExportField::None{label: _} => "".to_string(),
        };
        data.push(value);
    }

    /// Filter all fields value into a String vector.
    /// 
    /// # Arguments
    /// 
    /// * `fields` - Export fields.
    /// * `source` - Source data to filter.
    fn filter_data(fields: &[ExportField], source: ExportData) -> Vec<String> {
        let mut data = Vec::new();
        for field in fields {
            Self::filter_single(field, &mut data, &source)
        }
        data
    }
}

impl<W: Write> ExporterWriter for ExporterCSVWriter<W> {
    fn write(&mut self, _: &str) -> Result<()> {
        unimplemented!()
    }

    fn write_headers(&mut self, headers: &[String]) -> Result<()> {
        self.writer.write_record(headers)?;
        Ok(())
    }

    fn write_data(&mut self, fields: &[ExportField], source: ExportData, _: bool) -> Result<()> {
        let data = Self::filter_data(fields, source);
        self.writer.write_record(&data)?;
        Ok(())
    }

    fn write_end(&mut self) -> Result<()> {
        Ok(())
    }
}

struct ExporterJSONWriter<W: Write> {
    pub writer: W
}

impl<W: Write> ExporterJSONWriter<W> {
    /// Filter a single field into a Json map.
    /// 
    /// # Arguments
    /// 
    /// * `field` - Export field.
    /// * `data` - String vector to store values into.
    /// * `source` - Source data to filter.
    fn filter_single(field: &ExportField, data: &mut JSMap<String, JSValue>, source: &ExportData) {
        match field {
            ExportField::SpentTime{label, decimal} => {
                let value = JSValue::Number(JSNumber::from_f64(
                    Self::calc_spent_time(*decimal, source)
                ).unwrap());
                let key = match label {
                    Some(s) => s,
                    None => "spent_time"
                };
                data[key] = value;
            },
            ExportField::MatchFlag{label, mask} => {
                let value = JSValue::String(Self::calc_match_flag(mask, source));
                let key = match label {
                    Some(s) => s,
                    None => "matched"
                };
                data[key] = value;
            },
            ExportField::Input{label, name} => {
                let value = match source.input.get(name) {
                    Some(v) => v.clone(),
                    None => JSValue::Null
                };
                let key = match label {
                    Some(s) => s,
                    None => &name
                };
                data[key] = value;
            },
            ExportField::Record{label, name} => {
                let value = match source.record.get(name) {
                    Some(v) => v.into(),
                    None => JSValue::Null
                };
                let key = match label {
                    Some(s) => s,
                    None => &name
                };
                data[key] = value;
            },
            ExportField::AllInput{overrides} => {
                for (s, v) in source.input.iter() {
                    // apply field override
                    if let Some(map) = overrides {
                        if let Some(new_field) = map.get(s) {
                            Self::filter_single(new_field, data, source);
                            continue
                        }
                    }

                    // add field value
                    data[s] = v.clone();
                }
            },
            ExportField::AllRecord{overrides} => {
                for (s, v) in source.record.iter() {
                    // apply field override
                    if let Some(map) = overrides {
                        if let Some(new_field) = map.get(s) {
                            Self::filter_single(new_field, data, source);
                            continue
                        }
                    }

                    // add field value
                    data[s] = v.into();
                }
            },
            ExportField::None{label} => data[label] = "".into(),
        };
    }

    /// Filter all field values into a Json map.
    /// 
    /// # Arguments
    /// 
    /// * `fields` - Export fields.
    /// * `source` - Source data to filter.
    fn filter_data(fields: &[ExportField], source: ExportData) -> JSMap<String, JSValue> {
        let mut data = JSMap::new();
        for field in fields {
            Self::filter_single(field, &mut data, &source)
        }
        data
    }
}

impl<W: Write> ExporterWriter for ExporterJSONWriter<W> {
    fn write(&mut self, text: &str) -> Result<()> {
        self.writer.write_all(text.as_bytes())?;
        Ok(())
    }

    fn write_headers(&mut self, _: &[String]) -> Result<()> {
        self.writer.write_all(&[b'['])?;
        Ok(())
    }

    fn write_data(&mut self, fields: &[ExportField], source: ExportData, is_first: bool) -> Result<()> {
        let data = Self::filter_data(fields, source);
        if !is_first {
            self.writer.write_all(&[b','])?;
        }
        serde_json::to_writer(&mut self.writer, &data)?;
        Ok(())
    }

    fn write_end(&mut self) -> Result<()> {
        self.writer.write_all(&[b']'])?;
        Ok(())
    }
}

/// Represent an exporter instance.
pub struct Exporter<'s> {
    /// Data source.
    pub source: &'s Source,

    /// Output file type
    pub file_type: ExportFileType
}

impl<'s> Exporter<'s> {
    /// Creates a new exporter
    /// 
    /// # Arguments
    /// 
    /// * `source` - Data source.
    /// * `file_type` - Output file type.
    pub fn new(source: &'s Source, file_type: ExportFileType) -> Self {
        Self{
            source,
            file_type
        }
    }

    fn add_csv_headers(&self, field: &ExportField, headers: &mut Vec<String>, input_headers: &[String]) {
        let field_name = match field {
            ExportField::SpentTime{label, decimal: _} => match label {
                Some(s) => s.to_string(),
                None => "spent_time".to_string()
            },
            ExportField::MatchFlag{label, mask: _} => match label {
                Some(s) => s.to_string(),
                None => "matched".to_string()
            },
            ExportField::Input{label, name} => match label {
                Some(s) => s.to_string(),
                None => name.to_string()
            },
            ExportField::Record{label, name} => match label {
                Some(s) => s.to_string(),
                None => name.to_string()
            },
            ExportField::AllInput{overrides} => {
                for s in input_headers.iter() {
                    // apply field override
                    if let Some(map) = overrides {
                        if let Some(new_field) = map.get(s) {
                            self.add_csv_headers(new_field, headers, input_headers);
                            continue
                        }
                    }

                    // add field header
                    headers.push(s.to_string());
                }
                return
            },
            ExportField::AllRecord{overrides} => {
                for v in self.source.table.record_header.iter() {
                    let name = v.get_name();

                    // apply field override
                    if let Some(map) = overrides {
                        if let Some(new_field) = map.get(name) {
                            self.add_csv_headers(new_field, headers, input_headers);
                            continue
                        }
                    }

                    // add field header
                    headers.push(name.to_string());
                }
                return
            }
            ExportField::None{label} => label.to_string()
        };
        headers.push(field_name);
    }

    /// Export the input plus records data into a csv writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `fields` - List of fields to export.
    fn export_from_csv(&self, writer: &mut impl ExporterWriter, fields: &[ExportField], match_filter: Option<&[MatchFlag]>) -> Result<()> {
        // create the index reader and move to first value
        let mut index_rdr = self.source.index.new_index_reader()?;
        let pos = Indexer::calc_value_pos(0);
        index_rdr.seek(SeekFrom::Start(pos))?;

        // create the table reader and move to first record
        let mut table_rdr = self.source.table.new_reader()?;
        let pos = self.source.table.calc_record_pos(0);
        table_rdr.seek(SeekFrom::Start(pos))?;

        // create input CSV reader
        let input_rdr = self.source.index.new_input_reader()?;
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(input_rdr);
        let mut input_headers = Vec::new();
        for s in csv_reader.headers()? {
            input_headers.push(s.to_string());
        }

        // write headers
        let mut headers = Vec::new();
        for field in fields {
            self.add_csv_headers(field, &mut headers, &input_headers);
        }
        writer.write_headers(&headers)?;
        
        // iterate input as CSV
        let mut is_first = true;
        for result in csv_reader.deserialize() {
            // read input and source data
            let export_data = ExportData{
                input_headers: input_headers.clone(),
                input: result?,
                index: IndexValue::read_from(&mut index_rdr)?,
                record: self.source.table.record_header.read_record(&mut table_rdr)?
            };

            // filter by match flag when required
            if let Some(filter) = match_filter {
                if !filter.iter().any(|&v|v==export_data.index.data.match_flag) {
                    continue;
                }
            }

            // write data
            writer.write_data(fields, export_data, is_first)?;

            // the first record has been added, so set is_first flag to false 
            if is_first {
                is_first = false;
            }
        };

        writer.write_end()?;
        Ok(())
    }

    /// Export the source data into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `source` - Data source to export.
    /// * `file_type` - Output file type.
    /// * `fields` - List of fields to export.
    pub fn export_to(&self, writer: &mut impl Write, fields: &[ExportField], match_filter: Option<&[MatchFlag]>) -> Result<()> {
        // validate before export
        if !self.source.index.header.indexed {
            bail!("input file must be indexed to be exported");
        }

        // export data
        match self.file_type {
            ExportFileType::CSV => {
                let mut exporter_writer = ExporterCSVWriter{
                    writer: csv::Writer::from_writer(writer)
                };
                match self.source.index.header.input_type {
                    InputType::CSV => self.export_from_csv(
                        &mut exporter_writer,
                        fields,
                        match_filter
                    ),
                    InputType::JSON => unimplemented!(),
                    InputType::Unknown => bail!("unsupported input file type")
                }
            },
            ExportFileType::JSON => {
                let mut exporter_writer = ExporterJSONWriter{
                    writer
                };
                match self.source.index.header.input_type {
                    InputType::CSV => self.export_from_csv(
                        &mut exporter_writer,
                        fields,
                        match_filter
                    ),
                    InputType::JSON => unimplemented!(),
                    InputType::Unknown => bail!("unsupported input file type")
                }
            }
        }
    }

    /// Export the source data into an output file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Output file path.
    /// * `fields` - Fields to be exported.
    pub fn export(&self, output_path: PathBuf, fields: &[ExportField], match_filter: Option<&[MatchFlag]>) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&output_path)?;
        let mut writer = BufWriter::new(file);
        self.export_to(&mut writer, fields, match_filter)
    }
}

#[cfg(tests)]
mod tests {
    use super::*;
//     /// Return the fake output content as bytes.
//     pub fn fake_output_bytes() -> Vec<u8> {
//         let buf = build_empty_extra_fields().to_vec();
//         let eef = String::from_utf8(buf).unwrap();
//         format!("\
//             name,size,price,color,match,time,comments\n\
//             fork,\"1 inch\",12.34,red{}\n\
//             keyboard,medium,23.45,\"black\nwhite\"{}\n\
//             mouse,\"12 cm\",98.76,white{}\n\
//             \"rust book\",500 pages,1,\"orange\"{}\
//         ", eef, eef, eef, eef).as_bytes().to_vec()
//     }

//     /// Create a fake output file based on the default fake input file.
//     /// 
//     /// # Arguments
//     /// 
//     /// * `path` - Output file path.
//     pub fn create_fake_output(path: &str) -> Result<()> {
//         let file = OpenOptions::new()
//             .create(true)
//             .truncate(true)
//             .write(true)
//             .open(path)?;
//         let mut writer = BufWriter::new(file);
//         writer.write_all(&fake_output_bytes())?;
//         writer.flush()?;

//         Ok(())
//     }
}