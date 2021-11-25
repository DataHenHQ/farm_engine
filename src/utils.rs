//! Utils module containing several functions and structs.

use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write, BufRead, BufWriter};

const BUF_SIZE: usize = 4096;
const EMPTY_RESULT_LINE_SIZE: usize = 40;

/// User config sample file.
const CONFIG_SAMPLE: &str = r#"
{
  "ui": {
    "image_url": {
      "a": "dh_image_url",
      "b": "match_image_url",
    },
    "product_name": {
      "a": "dh_product_name",
      "b": "match_product_name",
    },
    "data": [
      {
        "label": "Size",
        "a": "dh_size_std",
        "b": "match_size_std"
      }, {
        "label": "Size Unit",
        "a": "dh_size_unit",
        "b": "match_size_unit"
      }, {
        "label": "Price",
        "a": "dh_price",
        "b": "match_price"
      }, {
        "label": "GID",
        "a": "dh_global_id",
        "b": null,
        "show_more": true
      }
    ]
  }
}
"#;

/// UI data value used to describe an extra data compare field.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiDataValue {
    /// Label to be display on the compare UI.
    pub label: String,
    /// Product A field header key.
    pub a: Option<String>,
    /// Product B field header key.
    pub b: Option<String>,
    /// Show more flag, will be hidden when true until the user
    /// enable `show more` feature.
    pub show_more: Option<bool>
}

/// UI configuration used to describe the compare view.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiConfig {
    /// Image url compare UI configuration.
    pub image_url: Option<UiDataValue>,
    /// Product name compare UI configuration.
    pub product_name: Option<UiDataValue>,
    /// Extra data compare UI configuration collection.
    pub data: Vec<UiDataValue>
}

/// User configuration build from a JSON file.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserConfig {
    /// UI configuration object.
    pub ui: UiConfig
}

impl UserConfig {
    /// Build a UiConfig object from a JSON file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - JSON file path.
    pub fn from_file(path: &str) -> io::Result<UserConfig> {
        // open the file in read-only mode with buffer
        let file = File::open(path)?;
        let reader = io::BufReader::new(file);
    
        // read the JSON contents of the file into the user config
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}

/// Application configuration.
#[derive(Debug)]
pub struct AppConfig {
    /// Input file path.
    pub input: String,
    /// Output file path.
    pub output: String,
    /// CSV input file headers line string.
    pub headers: String,
    /// First data line from the input CSV file.
    pub start_pos: u64,
    /// User configuration object created from the provided JSON
    /// config file.
    pub user_config: UserConfig
}

/// Data type describing an action to apply to the compare.
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub struct ApplyData {
    pub approved: bool,
    pub skip: bool,
    pub time: i64
}

/// Reads the closest line on a CSV file from the provided position
/// and returns the line contents as bytes, the closes line position
/// and the next line position.
/// 
/// # Arguments
/// 
/// * `path` - CSV file path.
/// * `start_pos`  - File position from which search the closest line.
/// 
/// # Examples
/// 
/// ```
/// let file_path = "my_file.csv".to_string();
/// let start_pos = 10;
/// let (buf, pos, next_pos) = read_line(&file_path, start_pos).unwrap();
/// println!(String::from_utf8(buf).unwrap());
/// ```
pub fn read_line(path: &String, start_pos: u64) -> io::Result<(Vec<u8>, u64, u64)> {
    let file = File::open(path)?;
    let mut reader = io::BufReader::new(file);
    let mut pos = start_pos;

    // make sure the file pointer is at the start of a line
    if start_pos > 0 {
        // find closest new line position
        reader.seek(SeekFrom::Start(start_pos-1))?;
        let mut disposable_buf = Vec::new();
        reader.read_until(b'\n', &mut disposable_buf)?;

        // move to closest line first char
        let current_pos = reader.stream_position()?;
        pos = current_pos + 1u64;
        reader.seek(SeekFrom::Start(pos))?;
    }

    // read one line
    let mut buf = Vec::new();
    reader.read_until(b'\n', &mut buf)?;

    // remove any new line at the end
    if let Some(last) = buf.last() {
        if *last == b'\n' {
            buf.pop();
        }
        if let Some(last) = buf.last() {
            if *last == b'\r' {
                buf.pop();
            }
        }
    }

    // return line bytes, line position and next line position
    let next_pos = reader.stream_position()?;
    Ok((buf, pos, next_pos))
}

/// Parse the closest line contents into a JSON object and returns
/// the JSON object, the closest line position, and the next line
/// position.
/// 
/// # Arguments
/// 
/// * `headers` - Headers line string.
/// * `path` - CSV file path to read.
/// * `start_pos` - File position from which search the closest line.
pub fn parse_line(headers: &String, path: &String, start_pos: u64) -> Result<(serde_json::Value, u64, u64), String> {
    // get closest line bytes, position and next line position
    let (raw_data, pos, next_pos) = match read_line(path, start_pos) {
        Ok(v) => v,
        Err(e) => return Err(format!("{}", e))
    };

    // build CSV string using headers and line bytes
    let csv_text = format!("{}\n{}", headers, String::from_utf8(raw_data).unwrap());
    
    // read data from the built CSV string by using the headers for easy access
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(csv_text.as_bytes());
    
    // deserialize CSV string object into a JSON object
    for result in rdr.deserialize() {
        match result {
            Ok(record) => {
                // return data after the first successful record
                return Ok((record, pos, next_pos))
            }
            Err(e) => {
                println!("Couldn't parse the data at position {}: {}", start_pos, e);
            }
        }
    }

    // error out if no valid record found
    Err(format!("Couldn't parse the data at position {}", start_pos))
}

/// Write match data to the output file by using the closes line
/// data from the input file. Return io::Result.
/// 
/// # Arguments
/// 
/// * `config` - Application configuration containing input, output and headers data.
/// * `start_pos` - File position from which search the closest line.
/// * `append` - Append flag to decide whenever append or override the output file.
pub fn write_line(config: &AppConfig, text: String, start_pos: u64, append: bool) -> io::Result<()> {
    // get data from input file
    let (buf, pos, _) = read_line(&config.input, start_pos)?;

    // decide on append or just override, then open file
    let mut output_file = if append {
        OpenOptions::new().create(true).append(true).open(&config.output)?
    } else {
        OpenOptions::new().create(true).write(true).truncate(true).open(&config.output)?
    };

    // write new match data to output file
    let text = match text.len() {
        0 => format!("{}{}", String::from_utf8(buf).unwrap(), text),
        _ => format!("{},{}", String::from_utf8(buf).unwrap(), text)
    };
    writeln!(output_file, "{}", text)?;

    Ok(())
}

/// Analyze an input file to track new lines and record it's positions.
/// Returns total line count.
/// 
/// # Arguments
/// 
/// * `input_path` - File path to analize.
/// * `result_path` - File path to write results.
pub fn analize(input_path: &str, result_path: &str) -> io::Result<u64> {
    let file = File::open(input_path)?;
    let result_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(result_path)?;
    
    let empty_line: [U8; EMPTY_RESULT_LINE_SIZE] = [u8; EMPTY_RESULT_LINE_SIZE];
    file.write

    unimplemented!();
}

/// Fill a file with zero byte until the target size or ignore if
/// bigger. Return true if file is bigger.
/// 
/// # Arguments
/// 
/// * `path` - File path to fill.
/// * `target_size` - Target file size in bytes.
pub fn fill_file(path: &str, target_size: u64) -> io::Result<bool> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)?;

    file.sync_all()?;
    let size = file.metadata()?.len();

    // validate file current size vs target size
    if target_size < size {
        // file is bigger, return true
        return Ok(true);
    }
    if target_size == size {
        return Ok(false);
    }

    // fill file with zeros until target size is match
    let buf_size = 4096u64;
    let buf = [0u8; 4096];
    let wrt = BufWriter::new(file);
    while size + buf_size < target_size {
        wrt.write_all(&buf)?;
        size += buf_size;
    }
    let remaining = (target_size - size) as usize;
    if remaining > 0 {
        wrt.write_all(&buf[..remaining])?;
    }

    Ok(false)
}