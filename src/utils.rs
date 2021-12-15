//! Utils module containing several functions and structs.

use serde::{Deserialize};
use std::fs::{File};
use std::io::{self, Seek, SeekFrom, BufRead, BufReader};
use difference::{Changeset, Difference};

/// Data type describing an action to apply to the compare.
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub struct ApplyData {
    pub approved: bool,
    pub skip: bool,
    pub time: i64,
    pub comments: String
}

pub fn read_csv_line(path: &str, pos: u64) -> io::Result<(Vec<u8>, u64)> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(pos))?;

    // read data from the built CSV string by using the headers for easy access
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(reader);

    // read one line
    let mut buf: Vec<u8> = vec!();
    if let Some(result) = rdr.deserialize().next() {
        buf = result?;
    }

    // return line and next position
    let next_pos = rdr.position().byte();
    Ok((buf, next_pos))
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
pub fn read_line(path: &str, start_pos: u64) -> io::Result<(Vec<u8>, u64, u64)> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let pos = start_pos;

    // make sure the file pointer is at the start of a line
    if start_pos > 0 {
        // find closest new line position
        reader.seek(SeekFrom::Start(start_pos-1))?;
        let mut disposable_buf = Vec::new();
        reader.read_until(b'\n', &mut disposable_buf)?;

        // move to closest line first char
        let pos = reader.stream_position()?;
        reader.seek(SeekFrom::Start(pos))?;
    }

    // read one line
    let mut buf: Vec<u8> = Vec::new();
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
pub fn parse_line(headers: &str, path: &str, start_pos: u64) -> Result<(serde_json::Value, u64, u64), String> {
    // get closest line bytes, position and next line position
    let (raw_data, pos, next_pos) = match read_line(path, start_pos) {
        Ok(v) => v,
        Err(e) => return Err(e.to_string())
    };

    // build CSV string using headers and line bytes
    let csv_text = format!("{}\n{}", headers, String::from_utf8(raw_data).unwrap());
    
    // read data from the built CSV string by using the headers for easy access
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(csv_text.as_bytes());
    
    // deserialize CSV string object into a JSON object
    for result in rdr.deserialize::<serde_json::Map<String, serde_json::Value>>() {
        match result {
            Ok(record) => {
                // return data after the first successful record
                return Ok((serde_json::Value::Object(record), pos, next_pos))
            }
            Err(e) => {
                println!("Couldn't parse the data at position {}: {}", start_pos, e);
            }
        }
    }

    // error out if no valid record found
    Err(format!("Couldn't parse the data at position {}", start_pos))
}

/// Encode html entities on a string.
pub fn encode_html(s: &str) -> String {
    html_escape::encode_text(&s).to_string()
}

/// Diff 2 texts and add html tags to it's differences.
/// 
/// # Arguments
/// 
/// * `before` - The original value before changes.
/// * `after` - The value after changes.
pub fn diff_html_single(before: &str, after: &str) -> String {
    let before = html_escape::decode_html_entities(&before);
    let after = html_escape::decode_html_entities(&after);
    let changeset = Changeset::new(&before, &after, "");

    let mut text = String::new();
    for change in changeset.diffs {
        let segment: String = match change {
            Difference::Same(v) => encode_html(&v),
            Difference::Rem(_) => "".to_string(),
            Difference::Add(v) => format!("<span class='diff'>{}</span>", encode_html(&v))
        };

        text.push_str(&segment);
    }

    text
}