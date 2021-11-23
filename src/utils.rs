use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write, BufRead};

#[derive(Debug)]
pub struct AppConfig {
    pub input: String,
    pub output: String,
    pub headers: String,
    pub start_pos: u64
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
pub struct ApplyData {
    pub approved: bool,
    pub time: i64
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Record {
    pub similarity: String,
    #[serde(rename = "Match (Y/N)")]
    pub match_y_n: String,
    pub dh_product_name: String,
    pub match_product_name: String,
    pub dh_size_unit_std: String,
    pub match_size_unit_std: String,
    pub dh_size_std: String,
    pub match_size_std: String,
    pub dh_num_pieces: String,
    pub match_num_pieces: String,
    pub dh_price: String,
    pub match_price: String,
    pub dh_sku: String,
    pub match_sku: String,
    pub dh_img_url: String,
    pub image_url: String,
    #[serde(rename = "DH image")]
    pub dh_image: String,
    #[serde(rename = "Shopee image")]
    pub shopee_image: String
}

pub fn read_line(path: &String, pos: u64) -> io::Result<(Vec<u8>, u64)> {
    let file = File::open(path)?;
    let mut reader = io::BufReader::new(file);

    // make sure the file pointer is at the start of a line
    if pos > 0 {
        reader.seek(SeekFrom::Start(pos-1))?;
        let mut disposable_buf = Vec::new();
        reader.read_until(b'\n', &mut disposable_buf)?;
        let current_pos = reader.stream_position()?;
        reader.seek(SeekFrom::Start(current_pos + 1u64))?;
    }

    // read one line
    let mut buf = Vec::new();
    reader.read_until(b'\n', &mut buf)?;
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

    Ok((buf, reader.stream_position()?))
}

pub fn parse_line(headers: &String, path: &String, pos: u64) -> Result<(Record, u64), String> {
    let (raw_data, new_pos) = match read_line(path, pos) {
        Ok(v) => v,
        Err(e) => return Err(format!("{}", e))
    };
    let csv_text = format!("{}\n{}", headers, String::from_utf8(raw_data).unwrap());
    
    // read data from line by using the headers for easy access
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(csv_text.as_bytes());
    
    for result in rdr.deserialize() {
        match result {
            Ok(raw_record) => {
                let record: Record = raw_record;
                return Ok((record, new_pos))
            }
            Err(e) => {
                println!("Couldn't parse the data at position {}: {}", pos, e);
            }
        }
    }
    Err(format!("Couldn't parse the data at position {}", pos))
}

pub fn write_line(config: &AppConfig, text: String, pos: u64, append: bool) -> io::Result<()> {
    // get data from input file
    let (buf, _) = read_line(&config.input, pos)?;

    // decide on append or just override
    let mut output_file = if append {
        OpenOptions::new().create(true).append(true).open(&config.output)?
    } else {
        OpenOptions::new().create(true).write(true).truncate(true).open(&config.output)?
    };

    // open file and write
    let text = match text.len() {
        0 => format!("{}{}", String::from_utf8(buf).unwrap(), text),
        _ => format!("{},{}", String::from_utf8(buf).unwrap(), text)
    };
    writeln!(output_file, "{}", text)?;

    Ok(())
}