#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;
#[macro_use] extern crate serde_derive;

use rocket_contrib::serve::StaticFiles;
use rocket_contrib::templates::Template;
use rocket_contrib::json::Json;
use rocket::Data;
use rocket::State;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write, BufRead};

#[derive(Debug)]
struct AppConfig {
    pub input: String,
    pub output: String
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct ApplyData {
    approved: bool,
    time: u64
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SnakeCase")]
struct Record {
    similarity,
    match_y_n,
    dh_product_name,
    match_product_name,
    dh_size_unit_std,
    match_size_unit_std,
    dh_size_std,
    match_size_std,
    dh_num_pieces,
    match_num_pieces,
    dh_price,
    match_price,
    dh_sku,
    match_sku,
    dh_img_url,
    image_url,
    dh_image,
    shopee_image
}

fn read_line(path: &String, pos: u64) -> io::Result<(Vec<u8>, u64)> {
    let mut file = File::open(path)?;
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
        if *last == b'\r' {
            buf.pop();
        }
    }

    let current_pos = reader.stream_position()?;
    Ok((buf, reader.stream_position()?))
}

fn parse_line(headers: &String, path: &String, pos: u64) -> Result<(Record, u64), String> {
    let (raw_data, new_pos) = match read_line(path, pos) {
        Ok(v) => v,
        Err(e) => return Err(format!("{}", e))
    };
    let csv_text = format!("{}\n{}", headers, String::from_utf8(raw_data).unwrap());
    
    // read data from line by using the headers for easy access
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_text.as_bytes());
    for result in rdr.records() {
        if let Ok(record) = result {
            return Ok((record, new_pos))
        }
    }
    Err(format!("Couldn't parse the data at position {}", pos))
}

fn write_line(config: &AppConfig, text: String, pos: u64, append: bool) -> io::Result<()> {
    // get data from input file
    let (buf, _) = read_line(&config.input, pos)?;

    // decide on append or just override
    let mut output_file = if append {
        OpenOptions::new().append(true).open(&config.output)?
    } else {
        OpenOptions::new().write(true).open(&config.output)?
    };

    // open file and write
    let text = match text.len() {
        0 => format!("{}{}", String::from_utf8(buf).unwrap(), text),
        _ => format!("{},{}", String::from_utf8(buf).unwrap(), text)
    };
    write!(output_file, "{}", text);

    Ok(())
}