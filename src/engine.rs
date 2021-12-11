pub mod parse_error;
pub mod index;

use serde::Deserialize;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use index::indexer::Indexer;
use index::index_value::MatchFlag;
use parse_error::ParseError;

const BUF_SIZE: u64 = 4096;

/// Fill function action.
#[derive(Debug, PartialEq)]
pub enum FillAction {
    Created,
    Fill,
    Truncated,
    Bigger,
    Skip
}

/// Engine to manage index and navigation.
#[derive(Debug, Deserialize)]
pub struct Engine {
    /// Indexer engine object.
    pub index: Indexer
}

impl Engine {
    /// Creates a new engine and default index path as
    /// `<input_path>.matchqa.index` if not provided.
    /// 
    /// # Arguments
    /// 
    /// * `input_path` - Input file path.
    /// * `output_path` - Output file path.
    /// * `index_path` - Index path (Optional).
    pub fn new(input_path: &str, output_path: &str, index_path: Option<&str>) -> Self {
        let index_path = match index_path {
            Some(s) => s.to_string(),
            None => format!("{}.matchqa.index", input_path)
        };
        let input_path = input_path.to_string();
        let output_path = output_path.to_string();
        let indexer_obj = Indexer::new(
            &input_path,
            &output_path,
            &index_path
        );
        Self{
            index: indexer_obj
        }
    }

    /// Regenerates the index file based on the input file.
    pub fn index(&mut self) -> Result<(), ParseError> {
        self.index.index()
    }

    pub fn format_time(time_milis: u64) -> String {
        format!("{:0>20}", (time_milis as f32) / 1000f32)
    }

    pub fn format_comments(comments: &str) -> String {
        format!("\"{: <200}\"", comments)
    }

    pub fn record_output(&self, index: u64, match_flag: MatchFlag, time_milis: u64, comments: &str) -> Result<(), ParseError> {
        if comments.len() > 200 {
            return Err(ParseError::InvalidSize);
        }

        let mut value = match self.index.get_record_index(index)? {
            Some(v) => v,
            None => return Err(ParseError::InvalidValue)
        };

        // write output match data
        let file = OpenOptions::new()
            .write(true)
            .open(&self.index.output_path)?;
        let mut writer = BufWriter::new(file);
        writer.seek(SeekFrom::Start(value.output_pos))?;
        writer.write(&[b',', (&match_flag).into(), b','])?;
        writer.write(Self::format_time(time_milis).as_bytes())?;
        writer.write(&[b','])?;
        writer.write(Self::format_comments(comments).as_bytes())?;
        writer.flush()?;

        value.match_flag = match_flag.clone();
        self.index.update_index_value(index, &value)?;

        Ok(())
    }

    pub fn find_to_process(&self, from_index: u64) -> Result<Option<u64>, ParseError> {
        let (index, _) = match self.index.find_unmatched(from_index)? {
            Some(v) => v,
            None => return Ok(None)
        };
        Ok(Some(index))
    }

    pub fn get_data(&self, index: u64) -> Result<serde_json::Value, ParseError> {
        let first_value = match self.index.get_record_index(0)? {
            Some(v) => v,
            None => return Ok(serde_json::Value::Null)
        };
        let value = match self.index.get_record_index(index)? {
            Some(v) => v,
            None => return Ok(serde_json::Value::Null)
        };

        // build a fake CSV string
        let file = File::open(&self.index.input_path)?;
        let mut reader = BufReader::new(file);
        let mut buf: Vec<u8> = vec![0u8; first_value.input_start_pos as usize];
        reader.read(&mut buf)?;
        buf.push(b'\n');
        reader.seek(SeekFrom::Start(value.input_start_pos))?;
        let mut buf_value: Vec<u8> = vec![0u8; (value.input_end_pos - value.input_start_pos + 1) as usize];
        reader.read(&mut buf_value)?;
        buf.append(&mut buf_value);

        // read data
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(buf.as_slice());

        // deserialize CSV string object into a JSON object
        for result in reader.deserialize::<serde_json::Map<String, serde_json::Value>>() {
            match result {
                Ok(record) => {
                    // return data after the first successful record
                    return Ok(serde_json::Value::Object(record))
                }
                Err(e) => {
                    println!("Couldn't parse record at position {}: {}", value.input_start_pos, e);
                    return Err(ParseError::InvalidFormat)
                }
            }
        }

        Ok(serde_json::Value::Null)
    }
}

/// Get a file size.
/// 
/// # Arguments
/// 
/// * `path` - File path.
/// * `create` - If `true` then file will be created if not exists.
pub fn file_size(path: &str) -> std::io::Result<u64> {
    if !std::path::Path::new(path).exists() {
        return Ok(0);
    }
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::End(0))?;
    Ok(reader.stream_position()?)
}

/// Fill a file with zero byte until the target size or ignore if
/// bigger. Return true if file is bigger.
/// 
/// # Arguments
/// 
/// * `path` - File path to fill.
/// * `target_size` - Target file size in bytes.
/// * `truncate` - If `true` then it truncates de file and fill it.
pub fn fill_file(path: &str, target_size: u64, truncate: bool) -> std::io::Result<FillAction> {
    let mut action = FillAction::Fill;
    let file = if truncate {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?
    } else {
        OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(path)?
    };

    // get file size
    file.sync_all()?;
    let mut size = file.metadata()?.len();

    // change default action to created when new file
    if size < 1 {
        action = FillAction::Created;
    }

    // validate file current size vs target size
    if truncate {
        action = FillAction::Truncated;
    } else {
        if target_size < size {
            // file is bigger, return true
            return Ok(FillAction::Bigger);
        }
        if target_size == size {
            return Ok(FillAction::Skip);
        }
    }

    // fill file with zeros until target size is match
    let buf_size = 4096u64;
    let buf = [0u8; 4096];
    let mut wrt = BufWriter::new(file);
    while size + buf_size < target_size {
        wrt.write_all(&buf)?;
        size += buf_size;
        wrt.flush()?;
    }
    let remaining = (target_size - size) as usize;
    if remaining > 0 {
        wrt.write_all(&buf[..remaining])?;
    }
    wrt.flush()?;

    Ok(action)
}

/// Generates a hash value from a file contents.
/// 
/// # Arguments
/// 
/// * `path` - File path.
pub fn generate_hash(path: &str) -> std::io::Result<[u8; blake3::OUT_LEN]> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();

    loop {
        let mut chunk = vec![0u8; BUF_SIZE as usize];
        let bytes_count = reader.by_ref().take(BUF_SIZE).read_to_end(&mut chunk)?;
        if bytes_count == 0 {
            break;
        }
        hasher.update(&chunk[0..bytes_count]);
        if bytes_count < BUF_SIZE as usize {
            break;
        }
    }
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use index::index_header::HASH_SIZE;
    use crate::test_helper::*;
    use tempfile::TempDir;
    use std::io::{Read, BufReader};

    #[test]
    fn file_size_with_file() {
        with_tmpdir(&|dir: &TempDir| -> std::io::Result<()> {
            // test one
            let path = dir.path().join("my_file_a");
            let path_str = path.to_str().unwrap().to_string();
            create_file_with_bytes(&path_str, &[0u8; 34])?;
            assert_eq!(34, file_size(&path_str)?);

            // test two
            let path = dir.path().join("my_file_b");
            let path_str = path.to_str().unwrap().to_string();
            create_file_with_bytes(&path_str, &[0u8; 24])?;
            assert_eq!(24, file_size(&path_str)?);

            Ok(())
        });
    }

    #[test]
    fn file_size_without_file() {
        with_tmpdir(&|dir: &TempDir| -> std::io::Result<()> {
            let path = dir.path().join("my_file_non_exists");
            let path_str = path.to_str().unwrap().to_string();
            assert_eq!(0, file_size(&path_str)?);
            assert_eq!(false, path.exists());
            drop(path);

            Ok(())
        });
    }

    #[test]
    fn gen_hash() {
        with_tmpdir(&|dir: &TempDir| -> std::io::Result<()> {
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            create_file_with_bytes(&path_str, buf)?;
            
            let expected: &[u8] = &[64, 119, 46, 20, 183, 102, 90, 142, 127, 9,
                222, 65, 218, 9, 196, 25, 26, 202, 193, 50, 165, 152, 228, 227,
                99, 208, 118, 225, 144, 119, 5, 122];
            let value = generate_hash(&path_str)?;
            assert_eq!(HASH_SIZE, value.len());
            assert_eq!(expected, value);
            
            Ok(())
        });
    }

    #[test]
    fn fill_file_non_exists() {
        with_tmpdir(&|dir: &TempDir| -> std::io::Result<()> {
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            
            // fill file
            match fill_file(&path_str, 20, false) {
                Ok(action) => assert_eq!(FillAction::Created, action),
                Err(e) => return Err(e)
            }

            // read file after fill
            let file = File::open(&path_str)?;
            let mut reader = BufReader::new(file);
            let mut buf: Vec<u8> = vec!();
            reader.read_to_end(&mut buf)?;

            // compare
            let expected = [0u8; 20].to_vec();
            assert_eq!(expected, buf);

            // drop file
            drop(path);
            Ok(())
        });
    }

    #[test]
    fn fill_file_smaller() {
        with_tmpdir(&|dir: &TempDir| -> std::io::Result<()> {
            // create test file
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: [u8; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            create_file_with_bytes(&path_str, &buf)?;

            // fill file
            match fill_file(&path_str, 15, false) {
                Ok(action) => assert_eq!(FillAction::Fill, action),
                Err(e) => return Err(e)
            }

            // read file after fill
            let file = File::open(&path_str)?;
            let mut reader = BufReader::new(file);
            let mut buf: Vec<u8> = vec!();
            reader.read_to_end(&mut buf)?;

            // compare
            let expected = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0].to_vec();
            assert_eq!(expected, buf);

            // drop test file
            drop(path);
            Ok(())
        });
    }

    #[test]
    fn fill_file_bigger() {
        with_tmpdir(&|dir: &TempDir| -> std::io::Result<()> {
            // create test file
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: [u8; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            create_file_with_bytes(&path_str, &buf)?;

            // fill file
            match fill_file(&path_str, 10, false) {
                Ok(action) => assert_eq!(FillAction::Bigger, action),
                Err(e) => return Err(e)
            }

            // read file afer fill
            let file = File::open(&path_str)?;
            let mut reader = BufReader::new(file);
            let mut buf: Vec<u8> = vec!();
            reader.read_to_end(&mut buf)?;

            // compare
            let expected = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15].to_vec();
            assert_eq!(expected, buf);

            // drop test file
            drop(path);
            Ok(())
        });
    }

    #[test]
    fn fill_file_equal() {
        with_tmpdir(&|dir: &TempDir| -> std::io::Result<()> {
            // create test file
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: [u8; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            create_file_with_bytes(&path_str, &buf)?;

            // fill file
            match fill_file(&path_str, 15, false) {
                Ok(action) => assert_eq!(FillAction::Skip, action),
                Err(e) => return Err(e)
            }

            // read file after fill
            let file = File::open(&path_str)?;
            let mut reader = BufReader::new(file);
            let mut buf: Vec<u8> = vec!();
            reader.read_to_end(&mut buf)?;

            // compare
            let expected = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15].to_vec();
            assert_eq!(expected, buf);

            // drop test file
            drop(path);
            Ok(())
        });
    }

    #[test]
    fn fill_file_truncate() {
        with_tmpdir(&|dir: &TempDir| -> std::io::Result<()> {
            // create test file
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: [u8; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            create_file_with_bytes(&path_str, &buf)?;

            // fill file
            match fill_file(&path_str, 10, true) {
                Ok(action) => assert_eq!(FillAction::Truncated, action),
                Err(e) => return Err(e)
            }

            // read file after fill
            let file = File::open(&path_str)?;
            let mut reader = BufReader::new(file);
            let mut buf: Vec<u8> = vec!();
            reader.read_to_end(&mut buf)?;

            // compare
            let expected = [0u8; 10].to_vec();
            assert_eq!(expected, buf);

            // drop test file
            drop(path);
            Ok(())
        });
    }
}