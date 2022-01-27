pub mod error;
pub mod utils;
pub mod record;
pub mod index;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
use std::str::FromStr;
use sha3::{Digest, Sha3_256};
use path_absolutize::*;
use regex::Regex;
use index::indexer::Indexer;
use index::header::{Header as IndexHeader, HASH_SIZE};
use index::value::{Value as IndexValue, MatchFlag};
use anyhow::{bail, Result};
use self::error::ParseError;

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
#[derive(Debug)]
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
    /// * `index_path` - Index path (Optional).
    pub fn new(input_path: &str, index_path: Option<&str>) -> Self {
        let index_path = match index_path {
            Some(s) => s.to_string(),
            None => format!("{}.matchqa.index", input_path)
        };
        let input_path = input_path.to_string();
        let indexer_obj = Indexer::new(
            &input_path,
            &index_path
        );
        Self{
            index: indexer_obj
        }
    }

    /// Generates a regex expression to validate the index file extension.
    pub fn index_extension_regex() -> Regex {
        Regex::new(r"(?i)\.matchqa\.index$").unwrap()
    }

    /// Validate an index path extension.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Path to validate.
    pub fn validate_index_extension(path: &PathBuf, extension_regex: &Regex) -> bool {
        let file_name = match path.file_name() {
            Some(v) => match v.to_str() {
                Some(s) => s,
                None => return false
            },
            None => return false
        };
        extension_regex.is_match(file_name)
    }

    /// Expands a path and add any index found into the path list.
    /// 
    /// # Arguments
    /// 
    /// * `raw_path` - Path to expand.
    /// * `path_list` - Path list to add the found paths into.
    fn expand_index_path(raw_path: &PathBuf, path_list: &mut Vec<PathBuf>, raw_excludes: &Vec<PathBuf>) -> Result<()> {
        // canonalize the excluded paths
        let mut excludes: Vec<PathBuf> = vec!();
        for raw_exclude in raw_excludes {
            excludes.push(raw_exclude.absolutize()?.to_path_buf());
        }

        // resolve symlink and relative paths
        let path = raw_path.absolutize()?.to_path_buf();

        // check for exclusion
        for exclude in &excludes {
            if path.eq(exclude) {
                return Ok(())
            }
        }

        // check if single file
        if path.is_file() {
            // don't validate the file extension for explicit files,
            // just add the index file
            path_list.push(path);
            return Ok(());
        }
        
        // asume dir since the path is already canonizalized
        let extension_regex = Self::index_extension_regex();
        'dir_iter: for entry in path.read_dir()? {
            let entry = entry?;
            let file_path = entry.path();

            // check for exclusion
            for exclude in &excludes {
                if file_path.eq(exclude) {
                    continue 'dir_iter;
                }
            }

            // skip subdirectories
            if file_path.is_dir() {
                continue;
            }

            // skip non index files
            if !Self::validate_index_extension(&file_path, &extension_regex) {
                continue;
            }

            // add index file
            path_list.push(file_path);
        }

        Ok(())
    }

    /// Writes an output record value into a file writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Output file writer to write into.
    /// * `value` - Record index value.
    /// * `match_flag` - Match flag value to save.
    /// * `track_time` - Tracked time value to save.
    /// * `comments` - Comments value to save.
    fn write_output(writer: &mut (impl Write + Seek), value: &IndexValue, match_flag: MatchFlag, time_milis: u64, comments: &str) -> Result<()> {
        if comments.len() > 200 {
            bail!(ParseError::InvalidSize);
        }

        writer.seek(SeekFrom::Start(value.output_pos))?;
        writer.write_all(&[b',', match_flag.into(), b','])?;
        writer.write_all(Self::format_time(time_milis).as_bytes())?;
        writer.write_all(&[b','])?;
        writer.write_all(Self::format_comments(comments).as_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Regenerates the index file based on the input file.
    pub fn index(&mut self) -> Result<()> {
        self.index.index()
    }

    /// Format a track time value to a 20 chars fixed size.
    /// 
    /// # Arguments
    /// 
    /// * `track_time` - Track time value.
    pub fn format_time(track_time: u64) -> String {
        format!("{:0>20}", (track_time as f32) / 1000f32)
    }

    /// Format a comments value to a 200 chars fixed size.
    /// 
    /// # Arguments
    /// 
    /// * `comments` - Comments  value.
    pub fn format_comments(comments: &str) -> String {
        format!("\"{: <200}\"", comments)
    }

    /// Record an output into the output file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index to update.
    /// * `match_flag` - Match flag value to save.
    /// * `track_time` - Tracked time value to save.
    /// * `comments` - Comments value to save.
    pub fn record_output(&self, index: u64, match_flag: MatchFlag, track_time: u64, comments: &str) -> Result<()> {
        // write output match data
        let file = OpenOptions::new()
            .write(true)
            .open(&self.index.output_path)?;
        let mut writer = BufWriter::new(file);

        // write output data
        let mut value = match self.index.get_record_index(index)? {
            Some(v) => v,
            None => bail!(ParseError::InvalidValue)
        };
        Self::write_output(&mut writer, &value, match_flag, track_time, comments)?;

        // update index value
        value.match_flag = match_flag.clone();
        self.index.update_index_value(index, &value)?;

        Ok(())
    }

    /// Search the next unprocessed record if any.
    /// 
    /// # Arguments
    /// 
    /// * `from_index` - Index offset from which start searching.
    pub fn find_to_process(&self, from_index: u64) -> Result<Option<u64>> {
        let (index, _) = match self.index.find_unmatched(from_index)? {
            Some(v) => v,
            None => return Ok(None)
        };
        Ok(Some(index))
    }

    /// Retrive a record input data from a specific index.
    /// 
    /// $ Arguments
    /// 
    /// * `index` - Record index.
    pub fn get_data(&self, index: u64) -> Result<serde_json::Value> {
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
        reader.read_exact(&mut buf)?;
        buf.push(b'\n');
        reader.seek(SeekFrom::Start(value.input_start_pos))?;
        let mut buf_value: Vec<u8> = vec![0u8; (value.input_end_pos - value.input_start_pos) as usize];
        reader.read_exact(&mut buf_value)?;
        // dbg!(String::from_utf8(buf_value.clone()).unwrap());
        buf.append(&mut buf_value);

        // read data
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(buf.as_slice());

        // deserialize CSV string object into a JSON object
        if let Some(result) = reader.deserialize::<serde_json::Map<String, serde_json::Value>>().next() {
            match result {
                Ok(record) => {
                    // return data after the first successful record
                    return Ok(serde_json::Value::Object(record))
                }
                Err(e) => {
                    println!("Couldn't parse record at position {}: {}", value.input_start_pos, e);
                    bail!(ParseError::InvalidFormat)
                }
            }
        }

        Ok(serde_json::Value::Null)
    }

    pub fn export(&self, output_path: PathBuf) -> Result<()> {
        
    }

    /// Build a source index file list from an expanded path list.
    /// 
    /// # Arguments
    /// 
    /// * `expanded_path_list` - Expanded path list to build from.
    fn build_index_source_list(&self, expanded_path_list: Vec<PathBuf>) -> Result<Vec<BufReader<File>>> {
        let base_size = file_size(&self.index.index_path)?;
        let mut source_list: Vec<BufReader<File>> = vec!();
        for path in expanded_path_list {
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);
            println!("Open file \"{}\"", path.to_string_lossy());

            // validate index file size
            reader.seek(SeekFrom::End(0))?;
            let size = reader.stream_position()?;
            if size != base_size {
                bail!(ParseError::Other(format!(
                    "Index file size mismatch on file \"{}\"",
                    path.to_string_lossy()
                )));
            }

            // validate index header match
            let mut header = IndexHeader::new();
            Indexer::header_from_file(&mut reader, &mut header)?;
            if header != self.index.header {
                bail!(ParseError::Other(format!(
                    "Index header mismatch on file \"{}\"",
                    path.to_string_lossy()
                )));
            }

            // add to valid file source list
            source_list.push(reader);
        }
        Ok(source_list)
    }

    /// Join index files into a single one using a >50% rule to decide on match flags.
    /// 
    /// # Arguments
    /// 
    /// * `raw_path_list` - Index file path list to join.
    pub fn join(&self, raw_path_list: &Vec<PathBuf>) -> Result<()> {
        // skip if no indexed records found
        if !self.index.header.indexed || self.index.header.indexed_count < 1 {
            return Ok(());
        }

        // expand paths
        let mut path_list: Vec<PathBuf> = vec!();
        let index_path = match PathBuf::from_str(&self.index.index_path) {
            Ok(v) => v,
            Err(e) => bail!(e)
        };
        let exclusions = [index_path].to_vec();
        for path in raw_path_list {
            Self::expand_index_path(path, &mut path_list, &exclusions)?;
        }

        // open and validate source index files
        let mut source_list = self.build_index_source_list(path_list)?;

        // iterate and join index files
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.index.index_path)?;
        let mut index_reader = BufReader::new(&index_file);
        let mut index_writer = BufWriter::new(&index_file);
        let output_file = OpenOptions::new()
            .write(true)
            .open(&self.index.output_path)?;
        let mut output_writer = BufWriter::new(output_file);
        let match_values = MatchFlag::as_array();
        let total_sources = source_list.len() as f64;
        for index in 0..self.index.header.indexed_count {
            // initialize matches hash
            let mut matches: HashMap<u8, f64> = HashMap::new();
            for k in match_values {
                matches.insert(k.into(), 0f64);
            }

            // get base index value
            let mut index_value = match Indexer::value_from_file(&mut index_reader, true, index)? {
                Some(v) => v,
                None => bail!(ParseError::Other(format!(
                    "couldn't retrieve index record on index {} from base index file",
                    index
                )))
            };

            // iterate source index files and count match flag values
            for reader in source_list.iter_mut() {
                // get and validate source index value
                let value = match Indexer::value_from_file(reader, true, index)? {
                    Some(v) => v,
                    None => bail!(ParseError::Other(format!(
                        "couldn't retrieve index record on index {}",
                        index
                    )))
                };
                if index_value.input_start_pos != value.input_start_pos || index_value.input_end_pos != value.input_end_pos {
                    bail!(ParseError::Other("Source index value doesn't match base value".to_string()));
                }

                // record match flag counter
                let count = match matches.get_mut(&value.match_flag.into()) {
                    Some(v) => v,
                    None => bail!(ParseError::InvalidValue)
                };
                *count += 1f64;
            }

            // calculate match_flag value
            let mut match_flag = MatchFlag::None;
            for k in match_values {
                if *matches.get(&k.into()).unwrap() / total_sources > 0.5 {
                    match_flag = k;
                    break;
                }
            }
            if match_flag == MatchFlag::Skip {
                match_flag = MatchFlag::None
            }
            index_value.match_flag = match_flag;

            // record index and output values
            Self::write_output(
                &mut output_writer,
                &index_value,
                match_flag,
                0,
                ""
            )?;
            Indexer::update_index_file_value(
                &mut index_writer,
                index,
                &index_value
            )?;
        }

        Ok(())
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
pub fn generate_hash(path: &str) -> std::io::Result<[u8; HASH_SIZE]> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha3_256::new();

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
    let hash: [u8; HASH_SIZE] = hasher.finalize().try_into().expect("invalid HASH_SIZE value, adjust to your current hash algorightm");
    Ok(hash)
}

#[cfg(test)]
pub mod test_helper;

#[cfg(test)]
mod tests {
    use super::*;
    use index::header::HASH_SIZE;
    use crate::test_helper::*;
    use tempfile::TempDir;
    use std::io::{Read, BufReader};

    #[test]
    fn file_size_with_file() {
        with_tmpdir(&|dir: &TempDir| -> Result<()> {
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
        with_tmpdir(&|dir: &TempDir| -> Result<()> {
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
        with_tmpdir(&|dir: &TempDir| -> Result<()> {
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            create_file_with_bytes(&path_str, buf)?;
            
            let expected: &[u8] = &[12, 213, 40, 91, 168, 82, 79, 228, 42, 200,
              240, 7, 109, 233, 19, 93, 5, 97, 50, 169, 153, 98, 19, 174, 28,
              15, 20, 32, 201, 8, 65, 139];
            let value = generate_hash(&path_str)?;
            assert_eq!(HASH_SIZE, value.len());
            assert_eq!(expected, value);
            
            Ok(())
        });
    }

    #[test]
    fn fill_file_non_exists() {
        with_tmpdir(&|dir: &TempDir| -> Result<()> {
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            
            // fill file
            match fill_file(&path_str, 20, false) {
                Ok(action) => assert_eq!(FillAction::Created, action),
                Err(e) => bail!(e)
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
        with_tmpdir(&|dir: &TempDir| -> Result<()> {
            // create test file
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: [u8; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            create_file_with_bytes(&path_str, &buf)?;

            // fill file
            match fill_file(&path_str, 15, false) {
                Ok(action) => assert_eq!(FillAction::Fill, action),
                Err(e) => bail!(e)
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
        with_tmpdir(&|dir: &TempDir| -> Result<()> {
            // create test file
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: [u8; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            create_file_with_bytes(&path_str, &buf)?;

            // fill file
            match fill_file(&path_str, 10, false) {
                Ok(action) => assert_eq!(FillAction::Bigger, action),
                Err(e) => bail!(e)
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
        with_tmpdir(&|dir: &TempDir| -> Result<()> {
            // create test file
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: [u8; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            create_file_with_bytes(&path_str, &buf)?;

            // fill file
            match fill_file(&path_str, 15, false) {
                Ok(action) => assert_eq!(FillAction::Skip, action),
                Err(e) => bail!(e)
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
        with_tmpdir(&|dir: &TempDir| -> Result<()> {
            // create test file
            let path = dir.path().join("my_file");
            let path_str = path.to_str().unwrap().to_string();
            let buf: [u8; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            create_file_with_bytes(&path_str, &buf)?;

            // fill file
            match fill_file(&path_str, 10, true) {
                Ok(action) => assert_eq!(FillAction::Truncated, action),
                Err(e) => bail!(e)
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
