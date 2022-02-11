use anyhow::{bail, Result};
// use path_absolutize::*;
use std::collections::HashMap;
use std::ffi::{OsString};
use super::indexer::Indexer;
use super::table::Table;

/// Represents a data source.
pub struct Source {
    /// Indexer.
    pub index: Indexer,
    /// Table.
    pub table: Table
}

impl Source {
    

    // /// Validate an index path extension.
    // /// 
    // /// # Arguments
    // /// 
    // /// * `path` - Path to validate.
    // pub fn validate_index_extension(path: &PathBuf, extension_regex: &Regex) -> bool {
    //     let file_name = match path.file_name() {
    //         Some(v) => match v.to_str() {
    //             Some(s) => s,
    //             None => return false
    //         },
    //         None => return false
    //     };
    //     extension_regex.is_match(file_name)
    // }

    // /// Expands a path and add any index found into the path list.
    // /// 
    // /// # Arguments
    // /// 
    // /// * `raw_path` - Path to expand.
    // /// * `path_list` - Path list to add the found paths into.
    // fn expand_index_path(raw_path: &PathBuf, path_list: &mut Vec<PathBuf>, raw_excludes: &Vec<PathBuf>) -> Result<()> {
    //     // canonalize the excluded paths
    //     let mut excludes: Vec<PathBuf> = vec!();
    //     for raw_exclude in raw_excludes {
    //         excludes.push(raw_exclude.absolutize()?.to_path_buf());
    //     }

    //     // resolve symlink and relative paths
    //     let path = raw_path.absolutize()?.to_path_buf();

    //     // check for exclusion
    //     for exclude in &excludes {
    //         if path.eq(exclude) {
    //             return Ok(())
    //         }
    //     }

    //     // check if single file
    //     if path.is_file() {
    //         // don't validate the file extension for explicit files,
    //         // just add the index file
    //         path_list.push(path);
    //         return Ok(());
    //     }
        
    //     // asume dir since the path is already canonizalized
    //     let extension_regex = Self::index_extension_regex();
    //     'dir_iter: for entry in path.read_dir()? {
    //         let entry = entry?;
    //         let file_path = entry.path();

    //         // check for exclusion
    //         for exclude in &excludes {
    //             if file_path.eq(exclude) {
    //                 continue 'dir_iter;
    //             }
    //         }

    //         // skip subdirectories
    //         if file_path.is_dir() {
    //             continue;
    //         }

    //         // skip non index files
    //         if !Self::validate_index_extension(&file_path, &extension_regex) {
    //             continue;
    //         }

    //         // add index file
    //         path_list.push(file_path);
    //     }

    //     Ok(())
    // }

    // /// Regenerates the index file based on the input file.
    // pub fn index(&mut self) -> Result<()> {
    //     self.indexer.index()
    // }

    // /// Search the next unprocessed record if any.
    // /// 
    // /// # Arguments
    // /// 
    // /// * `from_index` - Index offset from which start searching.
    // pub fn find_to_process(&self, from_index: u64) -> Result<Option<u64>> {
    //     self.index.find_unmatched(from_index)
    // }

    // /// Retrive a record input data from a specific index.
    // /// 
    // /// $ Arguments
    // /// 
    // /// * `index` - Record index.
    // pub fn get_data(&self, index: u64) -> Result<serde_json::Value> {
    //     let first_value = match self.index.record(0)? {
    //         Some(v) => v,
    //         None => return Ok(serde_json::Value::Null)
    //     };
    //     let value = match self.index.record(index)? {
    //         Some(v) => v,
    //         None => return Ok(serde_json::Value::Null)
    //     };

    //     // build a fake CSV string
    //     let file = File::open(&self.index.input_path)?;
    //     let mut reader = BufReader::new(file);
    //     let mut buf: Vec<u8> = vec![0u8; first_value.index.input_start_pos as usize];
    //     reader.read_exact(&mut buf)?;
    //     buf.push(b'\n');
    //     reader.seek(SeekFrom::Start(value.index.input_start_pos))?;
    //     let mut buf_value: Vec<u8> = vec![0u8; (value.index.input_end_pos - value.index.input_start_pos) as usize];
    //     reader.read_exact(&mut buf_value)?;
    //     // dbg!(String::from_utf8(buf_value.clone()).unwrap());
    //     buf.append(&mut buf_value);

    //     // read data
    //     let mut reader = csv::ReaderBuilder::new()
    //         .has_headers(true)
    //         .flexible(true)
    //         .from_reader(buf.as_slice());

    //     // deserialize CSV string object into a JSON object
    //     if let Some(result) = reader.deserialize::<serde_json::Map<String, serde_json::Value>>().next() {
    //         match result {
    //             Ok(record) => {
    //                 // return data after the first successful record
    //                 return Ok(serde_json::Value::Object(record))
    //             }
    //             Err(e) => {
    //                 println!("Couldn't parse record at position {}: {}", value.index.input_start_pos, e);
    //                 bail!(ParseError::InvalidFormat)
    //             }
    //         }
    //     }

    //     Ok(serde_json::Value::Null)
    // }

    // /// Build a source index file list from an expanded path list.
    // /// 
    // /// # Arguments
    // /// 
    // /// * `expanded_path_list` - Expanded path list to build from.
    // fn build_index_source_list(&self, expanded_path_list: Vec<PathBuf>) -> Result<Vec<BufReader<File>>> {
    //     let base_size = file_size(&self.index.index_path)?;
    //     let mut source_list: Vec<BufReader<File>> = vec!();
    //     for path in expanded_path_list {
    //         let file = File::open(&path)?;
    //         let mut reader = BufReader::new(file);
    //         println!("Open file \"{}\"", path.to_string_lossy());

    //         // validate index file size
    //         reader.seek(SeekFrom::End(0))?;
    //         let size = reader.stream_position()?;
    //         if size != base_size {
    //             bail!(ParseError::Other(format!(
    //                 "Index file size mismatch on file \"{}\"",
    //                 path.to_string_lossy()
    //             )));
    //         }

    //         // validate index header match
    //         let index_header = IndexHeader::read_from(&mut reader)?;
    //         let record_header = RecordHeader::read_from(&mut reader)?;
    //         if index_header != self.index.index_header || record_header != self.index.record_header {
    //             bail!(ParseError::Other(format!(
    //                 "Index header mismatch on file \"{}\"",
    //                 path.to_string_lossy()
    //             )));
    //         }

    //         // add to valid file source list
    //         source_list.push(reader);
    //     }
    //     Ok(source_list)
    // }

    // /// Join index files into a single one using a >50% rule to decide on match flags.
    // /// 
    // /// # Arguments
    // /// 
    // /// * `raw_path_list` - Index file path list to join.
    // pub fn join(&self, raw_path_list: &Vec<PathBuf>) -> Result<()> {
    //     // skip if no indexed records found
    //     if !self.index.index_header.indexed || self.index.index_header.indexed_count < 1 {
    //         return Ok(());
    //     }

    //     // expand paths
    //     let mut path_list: Vec<PathBuf> = vec!();
    //     let exclusions = [self.index.index_path].to_vec();
    //     for path in raw_path_list {
    //         Self::expand_index_path(path, &mut path_list, &exclusions)?;
    //     }

    //     // open and validate source index files
    //     let mut source_list = self.build_index_source_list(path_list)?;

    //     // iterate and join index files
    //     let mut target_indexer = Indexer::new(self.index.input_path, self.index.index_path);
    //     let index_file = OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .open(&self.index.index_path)?;
    //     let mut index_reader = BufReader::new(&index_file);
    //     let mut index_writer = BufWriter::new(&index_file);
    //     let match_values = MatchFlag::as_array();
    //     let total_sources = source_list.len() as f64;
    //     for index in 0..self.index.index_header.indexed_count {
    //         // initialize matches hash
    //         let mut matches: HashMap<u8, f64> = HashMap::new();
    //         for k in match_values {
    //             matches.insert(k.into(), 0f64);
    //         }

    //         // get base index value
    //         let mut index_value = match indexer.seek_record_from((&mut index_reader, true, index)? {
    //             Some(v) => v,
    //             None => bail!(ParseError::Other(format!(
    //                 "couldn't retrieve index record on index {} from base index file",
    //                 index
    //             )))
    //         };

    //         // iterate source index files and count match flag values
    //         for reader in source_list.iter_mut() {
    //             // get and validate source index value
    //             let value = match Indexer::value_from_file(reader, true, index)? {
    //                 Some(v) => v,
    //                 None => bail!(ParseError::Other(format!(
    //                     "couldn't retrieve index record on index {}",
    //                     index
    //                 )))
    //             };
    //             if index_value.input_start_pos != value.input_start_pos || index_value.input_end_pos != value.input_end_pos {
    //                 bail!(ParseError::Other("Source index value doesn't match base value".to_string()));
    //             }

    //             // record match flag counter
    //             let count = match matches.get_mut(&value.match_flag.into()) {
    //                 Some(v) => v,
    //                 None => bail!(ParseError::InvalidValue)
    //             };
    //             *count += 1f64;
    //         }

    //         // calculate match_flag value
    //         let mut match_flag = MatchFlag::None;
    //         for k in match_values {
    //             if *matches.get(&k.into()).unwrap() / total_sources > 0.5 {
    //                 match_flag = k;
    //                 break;
    //             }
    //         }
    //         if match_flag == MatchFlag::Skip {
    //             match_flag = MatchFlag::None
    //         }
    //         index_value.match_flag = match_flag;

    //         // record index and output values
    //         Self::write_output(
    //             &mut output_writer,
    //             &index_value,
    //             match_flag,
    //             0,
    //             ""
    //         )?;
    //         Indexer::update_index_file_value(
    //             &mut index_writer,
    //             index,
    //             &index_value
    //         )?;
    //     }

    //     Ok(())
    // }
}