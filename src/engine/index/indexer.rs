use serde::Deserialize;
use std::fs::{File, OpenOptions};
use std::convert::TryFrom;
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use crate::engine::parse_error::ParseError;
use crate::engine::{file_size, fill_file, generate_hash};
use crate::engine::index::IndexStatus;
use super::LoadFrom;
use super::index_header::{HEADER_LINE_SIZE, IndexHeader};
use super::index_value::{VALUE_LINE_SIZE, MatchFlag, IndexValue};

const HEADER_EXTRA_FIELDS: &str = ",match,time,comments";

/// Indexer engine.
#[derive(Debug, Deserialize, PartialEq)]
pub struct Indexer {
    /// Input file path.
    pub input_path: String,

    /// Output file path.
    pub output_path: String,

    /// Index file path.
    pub index_path: String,

    /// Current index header.
    pub header: IndexHeader,

    /// Empty extra fields line.
    empty_extra_fields: Vec<u8>
}

impl Indexer {
    pub fn new(input_path: &str, output_path: &str, index_path: &str) -> Self {
        Self{
            input_path: input_path.to_string(),
            output_path: output_path.to_string(),
            index_path: index_path.to_string(),
            header: IndexHeader::new(),
            empty_extra_fields: format!(
                // ',{match_flag:1},{time:20},"{comments:200}"'
                ",{: >1},{:0>20},\"{: <200}\"",
                "",
                "",
                ""
            ).as_bytes().to_vec()
        }
    }

    /// Initialize index file.
    /// 
    /// # Arguments
    /// 
    /// * `truncate` - If `true` then it truncates de file and initialize it.
    pub fn init_index(&self, truncate: bool) -> std::io::Result<()> {
        fill_file(&self.index_path, HEADER_LINE_SIZE as u64, truncate)?;
        Ok(())
    }

    /// Load index headers.
    pub fn load_headers(&mut self) -> Result<(), ParseError> {
        let file = File::open(&self.index_path)?;
        let mut reader = BufReader::new(file);

        let mut buf = [0u8; HEADER_LINE_SIZE];
        reader.read_exact(&mut buf[..])?;
        self.header.load_from(&buf[..])?;

        Ok(())
    }

    /// Count how many records has been indexed so far.
    pub fn count_indexed(&self) -> Result<u64, ParseError> {
        let size = file_size(&self.index_path)?;

        // get and validate file size
        if HEADER_LINE_SIZE as u64 > size {
            return Err(ParseError::InvalidSize);
        }

        // calculate record count
        let record_count = (size - HEADER_LINE_SIZE as u64) / VALUE_LINE_SIZE as u64;
        Ok(record_count)
    }

    /// Calculate the target record's index data position at the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn calc_record_index_pos(index: u64) -> u64 {
        HEADER_LINE_SIZE as u64 + index * VALUE_LINE_SIZE as u64
    }

    /// Get the record's index data.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn get_record_index(&self, index: u64) -> Result<Option<IndexValue>, ParseError> {
        let index_pos = Self::calc_record_index_pos(index);
        let index_file = File::open(&self.index_path)?;
        let mut reader = BufReader::new(index_file);
        
        // validate record index position
        reader.seek(SeekFrom::End(0))?;
        let size = reader.stream_position()?;
        if size < index_pos {
            if self.header.indexed {
                return Ok(None);
            }
            return Err(ParseError::Unavailable(IndexStatus::Indexing))
        }

        // retrive input pos
        reader.seek(SeekFrom::Start(index_pos))?;
        let mut buf = [0u8; VALUE_LINE_SIZE];
        reader.read_exact(&mut buf)?;
        Ok(Some(IndexValue::try_from(&buf[..])?))
    }

    /// Updates an index value.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Index value index.
    /// * `value` - Index value to save.
    pub fn update_index_value(&self, index: u64, value: &IndexValue) -> Result<(), ParseError> {
        let file = OpenOptions::new()
            .write(true)
            .open(&self.index_path)?;
        let mut writer = BufWriter::new(file);
        let pos = Self::calc_record_index_pos(index);
        writer.seek(SeekFrom::Start(pos))?;
        let buf: Vec<u8> = value.into();
        writer.write_all(buf.as_slice())?;
        writer.flush()?;

        Ok(())
    }

    /// Return the index and index value of the closest non matched record.
    /// 
    /// # Arguments
    /// 
    /// * `from_index` - Index offset as search starting point.
    pub fn find_unmatched(&self, from_index: u64) -> Result<Option<(u64, IndexValue)>, ParseError> {
        // validate index size
        if self.header.indexed_count < 1 {
            return Ok(None);
        }

        // find index size
        let size = Self::calc_record_index_pos(self.header.indexed_count);

        // seek start point by using the provided offset
        let file = File::open(&self.index_path)?;
        let mut reader = BufReader::new(file);
        let mut pos = HEADER_LINE_SIZE as u64;
        let mut index = from_index;
        pos += VALUE_LINE_SIZE as u64 * index;
        reader.seek(SeekFrom::Start(pos))?;

        // search next unmatched record
        let mut buf = [0u8; VALUE_LINE_SIZE];
        while pos < size {
            reader.read_exact(&mut buf)?;
            if buf[VALUE_LINE_SIZE - 1] < 1u8 {
                return Ok(Some((index, IndexValue::try_from(&buf[..])?)));
            }
            index += 1;
            pos += VALUE_LINE_SIZE as u64;
        }

        Ok(None)
    }

    /// Perform a healthckeck over the index file by reading
    /// the headers and checking the file size.
    pub fn healthcheck(&mut self) -> Result<IndexStatus, ParseError> {
        self.load_headers()?;
        
        // validate headers
        match self.header.hash {
            Some(saved_hash) => {
                let hash = generate_hash(&self.input_path)?;
                if saved_hash != hash {
                    return Ok(IndexStatus::Corrupted);
                }
            },
            None => return Ok(IndexStatus::New)
        }

        // validate incomplete
        if !self.header.indexed {
            // count indexed records to make sure at least 1 record was indexed
            if self.count_indexed()? < 1 {
                // if not a single record was indexed, then treat it as corrupted
                return Ok(IndexStatus::Corrupted);
            }
            return Ok(IndexStatus::Incomplete);
        }

        // validate file size
        let real_size = file_size(&self.index_path)?;
        let size = Self::calc_record_index_pos(self.header.indexed_count);
        if real_size != size {
            return Ok(IndexStatus::Corrupted);
        }

        Ok(IndexStatus::Indexed)
    }

    /// Get the last index position.
    fn last_index_pos(&self) -> u64 {
        Self::calc_record_index_pos(self.header.indexed_count - 1)
    }

    /// Get the latest indexed record.
    fn last_indexed_record(&self) -> Result<Option<IndexValue>, ParseError> {
        if self.header.indexed_count < 1 {
            return Ok(None);
        }
        self.get_record_index(self.header.indexed_count - 1)
    }

    /// Index a new or incomplete index.
    fn index_records(&mut self) -> Result<(), ParseError> {
        let last_index = self.last_indexed_record()?;

        // open files to create index
        let input_file = File::open(&self.input_path)?;
        let input_file_nav = File::open(&self.input_path)?;
        let output_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.output_path)?;
        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.index_path)?;

        // create reader and writer buffers
        let mut input_rdr = BufReader::new(input_file);
        let mut input_rdr_nav = BufReader::new(input_file_nav);
        let mut output_wrt = BufWriter::new(output_file);
        let mut index_wrt = BufWriter::new(index_file);

        // find input file size
        input_rdr_nav.seek(SeekFrom::End(0))?;

        // seek latest record when exists
        let mut is_first = true;
        if let Some(value) = last_index {
            is_first = false;
            input_rdr.seek(SeekFrom::Start(value.input_end_pos + 1))?;
            index_wrt.seek(SeekFrom::Start(self.last_index_pos() + VALUE_LINE_SIZE as u64))?;
            output_wrt.seek(SeekFrom::Start(value.output_pos + self.empty_extra_fields.len() as u64))?;
        }

        // create index headers
        if is_first {
            let header = &self.header;
            let buf_header: Vec<u8> = header.into();
            index_wrt.write_all(buf_header.as_slice())?;
            index_wrt.flush()?;
        }
        
        // index records
        let mut input_csv = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(input_rdr);
        let header_extra_fields_bytes = HEADER_EXTRA_FIELDS.as_bytes();
        let mut iter = input_csv.records();
        let mut values_indexed = 0u64;
        let mut input_start_pos: u64;
        let mut input_end_pos: u64;
        let mut output_pos: u64;
        loop {
            let item = iter.next();
            if item.is_none() {
                break;
            }
            match item.unwrap() {
                Ok(record) => {
                    // calculate input positions
                    input_start_pos = record.position().unwrap().byte();
                    input_end_pos = iter.reader().position().byte();
                    let length: usize = (input_end_pos - input_start_pos) as usize;

                    // read CSV file line and store it on the buffer
                    let mut buf: Vec<u8> = vec![0u8; length];
                    input_rdr_nav.seek(SeekFrom::Start(input_start_pos))?;
                    input_rdr_nav.read_exact(&mut buf)?;

                    // remove new line at the beginning and end of buffer

                    let mut limit = buf.len();
                    let mut start_index = 0;
                    for _ in 0..2 {
                        if limit - start_index + 1 < 1 {
                            break;
                        }
                        if buf[limit-1] == b'\n' || buf[limit-1] == b'\r' {
                            input_end_pos -= 1;
                            limit -= 1;
                        }
                        if limit - start_index + 1 < 1 {
                            break;
                        }
                        if buf[start_index] == b'\n' || buf[start_index] == b'\r' {
                            input_start_pos += 1;
                            start_index += 1;
                        }
                    }

                    // copy input record into output and add extras
                    if !is_first {
                        output_wrt.write_all(&[b'\n'])?;
                    }
                    output_wrt.write_all(&buf[start_index..limit])?;
                    output_pos = output_wrt.stream_position()?;
                    if is_first {
                        // write header extra fields when first row
                        output_wrt.write_all(header_extra_fields_bytes)?;
                    } else {
                        // write value extra fields when non first row
                        values_indexed = 1;
                        output_wrt.write_all(&self.empty_extra_fields)?;
                    }
                },
                Err(e) => return Err(ParseError::CSV(e))
            }

            // skip index write when input headers
            if is_first{
                is_first = false;
                continue;
            }

            // write index value for this record
            let value = IndexValue{
                input_start_pos,
                input_end_pos,
                output_pos,
                match_flag: MatchFlag::None
            };
            //println!("{:?}", value);
            let buf: Vec<u8> = Vec::from(&value);
            index_wrt.write_all(&buf[..])?;
            self.header.indexed_count += values_indexed;
        }

        // write headers
        index_wrt.rewind()?;
        self.header.indexed = true;
        let header = &self.header;
        let buf_header: Vec<u8> = header.into();
        index_wrt.write_all(buf_header.as_slice())?;
        index_wrt.flush()?;

        Ok(())
    }
    
    /// Analyze an input file to track each record position
    /// into an index file.
    pub fn index(&mut self) -> Result<(), ParseError> {
        let mut retry_count = 0;
        let retry_limit = 3;

        // initialize index file when required
        self.init_index(false)?;
        loop {
            // retry a few times to fix corrupted index files
            retry_count += 1;
            if retry_count > retry_limit {
                return Err(ParseError::RetryLimit);
            }

            // perform healthcheck over the index file
            match self.healthcheck()? {
                IndexStatus::Indexed => return Ok(()),
                IndexStatus::New => {
                    // create initial header
                    self.header.hash = Some(generate_hash(&self.input_path)?);
                    break;
                },
                IndexStatus::Incomplete => break,
                IndexStatus::Indexing => return Err(ParseError::Unavailable(IndexStatus::Indexing)),

                // recreate index file and retry healthcheck when corrupted
                IndexStatus::Corrupted => {
                    self.init_index(true)?;
                    continue;
                }
            }
        }

        self.index_records()
    }
}

#[cfg(test)]
pub mod test_helper {
    use super::*;
    use crate::test_helper::*;
    use crate::engine::index::index_header::test_helper::build_header_bytes;
    use crate::engine::index::index_value::test_helper::build_value_bytes;
    use crate::engine::index::index_header::HASH_SIZE;
    use tempfile::TempDir;
    use std::io::{Write, BufWriter};

    /// Returns the fake input content as bytes.
    pub fn fake_input_bytes() -> Vec<u8> {
        "\
            name,size,price,color\n\
            fork,\"1 inch\",12.34,red\n\
            keyboard,medium,23.45,\"black\nwhite\"\n\
            mouse,\"12 cm\",98.76,white\n\
            \"rust book\",500 pages,1,\"orange\"\
        ".as_bytes().to_vec()
    }

    /// Returns the fake input hash value.
    pub fn fake_input_hash() -> [u8; HASH_SIZE] {
        [ 47, 130, 231, 73, 14, 84, 144, 114, 198, 155, 94, 35, 15,
          101, 71, 156, 48, 113, 13, 217, 129, 108, 130, 240, 24, 19,
          159, 141, 205, 59, 71, 227]
    }

    /// Create a fake input file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Input file path.
    pub fn create_fake_input(path: &str) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&fake_input_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Returns the empty extra fields value.
    pub fn build_empty_extra_fields() -> [u8; 226] {
        let mut buf = [0u8; 226];
        buf[0] = 44;
        buf[1] = 32;
        buf[2] = 44;
        buf[23] = 44;
        buf[24] = 34;
        buf[225] = 34;
        for i in 0..20 {
            buf[3+i] = 48;
        }
        for i in 0..200 {
            buf[25+i] = 32;
        }
        buf
    }

    /// Return the fake output content as bytes.
    pub fn fake_output_bytes() -> Vec<u8> {
        let buf = build_empty_extra_fields().to_vec();
        let eef = String::from_utf8(buf).unwrap();
        format!("\
            name,size,price,color,match,time,comments\n\
            fork,\"1 inch\",12.34,red{}\n\
            keyboard,medium,23.45,\"black\nwhite\"{}\n\
            mouse,\"12 cm\",98.76,white{}\n\
            \"rust book\",500 pages,1,\"orange\"{}\
        ", eef, eef, eef, eef).as_bytes().to_vec()
    }

    /// Create a fake output file based on the default fake input file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Output file path.
    pub fn create_fake_output(path: &str) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&fake_output_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Return the fake index content as bytes.
    /// 
    /// # Arguments
    /// 
    /// * `empty` - If `true` then build all records with MatchFlag::None.
    pub fn fake_index_bytes(empty: bool) -> Vec<u8> {
        let mut buf: Vec<u8> = vec!();

        // write header
        append_bytes(&mut buf, &build_header_bytes(true, &fake_input_hash(), true, 4));

        // write values
        append_bytes(&mut buf, &build_value_bytes(22, 45, 65, if empty { 0 } else { b'Y' }));
        append_bytes(&mut buf, &build_value_bytes(46, 81, 327, 0));
        append_bytes(&mut buf, &build_value_bytes(82, 107, 579, if empty { 0 } else { b'N' }));
        append_bytes(&mut buf, &build_value_bytes(108, 140, 838, 0));

        buf
    }

    /// Create a fake index file based on the default fake input file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Index file path.
    /// * `empty` - If `true` then build all records with MatchFlag::None.
    pub fn create_fake_index(path: &str, empty: bool) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(fake_index_bytes(empty).as_slice())?;
        writer.flush()?;

        Ok(())
    }

    /// Execute a function with both a temp directory and a new Indexer.
    /// 
    /// # Arguments
    /// 
    /// * `f` - Function to execute.
    pub fn with_tmpdir_and_indexer(f: &impl Fn(&TempDir, &mut Indexer) -> Result<(), ParseError>) {
        let sub = |dir: &TempDir| -> std::io::Result<()> {
            // generate default file names for files
            let input_path = dir.path().join("i.csv");
            let output_path = dir.path().join("o.csv");
            let index_path = dir.path().join("i.index");

            // create Indexer and execute
            let input_path_str = input_path.to_str().unwrap().to_string();
            let mut indexer = Indexer::new(
                &input_path_str,
                output_path.to_str().unwrap(),
                index_path.to_str().unwrap()
            );

            // execute function
            match f(&dir, &mut indexer) {
                Ok(_) => Ok(()),
                Err(e) => match e {
                    ParseError::IO(eio) => return Err(eio),
                    _ => panic!("{:?}", e)
                }
            }
        };
        with_tmpdir(&sub)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helper::*;
    use crate::test_helper::*;
    use crate::engine::index::index_header::test_helper::{random_hash, build_header_bytes};
    use crate::engine::index::index_value::test_helper::{build_value_bytes};
    use crate::engine::index::index_header::{HEADER_LINE_SIZE, HASH_SIZE};
    use crate::engine::index::index_value::VALUE_LINE_SIZE;
    use crate::engine::index::POSITION_SIZE;
    use tempfile::TempDir;

    #[test]
    fn new() {
        let buf = build_empty_extra_fields();
        assert_eq!(
            Indexer{
                input_path: "my_input.csv".to_string(),
                output_path: "my_output.csv".to_string(),
                index_path: "my_index.index".to_string(),
                header: IndexHeader::new(),
                empty_extra_fields: buf.to_vec()
            },
            Indexer::new("my_input.csv", "my_output.csv", "my_index.index")
        )
    }

    #[test]
    fn init_index_non_trucate() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            let buf: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            create_file_with_bytes(&indexer.index_path, &buf)?;

            // create expected file contents
            let mut expected = [0u8; HEADER_LINE_SIZE];
            for i in 0..buf.len() {
                expected[i] = buf[i];
            }
            for i in buf.len()..HEADER_LINE_SIZE {
                expected[i] = 0;
            }

            indexer.init_index(false)?;

            let index = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(index);
            let mut buf_after: Vec<u8> = vec!();
            reader.read_to_end(&mut buf_after)?;
            assert_eq!(expected, buf_after.as_slice());

            Ok(())
        });
    }

    #[test]
    fn load_headers() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            // build fake index file
            let hash = random_hash();
            let buf_header = build_header_bytes(true, &hash, true, 3554645435937);
            let mut buf = [0u8; HEADER_LINE_SIZE + 20];
            let buf_frag = &mut buf[..HEADER_LINE_SIZE];
            buf_frag.copy_from_slice(&buf_header);
            create_file_with_bytes(&indexer.index_path, &buf)?;

            // create expected file contents
            let expected = IndexHeader{
                indexed: true,
                hash: Some(hash),
                indexed_count: 3554645435937
            };

            indexer.load_headers()?;

            assert_eq!(expected, indexer.header);

            Ok(())
        });
    }

    #[test]
    fn count_indexed() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            create_fake_index(&indexer.index_path, false)?;
            assert_eq!(4u64, indexer.count_indexed()?);
            Ok(())
        });
    }

    #[test]
    fn calc_record_index_pos() {
        assert_eq!(92u64, Indexer::calc_record_index_pos(2));
    }

    #[test]
    fn get_record_index() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            create_fake_input(&indexer.input_path)?;
            create_fake_index(&indexer.index_path, false)?;

            // first line
            let expected = IndexValue{
                input_start_pos: 22,
                input_end_pos: 45,
                output_pos: 65,
                match_flag: MatchFlag::Yes
            };
            match indexer.get_record_index(0)? {
                Some(v) => assert_eq!(expected, v),
                None => assert!(false, "should have return an IndexValue")
            }

            // second line
            let expected = IndexValue{
                input_start_pos: 46,
                input_end_pos: 81,
                output_pos: 327,
                match_flag: MatchFlag::None
            };
            match indexer.get_record_index(1)? {
                Some(v) => assert_eq!(expected, v),
                None => assert!(false, "should have return an IndexValue")
            }

            // third line
            let expected = IndexValue{
                input_start_pos: 82,
                input_end_pos: 107,
                output_pos: 579,
                match_flag: MatchFlag::No
            };
            match indexer.get_record_index(2)? {
                Some(v) => assert_eq!(expected, v),
                None => assert!(false, "should have return an IndexValue")
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_new_index() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            create_file_with_bytes(&indexer.index_path, &[0u8; HEADER_LINE_SIZE])?;
            assert_eq!(IndexStatus::New, indexer.healthcheck()?);
            Ok(())
        });
    }

    #[test]
    fn healthcheck_hash_mismatch() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            let mut buf = [0u8; HEADER_LINE_SIZE];

            // set valid_hash flag as true
            buf[9] = 1u8;

            // force hash bytes to be invalid
            buf[10] = 3u8;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_incomplete_corrupted() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            let mut buf = [0u8; HEADER_LINE_SIZE+5];

            // set indexed flag as false
            buf[0] = 0u8;

            // set valid_hash flag as true
            buf[9] = 1u8;

            // set fake input file hash value
            let buf_hash = &mut buf[10..10+HASH_SIZE];
            buf_hash.copy_from_slice(fake_input_hash().as_slice());

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_incomplete_valid() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            let mut buf = [0u8; HEADER_LINE_SIZE+VALUE_LINE_SIZE];

            // set indexed flag as false
            buf[0] = 0u8;

            // set valid_hash flag as true
            buf[9] = 1u8;

            // set fake input file hash value
            let buf_hash = &mut buf[10..10+HASH_SIZE];
            buf_hash.copy_from_slice(fake_input_hash().as_slice());

            // set fake index value
            let buf_value = &mut buf[10+HASH_SIZE..10+HASH_SIZE+VALUE_LINE_SIZE];
            buf_value.copy_from_slice(&build_value_bytes(10, 20, 21, b'Y'));

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Incomplete, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_indexed_corrupted() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            let mut buf = [0u8; HEADER_LINE_SIZE];

            // set indexed flag as true
            buf[0] = 1u8;

            // force indexed_count to be invalid
            let buf_indexed_count = &mut buf[1..1+POSITION_SIZE];
            buf_indexed_count.copy_from_slice(&10000u64.to_be_bytes());

            // set valid_hash flag as true
            buf[9] = 1u8;

            // set fake input file hash value
            let buf_hash = &mut buf[10..10+HASH_SIZE];
            buf_hash.copy_from_slice(fake_input_hash().as_slice());

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_indexed_valid() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            create_fake_index(&indexer.index_path, false)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(IndexStatus::Indexed, indexer.healthcheck()?);
            Ok(())
        });
    }

    #[test]
    fn last_indexed_record() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            create_fake_index(&indexer.index_path, false)?;
            let expected = Some(IndexValue{
                input_start_pos: 108,
                input_end_pos: 140,
                output_pos: 838,
                match_flag: MatchFlag::None
            });

            indexer.header.indexed_count = 4;
            assert_eq!(expected, indexer.last_indexed_record()?);
            
            Ok(())
        });
    }

    #[test]
    fn last_indexed_record_with_zero_indexed() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            create_fake_index(&indexer.index_path, false)?;
            indexer.header.indexed_count = 0;
            assert_eq!(None, indexer.last_indexed_record()?);
            
            Ok(())
        });
    }

    #[test]
    fn index_records() {
        with_tmpdir_and_indexer(&|_dir: &TempDir, indexer: &mut Indexer| -> Result<(), ParseError> {
            create_fake_input(&indexer.input_path)?;

            // index records
            indexer.index()?;

            // validate index bytes
            let expected = fake_index_bytes(true);
            let file = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(file);
            let mut buf: Vec<u8> = vec!();
            reader.read_to_end(&mut buf)?;
            assert_eq!(expected, buf);
            
            // validate output bytes
            let expected = fake_output_bytes();
            let file = File::open(&indexer.output_path)?;
            let mut reader = BufReader::new(file);
            let mut buf: Vec<u8> = vec!();
            reader.read_to_end(&mut buf)?;
            assert_eq!(expected, buf);
            
            Ok(())
        });
    }
}