pub mod error;
pub mod index;

use std::fs::{File, OpenOptions};
use std::io::{Read, Write, BufReader, BufWriter};
use index::indexer::Indexer;

const BUF_SIZE: u64 = 4096;

/// Fill function action.
#[derive(Debug)]
pub enum FillAction {
    Created,
    Fill,
    Bigger,
    Skip
}

/// Engine to manage index and navigation.
#[derive(Debug)]
pub struct Engine<'engine> {
    /// Input file path.
    input_path: String,
    /// Output file path.
    output_path: String,
    /// Indexer engine object.
    index: Indexer<'engine>
}

impl<'engine> Engine<'engine> {
    /// Creates a new engine and default index path as
    /// `<input_path>.index` if not provided.
    /// 
    /// # Arguments
    /// 
    /// * `input_path` - Input file path.
    /// * `output_path` - Output file path,
    /// * `index_path` - Index path (Optional).
    pub fn new(input_path: &str, output_path: &str, index_path: Option<&str>) -> Self {
        let index_path = match index_path {
            Some(s) => s.to_string(),
            None => format!("{}.index", input_path)
        };

        let input_path = input_path.to_string();
        Self{
            input_path,
            output_path: output_path.to_string(),
            index: Indexer{
                input_path: &input_path,
                index_path
            }
        }
    }

    /// Regenerates the index file based on the input file.
    pub fn index(&self) -> std::io::Result<bool> {
        unimplemented!()
    }
}

/// Get a file size.
/// 
/// # Arguments
/// 
/// * `path` - File path.
/// * `create` - If `true` then file will be created if not exists.
pub fn file_size(path: &str, create: bool) -> std::io::Result<u64> {
    let file = if create {
        OpenOptions::new().create(true).open(path)?
    } else {
        File::open(path)?
    };
    file.sync_all()?;
    Ok(file.metadata()?.len())
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
    if !truncate {
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
    }
    let remaining = (target_size - size) as usize;
    if remaining > 0 {
        wrt.write_all(&buf[..remaining])?;
    }

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