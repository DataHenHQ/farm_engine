pub mod error;
pub mod traits;
mod data;
pub mod db;

pub use data::{Data, Segment, SegmentMeta};
pub use uuid;

use path_absolutize::Absolutize;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter, Result as IoResult, Error as IoError};
use std::path::PathBuf;
use sha3::{Digest, Sha3_256};
use db::index::raw_match::header::HASH_SIZE;

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

/// Get a file size.
/// 
/// # Arguments
/// 
/// * `path` - File path.
pub fn file_size(path: &PathBuf) -> IoResult<u64> {
    let path = path.as_path();
    if !path.is_file() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("\"{}\" is not a file", path.to_string_lossy())));
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
/// * `writer` - File writer to fill.
/// * `target_size` - Target file size in bytes.
pub fn fill_writer(writer: &mut (impl Write + Seek), target_size: u64) -> IoResult<FillAction> {
    let mut action = FillAction::Fill;

    // get file size
    writer.flush()?;
    writer.seek(SeekFrom::End(0))?;
    let mut size = writer.stream_position()?;

    // change default action to created when new file
    if size < 1 {
        action = FillAction::Created;
    }

    // validate file current size vs target size
    if target_size < size {
        // file is bigger, return true
        return Ok(FillAction::Bigger);
    }
    if target_size == size {
        return Ok(FillAction::Skip);
    }

    // fill file with zeros until target size is match
    let buf_size = 4096u64;
    let buf = [0u8; 4096];
    while size + buf_size < target_size {
        writer.write_all(&buf)?;
        size += buf_size;
        writer.flush()?;
    }
    let remaining = (target_size - size) as usize;
    if remaining > 0 {
        writer.write_all(&buf[..remaining])?;
    }
    writer.flush()?;

    Ok(action)
}

/// Fill a file with zero byte until the target size or ignore if
/// bigger. Return true if file is bigger.
/// 
/// # Arguments
/// 
/// * `path` - File path to fill.
/// * `target_size` - Target file size in bytes.
/// * `truncate` - If `true` then it truncates de file and fill it.
pub fn fill_file(path: &PathBuf, target_size: u64, truncate: bool) -> IoResult<FillAction> {
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
    file.sync_all()?;
    let mut writer = BufWriter::new(file);
    match fill_writer(&mut writer, target_size)? {
        FillAction::Created => if truncate {
            Ok(FillAction::Truncated)
        } else {
            Ok(FillAction::Created)
        },
        v => Ok(v)
    }
}

/// Generates a hash value from a file contents.
/// 
/// # Arguments
/// 
/// * `reader` - File reader to generate hash from.
pub fn generate_hash(reader: &mut impl Read) -> IoResult<[u8; HASH_SIZE]> {
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

/// Validate a file path extension.
/// 
/// # Arguments
/// 
/// * `path` - Path to validate.
/// * `extension_regex` - Extension regex to validate.
pub fn validate_file_extension(path: &PathBuf, extension_regex: &Regex) -> bool {
    let file_name = match path.file_name() {
        Some(v) => match v.to_str() {
            Some(s) => s,
            None => return false
        },
        None => return false
    };
    extension_regex.is_match(file_name)
}

/// Scans a path and add any found matching into the path list.
/// 
/// # Arguments
/// 
/// * `source_path` - Source path to expand.
/// * `path_list` - Path list to add the found paths into.
/// * `raw_excludes` - Excluded paths to skip.
/// * `regex` - Regex to validate the file extension.
pub fn scan_path(source_path: &PathBuf, path_list: &mut Vec<PathBuf>, raw_excludes: &Vec<PathBuf>, regex: &Regex) -> Result<(), IoError> {
    // canonalize the excluded paths
    let mut excludes: Vec<PathBuf> = vec!();
    for raw_exclude in raw_excludes {
        excludes.push(raw_exclude.absolutize()?.to_path_buf());
    }

    // resolve symlink and relative paths
    let path = source_path.absolutize()?.to_path_buf();

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
        if !validate_file_extension(&file_path, &regex) {
            continue;
        }

        // add index file
        path_list.push(file_path);
    }

    Ok(())
}

#[cfg(test)]
pub mod test_helper;

#[cfg(test)]
mod tests {
    use anyhow::Result as AnyResult;
    use tempfile::TempDir;

    use super::*;
    use crate::test_helper::*;

    #[test]
    fn file_size_with_file() {
        with_tmpdir(&|dir: &TempDir| -> AnyResult<()> {
            // test one
            let path = dir.path().join("my_file_a");
            create_file_with_bytes(&path, &[0u8; 34])?;
            assert_eq!(34, file_size(&path)?);
            drop(path);

            // test two
            let path = dir.path().join("my_file_b");
            create_file_with_bytes(&path, &[0u8; 24])?;
            assert_eq!(24, file_size(&path)?);
            drop(path);

            Ok(())
        });
    }

    #[test]
    fn file_size_without_file() {
        with_tmpdir(&|dir: &TempDir| -> AnyResult<()> {
            let path = dir.path().join("my_file_non_exists");
            let expected = format!("\"{}\" is not a file", path.to_string_lossy());
            assert_eq!(false, path.exists());
            match file_size(&path) {
                Ok(v) => assert!(false, "expected an error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }
            Ok(())
        });
    }

    #[test]
    fn fill_file_non_exists() {
        with_tmpdir(&|dir: &TempDir| -> AnyResult<()> {
            let path = dir.path().join("my_file");
            
            // fill file
            match fill_file(&path, 20, false) {
                Ok(action) => assert_eq!(FillAction::Created, action),
                Err(e) => assert!(false, "expected FillAction::Created but got error: {:?}", e)
            }

            // read file after fill
            let file = File::open(&path)?;
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
        with_tmpdir(&|dir: &TempDir| -> AnyResult<()> {
            // create test file
            let path = dir.path().join("my_file");
            let buf: [u8; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            create_file_with_bytes(&path, &buf)?;

            // fill file
            match fill_file(&path, 15, false) {
                Ok(action) => assert_eq!(FillAction::Fill, action),
                Err(e) => assert!(false, "expected FillAction::Fill but got error: {:?}", e)
            }

            // read file after fill
            let file = File::open(&path)?;
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
        with_tmpdir(&|dir: &TempDir| -> AnyResult<()> {
            // create test file
            let path = dir.path().join("my_file");
            let buf: [u8; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            create_file_with_bytes(&path, &buf)?;

            // fill file
            match fill_file(&path, 10, false) {
                Ok(action) => assert_eq!(FillAction::Bigger, action),
                Err(e) => assert!(false, "expected FillAction::Bigger but got error: {:?}", e)
            }

            // read file afer fill
            let file = File::open(&path)?;
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
        with_tmpdir(&|dir: &TempDir| -> AnyResult<()> {
            // create test file
            let path = dir.path().join("my_file");
            let buf: [u8; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            create_file_with_bytes(&path, &buf)?;

            // fill file
            match fill_file(&path, 15, false) {
                Ok(action) => assert_eq!(FillAction::Skip, action),
                Err(e) => assert!(false, "expected FillAction::Skip but got error: {:?}", e)
            }

            // read file after fill
            let file = File::open(&path)?;
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
        with_tmpdir(&|dir: &TempDir| -> AnyResult<()> {
            // create test file
            let path = dir.path().join("my_file");
            let buf: [u8; 15] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            create_file_with_bytes(&path, &buf)?;

            // fill file
            match fill_file(&path, 10, true) {
                Ok(action) => assert_eq!(FillAction::Truncated, action),
                Err(e) => assert!(false, "expected FillAction::Truncated but got error: {:?}", e)
            }

            // read file after fill
            let file = File::open(&path)?;
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

    #[test]
    fn gen_hash() {
        with_tmpdir(&|dir: &TempDir| -> AnyResult<()> {
            let path = dir.path().join("my_file");
            let buf: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            create_file_with_bytes(&path, buf)?;
            
            let expected: &[u8] = &[12, 213, 40, 91, 168, 82, 79, 228, 42, 200,
              240, 7, 109, 233, 19, 93, 5, 97, 50, 169, 153, 98, 19, 174, 28,
              15, 20, 32, 201, 8, 65, 139];
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);
            let value = generate_hash(&mut reader)?;
            assert_eq!(HASH_SIZE, value.len());
            assert_eq!(expected, value);
            
            Ok(())
        });
    }


}
