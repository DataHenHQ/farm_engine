pub mod error;
pub mod helper;
pub mod traits;
pub mod db;

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
use sha3::{Digest, Sha3_256};
use db::indexer::header::HASH_SIZE;
use anyhow::{bail, Result};

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
/// * `create` - If `true` then file will be created if not exists.
pub fn file_size(path: &PathBuf) -> Result<u64> {
    let path = path.as_path();
    if !path.is_file() {
        bail!("\"{}\" is not a file", path.to_string_lossy());
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
pub fn fill_file(path: &PathBuf, target_size: u64, truncate: bool) -> std::io::Result<FillAction> {
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
pub fn generate_hash(reader: &mut impl Read) -> std::io::Result<[u8; HASH_SIZE]> {
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
    use crate::test_helper::*;

    #[test]
    fn file_size_with_file() {
        with_tmpdir(&|dir| -> Result<()> {
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
        with_tmpdir(&|dir| -> Result<()> {
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
        with_tmpdir(&|dir| -> Result<()> {
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
        with_tmpdir(&|dir| -> Result<()> {
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
        with_tmpdir(&|dir| -> Result<()> {
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
        with_tmpdir(&|dir| -> Result<()> {
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
        with_tmpdir(&|dir| -> Result<()> {
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
        with_tmpdir(&|dir| -> Result<()> {
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
