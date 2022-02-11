pub mod error;
pub mod helper;
pub mod traits;
pub mod db;

use std::fs::File;
use std::io::{Seek, SeekFrom, Read, BufReader};
use std::path::PathBuf;
use sha3::{Digest, Sha3_256};
use db::indexer::header::HASH_SIZE;
use anyhow::{bail, Result};

const BUF_SIZE: u64 = 4096;

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
