use anyhow::Result as AnyResult;
use tempfile::{TempDir, tempdir};
use std::fs::OpenOptions;
use std::io::{Write, BufWriter, Result as IoResult, Error as IoError, ErrorKind};
use std::path::PathBuf;

/// Create a file with the buffer as content.
/// 
/// # Arguments
/// 
/// * `path` - File path.
/// * `buf` - File content.
pub fn create_file_with_bytes(path: &PathBuf, buf: &[u8]) -> IoResult<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(buf)?;
    writer.flush()?;
    Ok(())
}

/// Execute a function with a temp directory.
/// 
/// # Arguments
/// 
/// * `f` - Function to execute.
pub fn with_tmpdir(f: &dyn Fn(&TempDir) -> AnyResult<()>) {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_str().unwrap().to_string();

    // execute code
    let res = f(&dir);

    if let Err(e) = dir.close() {
        panic!("Error: couldn't delete \"{}\": {:?}", dir_path, e);
    }
    if let Err(e) = res {
        panic!("{:?}", e);
    }
}

/// Append a byte slice into a byte vector.
/// 
/// # Arguments
/// 
/// * `buf` - Buffer to append byte slice into.
/// * `data` - Byte slice to append into buf.
pub fn append_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    let mut buf_data = data.to_vec();
    buf.append(&mut buf_data)
}

/// Copy bytes from the source into the target at a specific offset.
/// 
/// # Arguments
/// 
/// * `target` - Byte slice to copy bytes into.
/// * `source` - Byte slice to copy bytes from.
/// * `offset` - Target offset to start copy bytes.
pub fn copy_bytes(target: &mut [u8], source: &[u8], offset: usize) -> IoResult<()> {
    // validate source into target
    if source.len() + offset > target.len() {
        return Err(IoError::new(ErrorKind::UnexpectedEof, "source + offset is bigger than the target slice size"))
    }

    // copy source into target
    let buf = &mut target[offset..offset+source.len()];
    buf.copy_from_slice(source);
    Ok(())
}