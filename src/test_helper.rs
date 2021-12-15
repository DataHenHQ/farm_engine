use tempfile::{TempDir, tempdir};
use std::fs::{OpenOptions};
use std::io::{Write, BufWriter};

/// Create a file with the buffer as content.
/// 
/// # Arguments
/// 
/// * `path` - File path.
/// * `buf` - File content.
pub fn create_file_with_bytes(path: &str, buf: &[u8]) -> std::io::Result<()> {
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
pub fn with_tmpdir(f: &dyn Fn(&TempDir) -> std::io::Result<()>) {
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
pub fn append_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    let mut buf_data = data.to_vec();
    buf.append(&mut buf_data)
}