pub mod index;

/// Parsing error.
#[derive(Debug)]
pub enum ParseError {
    InvalidSize,
    InvalidFormat
}

/// Engine to manage index and navigation.
#[derive(Debug)]
pub struct Engine {
    /// Input file path.
    input_path: String,
    /// Output file path.
    output_path: String,
    /// Index file path.
    index_path: String
}

impl Engine {
    /// Creates a new engine and default index path as `<input_path>.index` if not provided.
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

        Self{
            input_path: input_path.to_string(),
            output_path: output_path.to_string(),
            index_path
        }
    }

    /// Regenerates the index file based on the input file.
    pub fn index(&self) -> std::io::Result<bool> {
        unimplemented!()
    }
}

/// Parse a position value from a string and return a position value.
/// 
/// # Arguments
/// 
/// * `s` - String to parse.
pub fn position_from_str(s: &str) -> Result<Option<u64>, ParseError> {
    // validate position size
    if s.len() != 20 {
        return Err(ParseError::InvalidSize);
    }

    // parse position value
    let pos: Option<u64> = match s {
        "                    " => return Ok(None),
        v => match v.parse() {
            Ok(n) => Some(n),
            Err(_) => return Err(ParseError::InvalidFormat)
        }
    };

    Ok(pos)
}