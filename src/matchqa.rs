use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write, BufRead, BufReader, BufWriter};
use crate::utils::{read_line, read_csv_line};

/// User config sample file.
pub const CONFIG_SAMPLE: &str = r#"
{
  "ui": {
    "image_url": {
      "a": "dh_image_url",
      "b": "match_image_url"
    },
    "product_name": {
      "a": "dh_product_name",
      "b": "match_product_name"
    },
    "data": [
      {
        "label": "Size",
        "a": "dh_size_std",
        "b": "match_size_std"
      }, {
        "label": "Size Unit",
        "a": "dh_size_unit",
        "b": "match_size_unit"
      }, {
        "label": "Price",
        "a": "dh_price",
        "b": "match_price"
      }, {
        "label": "GID",
        "a": "dh_global_id",
        "b": null,
        "show_more": true,
        "no_diff": true
      }
    ]
  }
}
"#;

/// UI data value used to describe an extra data compare field.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiDataValue {
    /// Label to be display on the compare UI.
    pub label: Option<String>,
    /// Product A field header key.
    pub a: Option<String>,
    /// Product B field header key.
    pub b: Option<String>,
    /// Show more flag, will be hidden when true until the user
    /// enable `show more` feature.
    pub show_more: Option<bool>,
    /// No diff will be executed if `true`.
    pub no_diff: Option<bool>
}

/// UI configuration used to describe the compare view.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiConfig {
    /// Image url compare UI configuration.
    pub image_url: Option<UiDataValue>,
    /// Product name compare UI configuration.
    pub product_name: Option<UiDataValue>,
    /// Extra data compare UI configuration collection.
    pub data: Vec<UiDataValue>
}

/// User configuration build from a JSON file.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserConfig {
    /// UI configuration object.
    pub ui: UiConfig
}

impl UserConfig {
    /// Build a UiConfig object from a JSON file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - JSON file path.
    pub fn from_file(path: &str) -> std::io::Result<UserConfig> {
        // open the file in read-only mode with buffer
        let file = File::open(path)?;
        let reader = BufReader::new(file);
    
        // read the JSON contents of the file into the user config
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}

/// Application.
#[derive(Debug)]
pub struct App {
    /// Input file path.
    pub input: String,
    /// Output file path.
    pub output: String,
    /// CSV input file headers line string.
    pub headers: String,
    /// First data line from the input CSV file.
    pub start_pos: u64,
    /// User configuration object created from the provided JSON
    /// config file.
    pub user_config: UserConfig
}

impl App {
    /// Initialize a new AppConfig object.
    pub fn new(input_path: &str, output_path: &str, config_path: &str) -> Result<Self, String> {
        let user_config = match UserConfig::from_file(&config_path) {
            Ok(v) => v,
            Err(e) => return Err(
                format!("Error parsing config file \"{}\": {}",
                &config_path,
                e
            ))
        };

        let mut config = Self{
            input: input_path.to_string(),
            output: output_path.to_string(),
            headers: "".to_string(),
            start_pos: 0,
            user_config: user_config
        };
        config.extract_headers()?;
        Ok(config)
    }

    /// Extracts the headers from the input file and saves it.
    pub fn extract_headers(&mut self) -> Result<(), String> {
        let (buf, _, start_pos) = match read_line(&self.input, 0) {
            Ok(v) => v,
            Err(e) => return Err(format!(
                "Error reading headers from input file \"{}\": {}",
                &self.input,
                e
            ))
        };
        self.headers = match String::from_utf8(buf) {
            Ok(s) => s.to_string(),
            Err(e) => return Err(
                format!("Error reading headers from input file \"{}\": {}",
                &self.input,
                e
            ))
        };
        self.start_pos = start_pos;

        Ok(())
    }

    /// Write match data to the output file by using the closes line
    /// data from the input file. Return io::Result.
    /// 
    /// # Arguments
    /// 
    /// * `config` - Application configuration containing input, output and headers data.
    /// * `start_pos` - File position from which search the closest line.
    /// * `append` - Append flag to decide whenever append or override the output file.
    pub fn write_output(&self, text: String, start_pos: u64, append: bool) -> io::Result<()> {
        // get data from input file
        let (buf, _) = read_csv_line(&self.input, start_pos)?;
    
        // decide on append or just override, then open file
        let mut output_file = if append {
            OpenOptions::new().create(true).append(true).open(&self.output)?
        } else {
            OpenOptions::new().create(true).write(true).truncate(true).open(&self.output)?
        };
    
        // write new match data to output file
        let text = match text.len() {
            0 => format!("{}{}", String::from_utf8(buf).unwrap(), text),
            _ => format!("{},{}", String::from_utf8(buf).unwrap(), text)
        };
        writeln!(output_file, "{}", text)?;
    
        Ok(())
    }
}