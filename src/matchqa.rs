use serde::{Serialize, Deserialize};
use std::fs::{File};
use std::io::{BufReader};

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
#[derive(Debug, Deserialize)]
pub struct App {
    /// App engine.
    pub engine: crate::engine::Engine,
    /// User configuration object created from the provided JSON
    /// config file.
    pub user_config: UserConfig
}

impl App {
    /// Initialize a new AppConfig object.
    pub fn new(input_path: &str, output_path: &str, index_path: Option<&str>, config_path: &str) -> Result<Self, String> {
        let user_config = match UserConfig::from_file(config_path) {
            Ok(v) => v,
            Err(e) => return Err(
                format!("Error parsing config file \"{}\": {}",
                &config_path,
                e
            ))
        };

        Ok(Self{
            engine: crate::engine::Engine::new(input_path, output_path, index_path),
            user_config
        })
    }
}