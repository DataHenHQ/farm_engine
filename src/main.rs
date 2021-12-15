//! # MatchQA
//! 
//! `matchqa` is an interactive compare tool used to compare
//! and match items from a CSV file by creating a web application
//! over localhost for the user to easilly compare items.

#[macro_use] extern crate rocket;

mod matchqa;
mod utils;
mod engine;

#[cfg(test)]
pub mod test_helper;

use rocket::State;
use rocket::fs::FileServer;
use rocket::serde::json::Json;
use rocket_dyn_templates::Template;
use rocket_dyn_templates::tera::Error as RcktTmplError;
use clap::{Arg, App as ClapApp, SubCommand, crate_version};
use chrono::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use self::utils::*;
use self::matchqa::{App, CONFIG_SAMPLE};
use self::engine::index::index_value::MatchFlag;
use self::engine::parse_error::ParseError;

//use engine::Engine;

/// Handle homepage GET requests.
/// 
/// # Arguments
/// 
/// * `app` - Global app object.
#[get("/")]
fn index(app: &State<App>) -> Template {
    let mut context = json!({
        "index": null
    });
    context["index"] = match app.engine.find_to_process(0).unwrap() {
        Some(v) => Value::Number(serde_json::Number::from(v)),
        None => return Template::render("finish", &context)
    };
    Template::render("home", &context)
}

/// Handle compare page GET requests.
/// 
/// # Arguments
/// 
/// * `app` - Global app object.
/// * `raw_index` - Raw file position from which search the closest line.
#[get("/compare/<raw_index>")]
fn compare(app: &State<App>, raw_index: &str) -> Template {
    let mut context = json!({
        "start_time": null,
        "index": null,
        "next_index": null,
        "comments_limit": 200,
        "data": null,
        "ui_config": app.user_config.ui
    });

    // parse index value
    let index: u64 = match raw_index.parse() {
        Ok(v) => v,
        Err(e) => {
            println!("{}", e);
            return Template::render("errors/bad_record", &context)
        }
    };
    context["index"] = Value::Number(serde_json::Number::from(index));

    // get data from file
    let data = match app.engine.get_data(index) {
        Ok(v) => v,
        Err(e) => {
            let err_msg = match e {
                ParseError::IO(eio) => eio.to_string(),
                ParseError::CSV(ecsv) => format!("error trying to parse the input file while extracting data: {}", ecsv),
                _ => "an error happen trying to extract the record data".to_string()
            };
            return Template::render("errors/bad_parse", json!({
                "error_msg": err_msg
            }));
        }
    };

    // build context
    let next_index = match app.engine.find_to_process(index+1).unwrap() {
        Some(v) => Value::Number(serde_json::Number::from(v)),
        None => Value::Number(serde_json::Number::from(-1)),
    };
    context["next_index"] = next_index;

    // finish when no more data
    if let serde_json::Value::Null = data {
        return Template::render("qa/no_record", &context);
    }

    // send compare data to 
    context["start_time"] = Value::Number(serde_json::Number::from(Utc::now().timestamp_millis()));
    context["data"] = data;
    Template::render("qa/compare", &context)
}

/// Handle compare match apply API endpoint POST requests.
/// 
/// # Arguments
/// 
/// * `app` - Global app object.
/// * `raw_index` - Raw file position from which search the closest line.
/// * `raw_data` - Post match data as JSON.
#[post("/compare/<raw_index>/apply", format = "json", data = "<raw_data>")]
fn apply(app: &State<App>, raw_index: &str, raw_data: Json<ApplyData>) -> &'static str {
    // parse position
    let index: u64 = match raw_index.parse() {
        Ok(v) => v,
        Err(e) => {
            println!("{}", e);
            return "Err"
        }
    };
    
    // calculate match data and track time
    let data = raw_data.into_inner();
    let match_flag = if data.skip {
        MatchFlag::Skip
    } else if data.approved {
        MatchFlag::Yes
    } else {
        MatchFlag::No
    };
    let track_time = Utc::now().timestamp_millis() - data.time;

    // save output
    let mut buf = data.comments.as_bytes().to_vec();
    if buf.len() > 200 {
        for _ in 0..buf.len()-200 {
            buf.pop();
        }
    }
    let comments = &String::from_utf8(buf).unwrap();
    if let Err(e) = app.engine.record_output(index, match_flag, track_time as u64, comments) {
        match e {
            ParseError::IO(eio) => println!("{}", eio),
            _ => println!("an error happen trying to save the output data")
        }
    }
    "OK"
}

#[get("/timestamp")]
fn timestamp(_app: &State<App>) -> String {
    Utc::now().timestamp_millis().to_string()
}

#[get("/compare/<raw_index>/pause")]
fn pause(_app: &State<App>, raw_index: &str) -> Template {
    let mut context = json!({
        "index": null
    });

    // parse position
    let index: u64 = match raw_index.parse() {
        Ok(v) => v,
        Err(e) => {
            println!("{}", e);
            return Template::render("errors/bad_record", json!({
                "error_msg": e.to_string()
            }))
        }
    };

    context["index"] = Value::Number(serde_json::Number::from(index));
    Template::render("qa/pause", &context)
}

/// Tera filter that displays the difference between 2 texts and adds html tags to it.
/// 
/// # Arguments
/// 
/// * `after` - Text after the changes.
/// * `before` - Text before the changes.
fn filter_diff_single(after: &Value, args: &HashMap<String, Value>) -> Result<Value, RcktTmplError> {
    let after = match after {
        Value::String(s) => s.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "Yes".to_string() } else { "No".to_string() },
        Value::Null => "".to_string(),
        _ => {
            return Err(RcktTmplError::from("This filter doesn't works with objects nor arrays."));
        }
    };
    let before = match &args["before"] {
        Value::String(s) => s.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "Yes".to_string() } else { "No".to_string() },
        Value::Null => "".to_string(),
        _ => {
            return Err(RcktTmplError::from("This filter doesn't works with objects nor arrays."));
        }
    };

    let text = diff_html_single(&before, &after);
    Ok(Value::String(text))
}

/// Handles the CLI behavior.
fn handle_cli() -> Result<App, String> {
    // CLI configuration
    let cli = ClapApp::new("MatchQA")
        .version(crate_version!())
        .author("Datahen Canada Inc.")
        .about("Easily compare 2 products to approve or reject equality.")
        .subcommand(SubCommand::with_name("start")
            .about("Start matchqa web server.")
            .arg(Arg::with_name("input_file")
                .short("i")
                .takes_value(true)
                .value_name("INPUT_FILE")
                .required(true)
                .help("Must provide an input CSV file path"))
            .arg(Arg::with_name("output_file")
                .short("o")
                .takes_value(true)
                .value_name("OUTPUT_FILE")
                .required(true)
                .help("Must provide an output CSV file path"))
            .arg(Arg::with_name("index_file")
                .short("I")
                .takes_value(true)
                .value_name("INDEX_FILE")
                .required(false)
                .help("provide an index file path [<INPUT_FILE>.matchqa.index]"))
            .arg(Arg::with_name("config_file")
                .short("c")
                .takes_value(true)
                .value_name("CONFIG_FILE")
                .required(true)
                .help("Must provide a JSON config file path")))
        .subcommand(SubCommand::with_name("config_sample")
            .about("Print a config file sample."));
    let mut help_msg = Vec::new();
    if let Err(e) = cli.write_help(&mut help_msg) {
        return Err(format!("Error generating help message: {}", e));
    }
    let cli_matches = cli.get_matches();

    // print config sample
    if cli_matches.subcommand_matches("config_sample").is_some() {
        return Err(CONFIG_SAMPLE.to_string());
    }

    // print help
    let cli_start = match cli_matches.subcommand_matches("start") {
        Some(v) => v,
        None => {
            return Err(String::from_utf8(help_msg).unwrap());
        }
    };

    // build app
    let input_path = cli_start.value_of("input_file").unwrap().to_string();
    let output_path = cli_start.value_of("output_file").unwrap().to_string();
    let config_path = cli_start.value_of("config_file").unwrap().to_string();
    let index_path = cli_start.value_of("index_file");
    let app = match App::new(&input_path, &output_path, index_path, &config_path) {
        Ok(v) => v,
        Err(e) => {
            return Err(e);
        }
    };

    Ok(app)
}

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    // use a different function to handle CLI behavior due to a "Future"
    //  issue from rocket:main async behavior
    let mut app = match handle_cli() {
        Ok(v) => v,
        Err(e) => {
            println!("{}", e);
            return Ok(())
        }
    };

    // index input file
    println!("Indexing into {}...", &app.engine.index.index_path);
    if let Err(e) = app.engine.index() {
        match e {
            ParseError::IO(eio) => println!("{}", eio),
            ParseError::CSV(ecsv) => println!("error trying to parse the input file while indexing: {}", ecsv),
            _ => println!("an error happen trying to index the input file")
        }
        return Ok(());
    };
    println!("Finished indexing. Found {} records.", app.engine.index.header.indexed_count);

    // calculate static directory path
    let default_static_path = match env::var("CARGO_MANIFEST_DIR") {
        Ok(s) => Some(s),
        Err(_) => None
    };
    let static_path = match default_static_path {
        Some(s) => match PathBuf::from_str(&s) {
            Ok(mut v) => {
                v.push("static");
                v
            },
            Err(e) => {
                println!("{}", e);
                return Ok(());
            }
        },
        None => match env::current_exe() {
            Ok(mut v) => {
                v.pop();
                v.push("static");
                v
            },
            Err(e) => {
                println!("{}", e);
                return Ok(());
            }
        }
    };
    println!("Watching \"{}\" for static directory...", static_path.to_string_lossy());

    // configure server and routes
    rocket::build()
        .attach(Template::custom(|engines| {
            engines.tera.register_filter("diff_single", filter_diff_single);
        }))
        .manage(app)
        .mount("/public", FileServer::from(static_path))
        .mount("/", routes![index])
        .mount("/qa", routes![compare, apply, pause])
        .ignite().await?
        .launch().await
}
