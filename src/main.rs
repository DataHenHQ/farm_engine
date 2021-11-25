//! # MatchQA
//! 
//! `matchqa` is an interactive compare tool used to compare
//! and match items from a CSV file by creating a web application
//! over localhost for the user to easilly compare items.

#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;

mod utils;
mod engine;

use rocket_contrib::serve::StaticFiles;
use rocket_contrib::templates::{Engines as RcktEngines, Template};
use rocket_contrib::templates::tera::Error as RcktError;
use rocket_contrib::json::Json;
use rocket::State;
use clap::{clap_app, crate_version};
use chrono::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use utils::*;
//use engine::Engine;

/// Handle homepage GET requests.
/// 
/// # Arguments
/// 
/// * `config` - Global application configuration.
#[get("/")]
fn index(config: State<AppConfig>) -> Template {
    let context = json!({
        "pos": config.start_pos
    });
    Template::render("home", &context)
}

/// Handle compare page GET requests.
/// 
/// # Arguments
/// 
/// * `config` - Global application configuration.
/// * `raw_start_pos` - Raw file position from which search the closest line.
#[get("/compare/<raw_start_pos>")]
fn compare(config: State<AppConfig>, raw_start_pos: &rocket::http::RawStr) -> Template {
    let mut context = json!({
        "start_time": null,
        "pos": null,
        "next_pos": null,
        "data": null,
        "ui_config": config.user_config.ui
    });

    // parse start position value
    let start_pos: u64 = match raw_start_pos.url_decode() {
        Ok(s) => match s.parse() {
            Ok(v) => v,
            Err(e) => {
                println!("{}", e);
                return Template::render("errors/bad_record", &context)
            }
        },
        Err(e) => {
            println!("{}", e);
            return Template::render("errors/bad_record", &context)
        }
    };

    // get data from file
    let (data, pos, next_pos) = match parse_line(&config.headers, &config.input, start_pos) {
        Ok(v) => v,
        Err(e) => {
            println!("{}", e);
            context["pos"] = Value::Number(serde_json::Number::from(start_pos));
            return Template::render("errors/bad_parse", &context)
        }
    };

    context["start_time"] = Value::Number(serde_json::Number::from(Utc::now().timestamp_nanos()));
    context["pos"] = Value::Number(serde_json::Number::from(pos));
    context["next_pos"] = Value::Number(serde_json::Number::from(next_pos));
    context["data"] = data;
    

    // add diff filter
    Template::render("qa/compare", &context)
}

/// Handle compare match apply API endpoint POST requests.
/// 
/// # Arguments
/// 
/// * `config` - Global application configuration.
/// * `raw_start_pos` - Raw file position from which search the closest line.
/// * `raw_data` - Post match data as JSON.
#[post("/compare/<raw_start_pos>/apply", format = "json", data = "<raw_data>")]
fn apply(config: State<AppConfig>, raw_start_pos: &rocket::http::RawStr, raw_data: Json<ApplyData>) -> &'static str {
    // parse position
    let start_pos: u64 = match raw_start_pos.url_decode() {
        Ok(s) => match s.parse() {
            Ok(v) => v,
            Err(e) => {
                println!("{}", e);
                return "Err"
            }
        },
        Err(e) => {
            println!("{}", e);
            return "Err"
        }
    };
    
    // calculate match data and track time
    let data = raw_data.into_inner();
    let matched = if data.skip { "S" } else 
        if data.approved { "Y" } else { "N" };
    let track_time = (Utc::now().timestamp_nanos() - data.time) / 1000000;

    // join original line contents with match data and write to output file
    let text = format!("{},{}", matched, track_time);
    if let Err(e) = write_line(&config, text, start_pos, true) {
        println!("{}", e);
        return "Err";
    }
    "OK"
}

/// Tera filter that displays the difference between 2 texts and adds html tags to it.
/// 
/// # Arguments
/// 
/// * `after` - Text after the changes.
/// * `before` - Text before the changes.
fn filter_diff_single(after: Value, args: HashMap<String, Value>) -> Result<Value, RcktError> {
    let after = match after {
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if b { "Yes".to_string() } else { "No".to_string() },
        Value::Null => "".to_string(),
        _ => {
            return Err(RcktError::from("This filter doesn't works with objects nor arrays."));
        }
    };
    let before = match &args["before"] {
        Value::String(s) => s.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "Yes".to_string() } else { "No".to_string() },
        Value::Null => "".to_string(),
        _ => {
            return Err(RcktError::from("This filter doesn't works with objects nor arrays."));
        }
    };

    let text = diff_html_single(&before, &after);
    Ok(Value::String(text))
}

fn main() {
    // CLI configuration
    let cli = clap_app!(
        matchqa =>
            (version:crate_version!())
            (author: "Datahen Canada Inc.")
            (about: "Easily compare 2 products to approve or reject equality.")
            (@subcommand start =>
                (about: "Start matchqa web server.")
                (@arg input_file: +required "Must provide an input CSV file path")
                (@arg output_file: +required "Must provide an output CSV file path")
                (@arg config_file: +required "Must provide a JSON config file path")
            )
            (@subcommand config_sample =>
                (about: "Print a config file sample.")
            )
    );
    let mut help_msg = Vec::new();
    if let Err(e) = cli.write_help(&mut help_msg) {
        println!("Error generiting help message: {}", e);
        return;
    }
    let cli_matches = cli.get_matches();

    // print config sample
    if let Some(_) = cli_matches.subcommand_matches("config_sample") {
        println!("{}", CONFIG_SAMPLE);
        return;
    }

    // print help
    let cli_start = match cli_matches.subcommand_matches("start") {
        Some(v) => v,
        None => {
            println!("{}", String::from_utf8(help_msg).unwrap());
            return;
        }
    };

    // build app config
    let input = cli_start.value_of("input_file").unwrap().to_string();
    let (buf, _, start_pos) = match read_line(&input, 0) {
        Ok(v) => v,
        Err(e) => {
            println!("Error reading headers from input file \"{}\": {}", &input, e);
            return;
        }
    };
    let headers = match String::from_utf8(buf) {
        Ok(s) => s.to_string(),
        Err(e) => {
            println!("Error reading headers from input file \"{}\": {}", &input, e);
            return;
        }
    };
    let user_config_path = cli_start.value_of("config_file").unwrap().to_string();
    let user_config = match UserConfig::from_file(&user_config_path) {
        Ok(v) => v,
        Err(e) => {
            println!("Error parsing config file \"{}\": {}", &user_config_path, e);
            return;
        }
    };
    let config = AppConfig{
        input,
        output: cli_start.value_of("output_file").unwrap().to_string(),
        headers: headers,
        start_pos,
        user_config
    };

    // write output headers
    if let Err(e) = write_line(&config, "manual_match,manual_match_time_ms".to_string(), 0, false) {
        println!("Error writing headers on output file \"{}\": {}", config.output, e);
        return;
    }

    // configure server and routes
    rocket::ignite()
        .attach(Template::custom(|engines: &mut RcktEngines| {
            engines.tera.register_filter("diff_single", filter_diff_single);
        }))
        .manage(config)
        .mount("/public", StaticFiles::from("static"))
        .mount("/", routes![index])
        .mount("/qa", routes![compare, apply])
        .launch();
}
