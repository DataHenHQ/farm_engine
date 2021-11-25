//! # MatchQA
//! 
//! `matchqa` is an interactive compare tool used to compare
//! and match items from a CSV file by creating a web application
//! over localhost for the user to easilly compare items.

#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;

use rocket_contrib::serve::StaticFiles;
use rocket_contrib::templates::Template;
use rocket_contrib::json::Json;
use rocket::State;
use clap::{clap_app, crate_version};
use chrono::prelude::*;
use serde_json::{json, Value};
use utils::*;

mod utils;

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
    context["data"] = serde_json::to_value(data).unwrap();
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

fn main() {
    // CLI configuration
    let clap = clap_app!(
        matchqa =>
            (version:crate_version!())
            (author: "Datahen Canada Inc.")
            (about: "Easily compare 2 products to approve or reject equality.")
            (@arg input_file: +required "Must provide an input CSV file path")
            (@arg output_file: +required "Must provide an output CSV file path")
            (@arg config_file: +required format!(
                "Must provide a JSON config file path, example:\n{}",
                CONFIG_SAMPLE
            ))
    ).get_matches();

    // build app config
    let input = clap.value_of("input_file").unwrap().to_string();
    let (buf, start_pos, next_pos) = match read_line(&input, 0) {
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
    let user_config_path = clap.value_of("config_file").unwrap().to_string();
    let user_config = match UserConfig::from_file(user_config_path) {
        Ok(v) => v,
        Err(e) => {
            println!("Error parsing config file \"{}\": {}", &user_config_path, e);
            return;
        }
    };
    let config = AppConfig{
        input,
        output: clap.value_of("output_file").unwrap().to_string(),
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
        .attach(Template::fairing())
        .manage(config)
        .mount("/public", StaticFiles::from("static"))
        .mount("/", routes![index])
        .mount("/qa", routes![compare, apply])
        .launch();
}
