//! # MatchQA
//! 
//! `matchqa` is an interactive compare tool used to compare
//! and match items from a CSV file by creating a web application
//! over localhost for the user to easilly compare items.

#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;

mod matchqa;
mod utils;
mod engine;

#[cfg(test)]
pub mod test_helper;

use rocket_contrib::serve::StaticFiles;
use rocket_contrib::templates::{Engines as RcktEngines, Template};
use rocket_contrib::templates::tera::Error as RcktError;
use rocket_contrib::json::Json;
use rocket::State;
use clap::{clap_app, crate_version};
use chrono::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
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
fn index(app: State<App>) -> Template {
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
fn compare(app: State<App>, raw_index: &rocket::http::RawStr) -> Template {
    let mut context = json!({
        "start_time": null,
        "index": null,
        "next_index": null,
        "comments_limit": 200,
        "data": null,
        "ui_config": app.user_config.ui
    });

    // parse index value
    let index: u64 = match raw_index.url_decode() {
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
    let data = match app.engine.get_data(index) {
        Ok(v) => v,
        Err(e) => {
            match e {
                ParseError::IO(eio) => println!("{}", eio),
                ParseError::CSV(ecsv) => println!("error trying to parse the input file while extracting data: {}", ecsv),
                _ => println!("an error happen trying to extract the record data")
            }
            return Template::render("errors/bad_parse", &context)
        }
    };

    // finish when no more data
    if let serde_json::Value::Null = data {
        return Template::render("errors/bad_record", &context)
    }

    // build context
    let next_index = match app.engine.find_to_process(index+1).unwrap() {
        Some(v) => Value::Number(serde_json::Number::from(v)),
        None => Value::Number(serde_json::Number::from(-1)),
    };

    context["start_time"] = Value::Number(serde_json::Number::from(Utc::now().timestamp_millis()));
    context["next_index"] = next_index;
    context["index"] = Value::Number(serde_json::Number::from(index));
    context["data"] = data;
    

    // add diff filter
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
fn apply(app: State<App>, raw_index: &rocket::http::RawStr, raw_data: Json<ApplyData>) -> &'static str {
    // parse position
    let index: u64 = match raw_index.url_decode() {
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
    let match_flag = if data.skip { MatchFlag::Skip } else 
        if data.approved { MatchFlag::Yes } else { MatchFlag::No };
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
fn timestamp(_app: State<App>) -> String {
    Utc::now().timestamp_millis().to_string()
}

#[get("/compare/<raw_index>/pause")]
fn pause(_app: State<App>, raw_index: &rocket::http::RawStr) -> Template {
    let mut context = json!({
        "index": null
    });

    // parse position
    let index: u64 = match raw_index.url_decode() {
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

    context["index"] = Value::Number(serde_json::Number::from(index));
    Template::render("qa/pause", &context)
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

    // build app
    let input_path = cli_start.value_of("input_file").unwrap().to_string();
    let output_path = cli_start.value_of("output_file").unwrap().to_string();
    let config_path = cli_start.value_of("config_file").unwrap().to_string();
    let mut app = match App::new(&input_path, &output_path, &config_path) {
        Ok(v) => v,
        Err(e) => {
            println!("{}", e);
            return;
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
        return;
    };
    println!("Done indexing");

    // configure server and routes
    rocket::ignite()
        .attach(Template::custom(|engines: &mut RcktEngines| {
            engines.tera.register_filter("diff_single", filter_diff_single);
        }))
        .manage(app)
        .mount("/public", StaticFiles::from("static"))
        .mount("/", routes![index])
        .mount("/qa", routes![compare, apply, pause])
        .launch();
}
