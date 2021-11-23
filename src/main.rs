#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use] extern crate rocket;

use rocket_contrib::serve::StaticFiles;
use rocket_contrib::templates::Template;
use rocket_contrib::json::Json;
use rocket::State;
use std::collections::HashMap;
use clap::{clap_app, crate_version};
use chrono::prelude::*;
use serde_json::{json, Value};
use utils::*;

mod utils;

#[get("/")]
fn index(_config: State<AppConfig>) -> Template {
    let context: HashMap<String, String> = HashMap::new();
    Template::render("home", &context)
}

#[get("/compare/<raw_pos>")]
fn compare(config: State<AppConfig>, raw_pos: &rocket::http::RawStr) -> Template {
    let mut context = json!({
        "start_time": null,
        "pos": null,
        "next_pos": null,
        "data": null
    });

    // parse position
    let pos: u64 = match raw_pos.url_decode() {
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
    let (data, next_pos) = match parse_line(&config.headers, &config.input, pos) {
        Ok(v) => v,
        Err(e) => {
            println!("{}", e);
            context["pos"] = Value::Number(serde_json::Number::from(pos));
            return Template::render("errors/bad_parse", &context)
        }
    };

    context["start_time"] = Value::Number(serde_json::Number::from(Utc::now().timestamp_nanos()));
    context["pos"] = Value::Number(serde_json::Number::from(pos));
    context["next_pos"] = Value::Number(serde_json::Number::from(next_pos));
    context["data"] = serde_json::to_value(data).unwrap();
    Template::render("qa/compare", &context)
}

#[post("/compare/<raw_pos>/apply", format = "json", data = "<raw_data>")]
fn apply(config: State<AppConfig>, raw_pos: &rocket::http::RawStr, raw_data: Json<ApplyData>) -> &'static str {
    // parse position
    let pos: u64 = match raw_pos.url_decode() {
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
    
    let data = raw_data.into_inner();
    let matched = if data.approved { "Y" } else { "N" };
    let track_time = Utc::now().timestamp_nanos() - data.time;

    if let Err(e) = write_line(&config, format!("{},{}", matched, track_time), pos, true) {
        println!("{}", e);
        return "Err";
    }
    "OK"
}

fn main() {
    // CLI configuration
    let clap = clap_app!(
        mdrend =>
            (version:crate_version!())
            (author: "Datahen Canada Inc.")
            (about: "Easily compare 2 products to approve or reject equality.")
            (@arg input_file: +required "Must provide an input CSV file path")
            (@arg output_file: +required "Must provide an output CSV file path")
    ).get_matches();

    // write output headers
    let config = AppConfig{
        input: clap.value_of("input_file").unwrap().to_string(),
        output: clap.value_of("output_file").unwrap().to_string(),
        headers: String::new()
    };
    if let Err(e) = write_line(&config, "manual_match,time".to_string(), 0, false) {
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
