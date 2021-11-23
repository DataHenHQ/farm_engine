use clap::{clap_app, crate_version};
use chrono::prelude::*;
use serde_json::Value;

#[get("/")]
fn index(_: State<AppConfig>) -> Template {
    let context: HashMap<String, String> = HashMap::new();
    Template::render("home", &context)
}

#[get("/compare/<record>")]
fn compare(config: State<AppConfig>, record: &RawStr) -> Template {
    let context = json!({
        "start_time": null,
        "new_pos": null,
        "data": null
    })

    // parse position
    let pos: u64 = match record.url_decode() {
        Ok(s) => s.parse(),
        Err(e) => return Template::render("errors/bad_record", &context)
    }

    // get data from file
    let (data, new_pos) = match parse_line(&config.headers, &config.input, pos) {
        Ok(v) => v,
        Err(e) => {
            context["pos"] => pos.to_string();
            return Template::render("errors/bad_parse", &context)
        }
    }

    context["start_time"] = Value::Number(Utc::now().timestamp_nanos());
    context["new_pos"] = Value::Number(new_pos);
    context["data"] = serde_json::to_value(data).unwrap();
    Template::render("qa/compare", &context)
}

#[post("/compare/apply", format = "json", data: = "<data>")]
fn apply(config: State<AppConfig>, raw_data: JSON<ApplyData>) -> &str {
    let data = raw_data.into_inner();
    let matched = if "1" == data.approved { "Y" } else { "N" };
    let track_time = data.time;

    write_line(&config, format!("{},{}", matched, ));
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
        input: clap.value_of("input_file"),
        output: clap.value_of("output_file")
    }
    if let Err(e) = write_line(&config, "manual_match,time", 0, false) {
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
