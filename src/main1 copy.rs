use std::fs::OpenOptions;

use dhfarm_engine::db::dbindex::{Indexer, Status as IndexStatus, value};
 use dhfarm_engine::db::dbindex::value::{StatusFlag, Data as IndexData, Value as IndexValue, Gid};
use dhfarm_engine::db::dbindex::header::InputType;
use dhfarm_engine::db::table::Table;
use dhfarm_engine::db::table::record::Value;
use dhfarm_engine::db::table::record::header::FieldType;
use dhfarm_engine::error::IndexError;

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder, Result, put};
use serde::Deserialize;

#[derive(Deserialize)]
struct Query {
    //#[serde(rename(deserialize = "gid"))]
    gid: String,
}

#[derive(Deserialize)]
struct Page {
    gid: String,
    url: String,
    status: bool,
}

#[post("/query")]
async fn query_api(
    query: web::Json<Query>
) -> Result<String> {
    println!("{}", query.gid.clone());
    // TODO: Add farm engine code here

    let input_path = "./mi_tablacc.csv".into(); //let input_path = "./mi_input.csv".into();
    let index_path = "./mi_index.fmindex".into();
    let mut index = Indexer::new(
        input_path,
        index_path,
        InputType::CSV
    );

    // cargar indice o crear indice nuevo al indexar el input
    if let Err(e) = index.index() {
        match e.downcast::<IndexError<IndexStatus>>() {
            Ok(ex) => match ex {
                IndexError::Unavailable(status) => match status {
                    IndexStatus::Indexing => panic!("{}", IndexError::Unavailable(IndexStatus::Indexing)),
                    IndexStatus::New => {
                        // truncate the file then index againnode_B
                        let file = OpenOptions::new()
                            .create(true)
                            .open(&index.index_path).unwrap();
                        file.set_len(0).unwrap();
                        index.index().unwrap();
                    },
                    IndexStatus::WrongInputFile => panic!("Bad input file!"),
                    _ => panic!("Yo que se?!")
                },
                err => panic!("{}", err)
            },
            Err(ex) => panic!("{}", ex)
        }
    }
    let mut table = match Table::from_file("./table_pages.fmtable".into()) {
        Ok(v) => v,
        Err(err) => return Err(actix_web::error::ErrorInternalServerError(err)),
    };

    let mut indice = match index.searchKey(0,Gid::new(&query.gid.to_string()))
    {
        Some(v) => v,
        None => return Ok("null".to_string()),
    };

    //indice = indice - offset;   ///5500 - 4999

    let record = match table.record(indice)
        {
            Ok(v) => match v {
                Some(v) => v,
                None => return Ok("null".to_string()),
            },
            Err(err) => return Err(actix_web::error::ErrorInternalServerError(err)),
        };
    

    Ok(serde_json::to_string(&record)?)//;
    //Ok("Hello".to_string())
}

fn fn_insert_api (page: web::Json<Page>) -> anyhow::Result<String> {

    let mut table = Table::from_file("./table_pages.fmtable".into())?;
    let mut addrecord= table.record_header.new_record()?;
    let record_count= table.header.record_count;
    addrecord.set("gid", Value::Str(page.gid.clone()))?;
    addrecord.set("url", Value::Str(page.url.clone()))?;
    addrecord.set("status", Value::Bool(page.status))?;
    
    table.save_record(record_count, &addrecord, true)?;

    let input_path = "./mi_tablacc.csv".into(); 
    let index_path = "./mi_index.fmindex".into();
    let mut index = Indexer::new(
        input_path,
        index_path,
        InputType::CSV
    );
    
    // cargar indice o crear indice nuevo al indexar el input
    if let Err(e) = index.index() {
        match e.downcast::<IndexError<IndexStatus>>() {
            Ok(ex) => match ex {
                IndexError::Unavailable(status) => match status {
                    IndexStatus::Indexing => panic!("{}", IndexError::Unavailable(IndexStatus::Indexing)),
                    IndexStatus::New => {
                        // truncate the file then index againnode_B
                        let file = OpenOptions::new()
                            .create(true)
                            .open(&index.index_path).unwrap();
                        file.set_len(0).unwrap();
                        index.index().unwrap();
                    },
                    IndexStatus::WrongInputFile => panic!("Bad input file!"),
                    _ => panic!("Yo que se?!")
                },
                err => panic!("{}", err)
            },
            Err(ex) => panic!("{}", ex)
        }
    }



//for i in 2..14{//index.header.indexed_count{
    //let index_nuevo= i;
    let gid = Gid::new(&page.gid);

    println!("<<<---------------- Id {} gid: {} ---------------------->>>",record_count,gid.get());

    index.insertNewIndex(1,gid,0,record_count);

    index.printIndex();

//}
Ok("Ok".to_string())
}

#[post("/insert")]
async fn insert_api( page: web::Json<Page>) -> Result<String> {

    match fn_insert_api(page) {
        Ok(v) => Ok(v),
        Err(err) => Err(actix_web::error::ErrorInternalServerError(err)),
    }

}

fn fn_index_api () -> anyhow::Result<String>{

    let mut table = Table::new("./table_pages.fmtable".into(), "Pages")?;
    table.record_header.add("gid", FieldType::Str(38))?;
    table.record_header.add("url", FieldType::Str(30))?;
    table.record_header.add("status", FieldType::Bool)?;
    table.load_or_create(false, false)?;
table.healthcheck()?;

    table.save_headers()?;


    let input_path = "./mi_tablacc.csv".into(); 
    let index_path = "./mi_index.fmindex".into();
    let mut index = Indexer::new(
        input_path,
        index_path,
        InputType::CSV
    );
    
    // cargar indice o crear indice nuevo al indexar el input
    if let Err(e) = index.index() {
        match e.downcast::<IndexError<IndexStatus>>() {
            Ok(ex) => match ex {
                IndexError::Unavailable(status) => match status {
                    IndexStatus::Indexing => panic!("{}", IndexError::Unavailable(IndexStatus::Indexing)),
                    IndexStatus::New => {
                        // truncate the file then index againnode_B
                        let file = OpenOptions::new()
                            .create(true)
                            .open(&index.index_path).unwrap();
                        file.set_len(0).unwrap();
                        index.index().unwrap();
                    },
                    IndexStatus::WrongInputFile => panic!("Bad input file!"),
                    _ => panic!("Yo que se?!")
                },
                err => panic!("{}", err)
            },
            Err(ex) => panic!("{}", ex)
        }
    }

   //index.new_index_writer(create);

//   let mut table = Table::from_file("./table_pages.fmtable".into())?;
   let mut addrecord= table.record_header.new_record()?;
   let record_count= table.header.record_count;
   addrecord.set("gid", "0".into())?;
   addrecord.set("url", "hjiahscuida.com".into())?;
   addrecord.set("status", Value::Bool(false))?;
   
   table.save_record(record_count, &addrecord, true)?;


    // Index id 0
    let mut value = index.value(0).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 0;
    value.data.parent = 0;
    value.data.left_node = 1;
    value.data.right_node = 0;
    value.data.gid = Gid::new("0");
    value.data.height = 1;
    index.save_value(0, &value).unwrap();


println!("el cero");
    index.insertNewIndex(1,Gid::new("0"),0,0);



    Ok("Ok".to_string())
}

#[put("/index")]
async fn index_api() -> Result<String> {
    match fn_index_api() {
        Ok(v) => Ok(v),
        Err(err) => Err(actix_web::error::ErrorInternalServerError(err)),
    }
}




#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new()
        .service(query_api)
        .service(index_api)
        .service(insert_api))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
