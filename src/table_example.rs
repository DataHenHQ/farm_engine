// use std::path::PathBuf;

// use dhfarm_engine::db::indexer::value::MatchFlag;
// use dhfarm_engine::db::source::Source;
// use dhfarm_engine::db::indexer::Indexer;
// use dhfarm_engine::db::indexer::header::InputType;
//use dhfarm_engine::db::table::record::Record;
use dhfarm_engine::db::table::record::Value;
use dhfarm_engine::db::table::{Table, Status as TableStatus};
use dhfarm_engine::db::table::record::header::FieldType;
// use serde_json::{Map as JSMap, Value as JSValue};

pub fn main() {
    let table_name = "mi_tabla";
    let table_path = "./table.fmtable".into();
    let mut table = Table::new(table_path, table_name).unwrap();
    match table.healthcheck().unwrap() {
        TableStatus::Good => {},
        TableStatus::New => {
            // crear las columnas
            table.record_header.add("gid", FieldType::Str(300)).unwrap();
            table.record_header.add("url", FieldType::Str(2048)).unwrap();
            table.record_header.add("headers", FieldType::Str(2000)).unwrap();
            table.record_header.add("body", FieldType::Str(500)).unwrap();
            table.record_header.add("response_code", FieldType::I16).unwrap();
            table.record_header.add("page_type", FieldType::Str(200)).unwrap();

            // crear el archivo de la tabla
            table.load_or_create(true, true).unwrap();

            // crear los registros
            let records = [
                {
                    let mut record = table.record_header.new_record().unwrap();
                    record.set("gid", Value::Str("datahen.com-123abc".to_string())).unwrap();
                    record.set("url", Value::Str("https://www.datahen.com/123".to_string())).unwrap();
                    record.set("headers", Value::Str(r#"{"Cookie": "foo1=bar1","User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0"}"#.to_string())).unwrap();
                    record.set("body", Value::Str("hello=world111111".to_string())).unwrap();
                    record.set("response_code", Value::I16(200i16)).unwrap();
                    record.set("page_type", Value::Str("product".to_string())).unwrap();
                    record
                },
                {
                    let mut record = table.record_header.new_record().unwrap();
                    record.set("gid", Value::Str("datahen.com-222abc".to_string())).unwrap();
                    record.set("url", Value::Str("https://www.datahen.com/222".to_string())).unwrap();
                    record.set("headers", Value::Str(r#"{"Cookie": "foo2=bar2","User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0"}"#.to_string())).unwrap();
                    record.set("body", Value::Str("hello=world222222".to_string())).unwrap();
                    record.set("response_code", Value::I16(200i16)).unwrap();
                    record.set("page_type", Value::Str("product".to_string())).unwrap();
                    record
                },
                {
                    let mut record = table.record_header.new_record().unwrap();
                    record.set("gid", Value::Str("datahen.com-333abc".to_string())).unwrap();
                    record.set("url", Value::Str("https://www.datahen.com/333".to_string())).unwrap();
                    record.set("headers", Value::Str(r#"{"Cookie": "foo3=bar3","User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0"}"#.to_string())).unwrap();
                    record.set("body", Value::Str("hello=world33333".to_string())).unwrap();
                    record.set("response_code", Value::I16(200i16)).unwrap();
                    record.set("page_type", Value::Str("product".to_string())).unwrap();
                    record
                },
                {
                    let mut record = table.record_header.new_record().unwrap();
                    record.set("gid", Value::Str("datahen.com-list111abc".to_string())).unwrap();
                    record.set("url", Value::Str("https://www.datahen.com/list?q=111".to_string())).unwrap();
                    record.set("headers", Value::Str(r#"{"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0"}"#.to_string())).unwrap();
                    record.set("body", Value::Str("query=frijoles111".to_string())).unwrap();
                    record.set("response_code", Value::I16(200i16)).unwrap();
                    record.set("page_type", Value::Str("search".to_string())).unwrap();
                    record
                },
                {
                    let mut record = table.record_header.new_record().unwrap();
                    record.set("gid", Value::Str("datahen.com-list222abc".to_string())).unwrap();
                    record.set("url", Value::Str("https://www.datahen.com/list?q=222".to_string())).unwrap();
                    record.set("headers", Value::Str(r#"{"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0"}"#.to_string())).unwrap();
                    record.set("body", Value::Str("query=frijoles222".to_string())).unwrap();
                    record.set("response_code", Value::I16(200i16)).unwrap();
                    record.set("page_type", Value::Str("search".to_string())).unwrap();
                    record
                }
            ];
            for record in records {
                table.save_record(table.header.record_count, &record, true).unwrap();
            }
        },
        status => panic!("FFFFFFF se la pelo: {}", status)
    };

    // Ya ta mi tabla!!!! YAAAAAAAAAAAYYYYY

    // Leer registros de la tabla
    for i in 0..table.header.record_count {
        if i < 1 {
            print!("[")
        } else {
            print!(",")
        }
        print!("{}", serde_json::to_string_pretty(&table.record(i).unwrap().unwrap()).unwrap());
    }
    println!("]");

    // Leer un registro e imprimir un valor
    let record = table.record(0).unwrap().unwrap();
    println!("{}", record.get("gid").unwrap());

    // Actualizar un valor del registro
    let mut record = table.record(1).unwrap().unwrap();
    record.set("response_code", Value::I16(123i16)).unwrap();
    table.save_record(1, &record, true).unwrap();
    let record = table.record(1).unwrap().unwrap();
    println!("{}", record.get("response_code").unwrap());

    // Nuevamente actualizamos el mismo registro solo para comprobar
    let mut record = table.record(1).unwrap().unwrap();
    record.set("response_code", Value::I16(234i16)).unwrap();
    table.save_record(1, &record, true).unwrap();
    let record = table.record(1).unwrap().unwrap();
    println!("{}", record.get("response_code").unwrap());
}
