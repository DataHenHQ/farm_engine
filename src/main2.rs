use std::fs::OpenOptions;

// use std::path::PathBuf;
// use dhfarm_engine::db::source::Source;
use dhfarm_engine::db::dbindex::{Indexer, Status as IndexStatus, value};
//use dhfarm_engine::db::dbindex::value::StatusFlag;
 use dhfarm_engine::db::dbindex::value::{StatusFlag, Data as IndexData, Value as IndexValue, Gid};
 //use dhfarm_engine::db::dbindex::value::{StatusFlag, Data as IndexData, Value as IndexValue};
use dhfarm_engine::db::dbindex::header::InputType;
// use dhfarm_engine::db::table::record::Record;
// use dhfarm_engine::db::table::record::Value;
// use dhfarm_engine::db::table::{Table, Status as TableStatus};
// use dhfarm_engine::db::table::record::header::FieldType;
use dhfarm_engine::error::IndexError;
//use serde_json::{Map as JSMap, Value as JSValue};


pub fn main() {
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

   //index.new_index_writer(create);

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

    // Index id 1
    let mut value = index.value(1).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 0;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("222");
    value.data.height = 1;
    index.save_value(1, &value).unwrap();

    // Index id 2
    let mut value = index.value(2).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 0;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("111");
    value.data.height = 1;
    index.save_value(2, &value).unwrap();


    // Index id 3
    let mut value = index.value(3).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 0;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("333");
    value.data.height = 1;
    index.save_value(3, &value).unwrap();

    // Index id 4
    let mut value = index.value(4).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 2;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("110");
    value.data.height = 1;
    index.save_value(4, &value).unwrap();

    // Index id 5
    let mut value = index.value(5).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 14;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("105");
    value.data.height = 1;
    index.save_value(5, &value).unwrap();

    // Index id 6
    let mut value = index.value(6).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 15;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("150");
    value.data.height = 1;
    index.save_value(6, &value).unwrap();


    // Index id 7
    let mut value = index.value(7).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 16;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("140");
    value.data.height = 1;
    index.save_value(7, &value).unwrap();


    // Index id 8
    let mut value = index.value(8).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 17;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("160");
    value.data.height = 1;
    index.save_value(8, &value).unwrap();


    // Index id 9
    let mut value = index.value(9).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 18;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("444");
    value.data.height = 1;
    index.save_value(9, &value).unwrap();

    // Index id 10
    let mut value = index.value(10).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 18;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("223");
    value.data.height = 1;
    index.save_value(10, &value).unwrap();

    // Index id 11
    let mut value = index.value(11).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 18;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("221");
    value.data.height = 1;
    index.save_value(11, &value).unwrap();

    // Index id 12
    let mut value = index.value(12).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 18;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("480");
    value.data.height = 1;
    index.save_value(12, &value).unwrap();

    // Index id 13
    let mut value = index.value(13).unwrap().unwrap();
    value.data.status_flag = StatusFlag::No;
    value.data.spent_time = 18;
    value.data.parent = 0;
    value.data.left_node = 0;
    value.data.right_node = 0;
    value.data.gid = Gid::new("500");
    value.data.height = 1;
    index.save_value(13, &value).unwrap();

    // Original

    index.printIndex();

println!("indexed count: {}",index.header.indexed_count);


for i in 2..14{//index.header.indexed_count{
    let index_nuevo= i;
    let gid = index.value(i).unwrap().unwrap().data.gid;

    println!("<<<---------------- Id {} gid: {} ---------------------->>>",i,gid.get());

    println!("{} {} {} {}",1,gid,0,index_nuevo);
//index.insertNewIndex(1,gid,0,index_nuevo);

    //index.printIndex();

}
index.printIndex();

println!("444 en id: {}",index.searchKey(0,Gid::new("444")).unwrap());
//println!("555 en id: {}",index.searchKey(0,Gid::new("555")).unwrap());

println!("221 en id: {}",index.searchKey(0,Gid::new("221")).unwrap());
print!("regrese");
//index.insertNewIndex(1,155,0,14);

//  index.printIndex();
/*
    // leer el indice y contar cuantas banderas hay de cada uno
    println!("Total: {}", index.header.indexed_count);
    let mut counter = 0;
    let mut yes = 0u64;
    let mut no = 0u64;
    let mut skip = 0u64;
    let mut none = 0u64;
    for i in 0..index.header.indexed_count {
        counter = i;
        if counter % 500 == 0 {
            println!("Processed: {}", counter)
        }
        let value = index.value(i).unwrap();
//println!("ffff {0} - {1} - {2} - {3} - {4} - {5} - {6}",i,index.value(i).unwrap().unwrap().data.spent_time,index.value(i).unwrap().unwrap().data.status_flag,index.value(i).unwrap().unwrap().data.parent,index.value(i).unwrap().unwrap().data.left_node,index.value(i).unwrap().unwrap().data.right_node,index.value(i).unwrap().unwrap().data.gid);
        let index_value = match value {
            Some(index) => index,
            None => break
        };
        match index_value.data.status_flag {
            StatusFlag::Yes => yes += 1,
            StatusFlag::No => no += 1,
            StatusFlag::Skip => skip += 1,
            StatusFlag::None => none += 1
        }
    }
    

    println!("Processed: {}, Yes: {}, No: {}, Skip: {}, None: {}", counter, yes, no, skip, none);


    // mostrar la data del CSV
    let value = index.value(2).unwrap().unwrap();
    let data = index.parse_input(&value).unwrap();
    println!("{}", serde_json::to_string_pretty(&data).unwrap());
 */

} 

