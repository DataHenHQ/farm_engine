pub mod header;
pub mod record;

use anyhow::{bail, Result};
use uuid::Uuid;
use regex::Regex;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
use std::marker::PhantomData;
use crate::{file_size, fill_file};
use crate::error::IndexError;
use crate::traits::{ByteSized, LoadFrom, WriteTo};
use crate::db::field::{Record as Fieldrecord, Value};
use header::Header;
use record::Record;

use self::record::metadata::StatusFlag;

/// Table engine version.
pub const VERSION: u32 = 2;

/// Table file extension.
pub const FILE_EXTENSION: &str = "fmbindex";

/// Table healthcheck status.
#[derive(Debug, PartialEq)]
pub enum Status {
    New,
    Good,
    NoFields,
    Corrupted
}

impl Display for Status{
    fn fmt(&self, f: &mut Formatter) -> FmtResult { 
        write!(f, "{}", match self {
            Self::New => "new",
            Self::Good => "good",
            Self::NoFields => "no fields",
            Self::Corrupted => "corrupted"
        })
    }
}

/// Table engine.
#[derive(Debug, PartialEq, Clone)]
pub struct Table {
    /// Table file path.
    pub path: PathBuf,

    /// Table header.
    pub header: Header
}

impl Table {
    /// Generates a regex expression to validate the index file extension.
    pub fn file_extension_regex() -> Regex {
        let expression = format!(r"(?i)\.{}$", FILE_EXTENSION);
        Regex::new(&expression).unwrap()
    }

    /// Create a new table instance.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Table file path.
    /// * `name` - Table name.
    pub fn new(path: PathBuf, name: &str, uuid: Option<Uuid>) -> Result<Self> {
        Ok(Self{
            path,
            header: Header::new(uuid)
        })
    }

    /// Loads a table from a file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Table file path.
    pub fn from_file(path: PathBuf) -> Result<Self> {
        let mut table = Self::new(path, "", Some(Uuid::from_bytes([0u8; Uuid::BYTES])))?;
        match table.healthcheck() {
            Ok(v) => match v {
                Status::Good => Ok(table),
                vu => bail!(IndexError::Unavailable(vu))
            },
            Err(e) => Err(e)
        }
    }

    /// Returns a table file buffered reader.
    pub fn new_reader(&self) -> Result<BufReader<File>> {
        let file = File::open(&self.path)?;
        Ok(BufReader::new(file))
    }

    /// Returns a table file buffered writer.
    /// 
    /// # Arguments
    /// 
    /// * `create` - Set to `true` when the file should be created.
    pub fn new_writer(&self, create: bool) -> Result<BufWriter<File>> {
        let mut options = OpenOptions::new();
        options.write(true);
        if create {
            options.create(true);
        }
        let file = options.open(&self.path)?;
        Ok(BufWriter::new(file))
    }

    /// Calculate the target record position at the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn calc_record_pos(&self, index: u64) -> u64 {
        let data_size = self.header.fields.record_byte_size() as u64;
        self.header.size_as_bytes() + index * data_size
    }

    /// Get the record's headers.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    pub fn load_headers_from(&mut self, reader: &mut (impl Read + Seek)) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        self.header.load_from(reader)?;
        Ok(())
    }
    
    /// Move to index position and then read the record from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    pub fn seek_record_from(&self, reader: &mut (impl Read + Seek), index: u64) -> Result<Option<Record>> {
        if self.header.fields.len() < 1 {
            let err: IndexError<bool> = IndexError::NoFields;
            bail!(err)
        }

        if self.header.indexed_count > index {
            let pos = self.calc_record_pos(index);
            reader.seek(SeekFrom::Start(pos))?;
            let mut record = Record::new();
            record.load_from(&self.header.fields, reader)?;
            return Ok(Some(record));
        }
        Ok(None)
    }

    /// Read the record from the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn record(&self, index: u64) -> Result<Option<Record>> {
        let mut reader = self.new_reader()?;
        self.seek_record_from(&mut reader, index)
    }

    /// Updates or append a record into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - File writer to save data into.
    /// * `index` - Record index.
    /// * `record` - Record to save.
    /// * `save_headers` - Headers will be saved on append when true.
    pub fn save_record_into(&mut self, writer: &mut (impl Write + Seek), index: u64, record: &Record, save_headers: bool) -> Result<()> {
        // validate table
        if self.header.fields.len() < 1 {
            let err: IndexError<bool> = IndexError::NoFields;
            bail!(err)
        }
        if index > self.header.indexed_count {
            bail!("can't write or append the record, the table file is too small");
        }

        // seek and write record
        let pos = self.calc_record_pos(index);
        writer.seek(SeekFrom::Start(pos))?;
        record.write_to(&self.header.fields, writer)?;
        
        // exit when no append
        if index < self.header.indexed_count {
            return Ok(())
        }

        // increase record count on append
        self.header.indexed_count += 1;
        if save_headers {
            self.save_headers_into(writer)?;
        }
        Ok(())
    }

    /// Updates or append a record into the table file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Index value index.
    /// * `record` - Record to save.
    /// * `save_headers` - Headers will be saved on append when true.
    pub fn save_record(&mut self, index: u64, record: &Record, save_headers: bool) -> Result<()> {
        let mut writer = self.new_writer(false)?;        
        self.save_record_into(&mut writer, index, record, save_headers)?;
        writer.flush()?;
        Ok(())
    }

    /// Perform a healthckeck over the table file by reading
    /// the headers and checking the file size.
    pub fn healthcheck(&mut self) -> Result<Status> {
        // check whenever table file exists
        match self.new_reader() {
            // try to load the table headers
            Ok(mut reader) => if let Err(e) = self.load_headers_from(&mut reader) {
                match e.downcast::<std::io::Error>() {
                    Ok(ex) => match ex.kind() {
                        std::io::ErrorKind::NotFound => {
                            // File not found so the table is new
                            return Ok(Status::New);
                        }
                        std::io::ErrorKind::UnexpectedEof => {
                            // if the file is empty then is new
                            let real_size = file_size(&self.path)?;
                            if real_size < 1 {
                                return Ok(Status::New);
                            }

                            // EOF eror means the table is corrupted
                            return Ok(Status::Corrupted);
                        },
                        _ => bail!(ex)
                    },
                    Err(ex) => return Err(ex)
                }
            },
            Err(e) => match e.downcast::<std::io::Error>() {
                Ok(ex) => match ex.kind() {
                    std::io::ErrorKind::NotFound => {
                        return Ok(Status::New)
                    },
                    _ => bail!(ex)
                },
                Err(ex) => bail!(ex)
            }
        };

        // validate corrupted table
        let real_size = file_size(&self.path)?;
        let expected_size = self.calc_record_pos(self.header.indexed_count);
        if real_size != expected_size {
            // sizes don't match, the file is corrupted
            return Ok(Status::Corrupted);
        }
        
        // validate field count
        if self.header.fields.len() < 1 {
            return Ok(Status::NoFields)
        }

        // all good
        Ok(Status::Good)
    }

    /// Saves the headers and then jump back to the last writer stream position.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    pub fn save_headers_into(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.flush()?;
        let old_pos = writer.stream_position()?;
        writer.rewind()?;
        self.header.write_to(writer)?;
        self.header.fields.write_to(writer)?;
        writer.flush()?;
        writer.seek(SeekFrom::Start(old_pos))?;
        Ok(())
    }

    /// Saves the headers and then jump back to the last writer stream position.
    pub fn save_headers(&self) -> Result<()> {
        let mut writer = self.new_writer(false)?;
        self.save_headers_into(&mut writer)
    }

    /// Loads or creates the table file.
    /// 
    /// # Arguments
    /// 
    /// * `override_on_error` - Overrides the table file if corrupted instead of error.
    /// * `force_override` - Always creates a new table file with the current headers.
    pub fn load_or_create(&mut self, override_on_error: bool, force_override: bool) -> Result<()> {
        let mut should_create = force_override;

        // perform index healthcheck
        if !force_override {
            match self.healthcheck() {
                Ok(v) => match v {
                    Status::Good => return Ok(()),
                    Status::New => should_create = true,
                    Status::NoFields => {
                        let err: IndexError<bool> = IndexError::NoFields;
                        bail!(err)
                    },
                    vu => if !override_on_error {
                        bail!(IndexError::Unavailable(vu))
                    }
                },
                Err(e) => return Err(e)
            }
        }

        // create table file when required
        if should_create {
            let mut writer = self.new_writer(true)?;
            let size = self.calc_record_pos(self.header.indexed_count);
            fill_file(&self.path, size, true)?;
            self.save_headers_into(&mut writer)?;
            writer.flush()?;
        }
        Ok(())
    }

    pub fn printIndex(&mut self){
        let mut counter = 0;
    println!("--- {}",self.header.indexed_count);
        for i in 0..self.header.indexed_count {
            counter = i;
            let value = self.record(i).unwrap();
            println!("{0} - {1} - {2} - {3} - {4} - {5} - {6} ",i,self.record(i).unwrap().unwrap().metadata.height,self.record(i).unwrap().unwrap().metadata.status_flag,self.record(i).unwrap().unwrap().metadata.parent,self.record(i).unwrap().unwrap().metadata.left_node,self.record(i).unwrap().unwrap().metadata.right_node,self.record(i).unwrap().unwrap().metadata.fields[0]);
            //println!("{0} - {1} - {2} - {3} - {4} - {5} - {6} - {7}",i,self.record(i).unwrap().unwrap().metadata.height,self.record(i).unwrap().unwrap().metadata.status_flag,self.record(i).unwrap().unwrap().metadata.parent,self.record(i).unwrap().unwrap().metadata.left_node,self.record(i).unwrap().unwrap().metadata.right_node,self.record(i).unwrap().unwrap().metadata.gid);
        }
    }


    pub fn insertIndex (&mut self, data: Record) -> Result<()> {
        

//println!("Insert {}",self.header.indexed_count);

        let last_index = self.header.indexed_count;
        let newNodeWasInserted = self.insertNewIndex(1, data, 0, last_index)?;
        Ok(())
    }

    pub fn insertNewIndex (&mut self, i: u64, data: Record, prev_node: u64, last_index: u64) -> Result<()> {
//println!("insertando");
        let newNodeWasInserted = self.insertNewAVLNode(i, data, prev_node, last_index);
//println!("debe rebalancear {}",newNodeWasInserted);
        if newNodeWasInserted{
//self.header.indexed_count += 1;
            let (desbalanceado, height_diff) = self.rebalance(last_index);

            let height = match self.record(last_index)? {               
                Some(v) => v.metadata.height,
                None => bail!(DbIndexError::NoLeftNode),
            }; //self.record(last_index).unwrap().unwrap().metadata.height;
            

            let desbalanceado_values =  match self.record(desbalanceado)? {
                    Some(v) => v,
                    None => panic!(" record not found")
                }; //i  - 2 -- A



            let mut L = desbalanceado_values.metadata.left_node;  // B
            let mut R = desbalanceado_values.metadata.right_node;  // D

            let mut L_H : i64 = -1;
            let mut R_H : i64 = -1;
            let mut LL:u64;
            let mut LL_v :Value;
            let mut LL_H : i64 = -1;
            let mut LR:u64;
            let mut LR_v :Value;
            let mut LR_H : i64 = -1;

            let RR:u64;
            let mut RR_v :Value;
            let mut RR_H : i64 = -1;
            let mut RL:u64;
            let mut RL_v :Value;
            let mut RL_H : i64 = -1;
            
            if L != 0{ // B
                let L_v = match self.record(L)? {
                    Some(v) => v,
                    None => panic!(" record not found")
                };  // B
                L_H = L_v.metadata.height;       

                LL = L_v.metadata.left_node;  //C
                if LL !=0{
                    LL_v = match self.record(LL)? {
                        Some(v) => v,
                        None => panic!(" record not found")
                    };  // C
                    LL_H = LL_v.metadata.height;   
                } else {
                    LL_H = -1;    
                }

                LR = L_v.metadata.right_node;  //E
                if LR !=0{
                    LR_v = match self.record(LR)? {
                        Some(v) => v,
                        None => panic!(" record not found")
                    };  // E
                    LR_H = LR_v.metadata.height;   
                } else {
                    LR_H = -1;            
                }
            }

            if R != 0{ // exists  D
                let R_v =  match self.record(R)? {
                    Some(v) => v,
                    None => panic!(" record not found")
                };  // D
                R_H = R_v.metadata.height;       

                RR = R_v.metadata.right_node;  //G
                if RR !=0{
                    RR_v = match self.record(RR)? {
                        Some(v) => v,
                        None => panic!(" record not found")
                    };  // G
                    RR_H = RR_v.metadata.height;
                } else {
                    RR_H = -1;
                }

                RL = R_v.metadata.left_node;  //F
                if RL !=0{
                    RL_v = match self.record(RL)? {
                        Some(v) => v,
                        None => panic!(" record not found")
                    };  // F
                    RL_H = RL_v.metadata.height;
                } else {
                    RL_H = -1;
                } 
            }

            //println!("L_H {} R_H {} LL_H {} LR_H {} RR_H {} RL_H {}", L_H , R_H, LL_H , LR_H,RR_H, RL_H);

            if height_diff > 1 { // left is bigger
                if L_H > R_H {
                    if LL_H > LR_H {
                        //LL
                        self.ll_rotation(desbalanceado);
                    }
                    if LL_H < LR_H{
                        //LR
                        self.lr_rotation(desbalanceado);
                    }
                }
            }

            if height_diff < -1 { // right is bigger
                if L_H < R_H {
                    if RR_H > RL_H{
                        //RR
                        self.rr_rotation(desbalanceado);
                    }
                    if RR_H < RL_H{
                        //RL
                        self.rl_rotation(desbalanceado);
                    }
                }
            }
 //           self.header.indexed_count -= 1;
            //self.printIndex();
        }
        Ok(())
    }

    pub fn index_key_validation () {
        Ok (())
    }

    pub fn insertNewAVLNode (&mut self, i: u64, data: Record, prev_node: u64, last_index: u64) -> bool{//-> Result<()> {
        //let value = self.record(i).unwrap(); // lo quite al final
//println!("data a insertar {}, del nodo {}", data.get(),last_index);
        //let mut index_nuevo=0;
        let mut newNodeWasInserted = false;
        let mut next_node =0;
        let mut parentToStartBalance=0;

        if last_index == 1 {
            // New node
            let mut value = Value::new();
            value.metadata.status_flag = StatusFlag::Yes;
            value.metadata.parent = 0;
            value.metadata.left_node = 0;
            value.metadata.right_node = 0;
            value.metadata.height = 1;

            value.metadata.fields = data;
            //value.data.gid = data;
            self.save_value(last_index, &value).unwrap();
            
            value._fields.get("gid"); // <- Option<&Value>
            value.fields.get_mut("gid"); // <- Option<&mut Value>
            //value.fields //('gid', data); // <- Option<()>

            //value.metadata.gid = data;
            self.save_record(last_index, &value,true).unwrap();

            // Update Node 0
            let mut value = match self.record(0) {
                Ok(opt) => match opt {
                    Some(existing_node) => existing_node,
                    None => panic!(" Record 0 doesn't exist"),
                },
                Err(err) => panic!(" Record 0 doesn't exist {}",err)
            };

            value.metadata.left_node = last_index;
            self.save_record(0, &value,false).unwrap();

            newNodeWasInserted = true;
            parentToStartBalance = value.metadata.parent;
            self.header.indexed_count += 1;
            self.save_header().unwrap();
        } else {
                

            let actual_node = match self.record(i) {
                Ok(opt) => match opt {
                    Some(existing_node) => existing_node,
                    None => panic!(" record not found -"),
                },
                Err(err) => panic!(" record not found --{}",err)
            };
            let gid = &actual_node.metadata.gid;  //  --> Ale metadaqta.gid;

            let mut should_be_left: bool = false;
            let mut should_be_right: bool = false;
            let mut should_create_node = true;

            if data.get().lt(gid.get()){ //data < gid{
                
                next_node = actual_node.metadata.left_node;
                //index_nuevo = existing_node.metadata.left_node;
                should_be_left = true;
            
            } 
            if data.get().gt(gid.get()){ //data > gid{
                next_node = actual_node.metadata.right_node;
                //index_nuevo = existing_node.metadata.right_node;
                should_be_right = true;
            } 
            if data.get().eq(gid.get()){
                should_create_node = false;
            } 

            if next_node == 0 { // Is Empty
                if should_be_left || should_be_right{    
                    /* If we already have de Index records and we are reindexing
                    // New node
                    let mut value = self.record(last_index).unwrap().unwrap();
                    value.metadata.status_flag = value.metadata.status_flag;
                    value.metadata.parent = i;
                    value.metadata.left_node = value.metadata.left_node;
                    value.metadata.right_node = value.metadata.right_node;
                    value.metadata.height = value.metadata.height;
                    value.metadata.gid = value.metadata.gid;  //  --> Ale metadaqta.gid;
                    self.save_record(last_index, &value).unwrap();
                    */

                    // New node
                    let mut value = Record::new();
                    value.metadata.status_flag = StatusFlag::Yes;
                    value.metadata.parent = i;
                    value.metadata.left_node = 0;
                    value.metadata.right_node = 0;
                    value.metadata.height = 1;
                    value.metadata.gid = data;
                    self.save_record(last_index, &value,true).unwrap();

                    // Actual node
                    let mut value = actual_node.clone();

                    if should_be_left{
                        value.metadata.left_node = last_index;
                        value.metadata.right_node = value.metadata.right_node;
                        value.metadata.height = value.metadata.height;
                        //println!("Inserted left");
                    } else {
                        value.metadata.left_node = value.metadata.left_node;
                        value.metadata.right_node = last_index;
                        value.metadata.height = value.metadata.height;
                        //println!("Inserted right");
                    }
                    self.save_record(i, &value,false).unwrap();

                    newNodeWasInserted = true;
                    parentToStartBalance = value.metadata.parent;
                    self.header.indexed_count += 1;
                    self.save_header().unwrap();
                    //self.printIndex();                
                }
            } else {
                if should_create_node{
                    //println!("New iteration id:{} data:{} last id:{} newNodeWasInserted {}",prev_node, data, i,newNodeWasInserted);
                    //println!("Posicion en la q estoy revisando id:{} data:{} last id:{} newNodeWasInserted {}",prev_node, data, i,newNodeWasInserted);
                    newNodeWasInserted = self.insertNewAVLNode(next_node, data, i,last_index);
                }
            }

        }
//println!("{}",newNodeWasInserted);
        return newNodeWasInserted;
        //Ok(())
    }

    pub fn searchKey (&mut self, i: u64, data: Record) -> Option<u64>{
       println!("{}",i);;
        let mut index_id = 0;
        let mut next_node =0;

        let existing_node = match self.record(i) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => panic!(" record not found"),
            },
            Err(err) => panic!(" record not found {}",err)
        };

        let gid = existing_node.metadata.gid;  //  --> Ale metadaqta.gid;

        let mut keep_searching = true;

        if data.get().lt(gid.get()){//data < gid{
            next_node = existing_node.metadata.left_node;          
        } 
        if data.get().gt(gid.get()){//data > gid{
            if i !=0 {
                next_node = existing_node.metadata.right_node;
            } else {
                next_node = existing_node.metadata.left_node;
            }
        } 
        if data.get().eq(gid.get()){
            keep_searching = false;
            index_id = i;
        } else {     
            if keep_searching && next_node != 0 {
                index_id = self.searchKey(next_node, data)?;
            } else {
                index_id =0; //return None;
            }
        }

        return Some(index_id);
        //Ok(())
    }

fn rebalance(&self,i:u64) -> (u64, i64){
//println!("rebalanceo");
    let mut counter = i;
    let mut last_counter = i;
    let mut height = 0;
    let mut calculated;
    let mut dif=0;

    while (counter != 0) && (dif > -2) && (dif < 2) {
        calculated= self.calc_height(counter);

        height= calculated.0;
        dif =calculated.1;
        
        let mut value = match self.record(counter) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => panic!(" record not found "),
            },
            Err(err) => panic!(" record not found {}",err)
        };
        value.metadata.height = height;
        self.save_record(counter, &value,false).unwrap();

        last_counter = counter;
        //println!("      rebalanced {} parent: {} height: {} dif: {}",counter,value.metadata.parent,height,dif);
        counter = value.metadata.parent;

        if (dif < -1) || (dif > 1) || (counter== last_counter) {
            //println!("      Last reviewed: {} D: {} H: {}", last_counter, dif, height);
            counter = 0;
        } 
    }
    return (last_counter,dif);

}

fn calc_height (&self,i:u64) -> (i64, i64){
//println!("recalculando height");
    let mut existing_node = match self.record(i) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let left_node_id = existing_node.metadata.left_node;
    let right_node_id = existing_node.metadata.right_node;

    let mut last_left_height = match self.record(left_node_id) {
        Ok(opt) => match opt {
            Some(v) => v.metadata.height,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let mut last_right_height = match self.record(right_node_id) {
        Ok(opt) => match opt {
            Some(v) => v.metadata.height,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };

    let last_left_height= if left_node_id != 0 {last_left_height+1} else{0};
    let last_right_height= if right_node_id != 0 {last_right_height+1} else{0};

    let recalculated_height = i64::max(last_left_height, last_right_height);
    let height_dif = last_left_height- last_right_height;

    return (recalculated_height,height_dif);
}

fn ll_rotation(&self, index_id: u64){
//println!("ll");
    let oldparent = index_id;
    let newright = oldparent;
    let oldparent_values = match self.record(oldparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let newparent = oldparent_values.metadata.left_node;
    let newparent_right = 0;
    // New parent
    let valuenewparent = match self.record(newparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let newright_left = valuenewparent.metadata.right_node;
    let newparent_parent = oldparent_values.metadata.parent;
    let oldparent_parent = oldparent_values.metadata.parent;

    
    // Old parent of parent
    let valueoldparent_parent = match self.record(oldparent_parent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New right
    let valuenewright = match self.record(newright) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New left for right
    let valuenewright_left = match self.record(newright_left) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };

    //println!("      LL id: {}",index_id);

    // New parent
    let mut value = valuenewparent.clone();
    value.metadata.parent = newparent_parent;
    value.metadata.right_node = oldparent;
    self.save_record(newparent, &value,false).unwrap();

    // Old parent of parent
    let mut value = valueoldparent_parent.clone();
    if value.metadata.left_node == oldparent {
        value.metadata.left_node = newparent;
        value.metadata.right_node = valueoldparent_parent.metadata.right_node;
    } else {
        value.metadata.left_node = valueoldparent_parent.metadata.left_node;
        value.metadata.right_node = newparent;
    }
    self.save_record(oldparent_parent, &value,false).unwrap();


    // New right
    let mut value = valuenewright.clone();
    value.metadata.parent = newparent;
    value.metadata.left_node = newright_left;
    value.metadata.height = valuenewright.metadata.height - valuenewparent.metadata.height;

    self.save_record(newright, &value,false).unwrap();

    // New left for right
    if newright_left !=0 {
        let mut value = valuenewright_left.clone();
        value.metadata.parent = newright;
        self.save_record(newright_left, &value,false).unwrap();
    }

    self.rebalance(index_id);

}

fn lr_rotation(&self, index_id: u64){
    let oldparent = index_id;  // A
    let newright = oldparent;  // A
    let valueoldparent= match self.record(oldparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let oldparent_left = valueoldparent.metadata.left_node;   // B
    let newparent= match self.record(oldparent_left) {
        Ok(opt) => match opt {
            Some(v) => v.metadata.right_node,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };  // C

    let newparent_right = oldparent;
    let oldparent_parent = valueoldparent.metadata.parent;
    let newparent_parent = valueoldparent.metadata.parent;


    let valuenewparent = match self.record(newparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let newright_left = valuenewparent.metadata.right_node; // CR
    let oldleft_right_left = valuenewparent.metadata.left_node; //CL
    
    // Old parent of parent
    let valueoldparent_parent = match self.record(oldparent_parent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New right
    let valuenewright = match self.record(newright) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New left for right
    let valuenewright_left = match self.record(newright_left) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New right for Old left
    let valueoldleft_right = match self.record(oldleft_right_left) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // Old left
    let valueoldparent_left = match self.record(oldparent_left) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };

    //println!("      LR id: {}",index_id);
    
    // New parent
    let mut value = valuenewparent.clone();
    value.metadata.parent = newparent_parent;
    value.metadata.left_node = oldparent_left;  // B
    value.metadata.right_node = newright; // A
    self.save_record(newparent, &value,false).unwrap();

    //println!("{:#}", value.data(newparent).);
    // Old parent of parent
    let mut value = valueoldparent_parent.clone();
    if value.metadata.left_node == oldparent {
        value.metadata.left_node = newparent;
        value.metadata.right_node = valueoldparent_parent.metadata.right_node;
    } else {
        value.metadata.left_node = valueoldparent_parent.metadata.left_node;
        value.metadata.right_node = newparent;
    }
    self.save_record(oldparent_parent, &value,false).unwrap();

    // New right
    let mut value = valuenewright.clone();
    value.metadata.parent = newparent;
    value.metadata.left_node = newright_left;
    self.save_record(newright, &value,false).unwrap();

    // New left for right
    if newright_left !=0 {
        let mut value = valuenewright_left.clone() ;
        value.metadata.parent = newright;
        self.save_record(newright_left, &value,false).unwrap();
    }

    // New right for old left
    if oldleft_right_left !=0 {    
        let mut value = match self.record(oldleft_right_left) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => panic!(" record not found"),
            },
            Err(err) => panic!(" record not found {}",err)
        };
        value.metadata.status_flag = valueoldleft_right.metadata.status_flag;
        value.metadata.parent = oldparent_left;
        value.metadata.left_node = valueoldleft_right.metadata.left_node;
        value.metadata.right_node = valueoldleft_right.metadata.right_node;
        value.metadata.height = valueoldleft_right.metadata.height;
        value.metadata.gid = valueoldleft_right.metadata.gid;  //  --> Ale metadaqta.gid;
        self.save_record(oldleft_right_left, &value,false).unwrap();
    }

    // old parent left  // B
    let mut value = valueoldparent_left.clone();
    value.metadata.parent = newparent;
    value.metadata.right_node = oldleft_right_left;

    self.save_record(oldparent_left, &value,false).unwrap();

    let height= self.calc_height(oldparent_left).0;
    
    let valueoldparent_left = match self.record(oldparent_left) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };

    let mut value = valueoldparent_left;
    value.metadata.height = height;
    self.save_record(oldparent_left, &value,false).unwrap();

    self.rebalance(index_id);

} 

fn rr_rotation(&self, index_id: u64){
    let oldparent = index_id; // A
    let newleft = oldparent; // A
    let valueoldparent = match self.record(oldparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let newparent = valueoldparent.metadata.right_node; // B
    let newparent_left = oldparent; // A
    // New parent
    let valuenewparent = match self.record(newparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let newleft_right = valuenewparent.metadata.left_node; // BL
    let newparent_parent = valueoldparent.metadata.parent; // 0
    let oldparent_parent = valueoldparent.metadata.parent; // 0 -- 3

    // Old parent of parent
    let valueoldparent_parent = match self.record(oldparent_parent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New left
    let valuenewleft = match self.record(newleft) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New right for left
    let valuenewleft_right = match self.record(newleft_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };

    //println!("      RR id: {}",index_id);

    // New parent
    let mut value = valuenewparent.clone();
    value.metadata.status_flag = valuenewparent.metadata.status_flag;
    value.metadata.parent = newparent_parent;
    value.metadata.right_node = valuenewparent.metadata.right_node;
    value.metadata.left_node = oldparent;
    value.metadata.gid = valuenewparent.metadata.gid;  //  --> Ale metadaqta.gid;
    self.save_record(newparent, &value,false).unwrap();


    // Old parent of parent
    let mut value = valueoldparent_parent.clone();
    value.metadata.status_flag = valueoldparent_parent.metadata.status_flag;
    value.metadata.parent = valueoldparent_parent.metadata.parent;
    if value.metadata.right_node == oldparent {
        value.metadata.right_node = newparent;
        value.metadata.left_node = valueoldparent_parent.metadata.left_node;
    } else {
        value.metadata.right_node = valueoldparent_parent.metadata.right_node;
        value.metadata.left_node = newparent;
    }
    value.metadata.height = valueoldparent_parent.metadata.height;
    value.metadata.gid = valueoldparent_parent.metadata.gid;  //  --> Ale metadaqta.gid;
    self.save_record(oldparent_parent, &value,false).unwrap();


    // New left
    let mut value = valuenewleft.clone();
    value.metadata.status_flag = valuenewleft.metadata.status_flag;
    value.metadata.parent = newparent;
    value.metadata.right_node = newleft_right;
    value.metadata.left_node = valuenewleft.metadata.left_node;
    value.metadata.height = valuenewleft.metadata.height - valuenewparent.metadata.height;
    value.metadata.gid = valuenewleft.metadata.gid;  //  --> Ale metadaqta.gid;
    self.save_record(newleft, &value,false).unwrap();


    // New right for left
    if newleft_right !=0 {
        let mut value = valuenewleft_right.clone();
        value.metadata.status_flag = valuenewleft_right.metadata.status_flag;
        value.metadata.parent = newleft;
        value.metadata.right_node = valuenewleft_right.metadata.right_node;
        value.metadata.left_node = valuenewleft_right.metadata.left_node;
        value.metadata.height = valuenewleft_right.metadata.height;
        value.metadata.gid = valuenewleft_right.metadata.gid;  //  --> Ale metadaqta.gid;
        self.save_record(newleft_right, &value,false).unwrap();
    }

    self.rebalance(index_id);

}

fn rl_rotation(&self, index_id: u64){
    let oldparent = index_id;  // A
    let newleft = oldparent;

    let valueoldparent =match self.record(oldparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };

    let oldparent_right = valueoldparent.metadata.right_node;
    let newparent= match self.record(oldparent_right) {
        Ok(opt) => match opt {
            Some(v) => v.metadata.left_node,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };

    let newparent_left = oldparent; //A
    let oldparent_parent = valueoldparent.metadata.parent;
    let newparent_parent = valueoldparent.metadata.parent;

     
    let valuenewparent = match self.record(newparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let newleft_right = valuenewparent.metadata.left_node;
    let oldright_left_right = valuenewparent.metadata.right_node;
   
    // Old parent of parent
    let valueoldparent_parent = match self.record(oldparent_parent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New left
    let valuenewleft = match self.record(newleft) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New right for left
    let valuenewleft_right = match self.record(newleft_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // New left for Old right
    let valueoldright_left = match self.record(oldright_left_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    // Old right
    let valueoldparent_right = match self.record(oldparent_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };

    //println!("      RL id: {}",index_id);

    // New parent
    let mut value = valuenewparent.clone();
    value.metadata.status_flag = valuenewparent.metadata.status_flag;
    value.metadata.parent = newparent_parent;
    value.metadata.right_node = oldparent_right;
    value.metadata.left_node = newleft;
    value.metadata.gid = valuenewparent.metadata.gid;  //  --> Ale metadaqta.gid;
    value.metadata.height = valuenewparent.metadata.height;
    self.save_record(newparent, &value,false).unwrap();


    // Old parent of parent
    let mut value = valueoldparent_parent.clone();
    value.metadata.status_flag = valueoldparent_parent.metadata.status_flag;
    value.metadata.parent = valueoldparent_parent.metadata.parent;
    if value.metadata.right_node == oldparent {
        value.metadata.right_node = newparent;
        value.metadata.left_node = valueoldparent_parent.metadata.left_node;
    } else {
        value.metadata.right_node = valueoldparent_parent.metadata.right_node;
        value.metadata.left_node = newparent;
    }
    value.metadata.height = valueoldparent_parent.metadata.height;
    value.metadata.gid = valueoldparent_parent.metadata.gid;  //  --> Ale metadaqta.gid;
    self.save_record(oldparent_parent, &value,false).unwrap();


    // New left
    let mut value = valuenewleft.clone();
    value.metadata.status_flag = valuenewleft.metadata.status_flag;
    value.metadata.parent = newparent;
    value.metadata.right_node = newleft_right;
    value.metadata.left_node = valuenewleft.metadata.left_node;
    value.metadata.height = valuenewleft.metadata.height;
    value.metadata.gid = valuenewleft.metadata.gid;  //  --> Ale metadaqta.gid;
    self.save_record(newleft, &value,false).unwrap();


    // New right for left
    if newleft_right !=0 {
        let mut value = valuenewleft_right.clone();
        value.metadata.status_flag = valuenewleft_right.metadata.status_flag;
        value.metadata.parent = newleft;
        value.metadata.right_node = valuenewleft_right.metadata.right_node;
        value.metadata.left_node = valuenewleft_right.metadata.left_node;
        value.metadata.height = valuenewleft_right.metadata.height;
        value.metadata.gid = valuenewleft_right.metadata.gid;  //  --> Ale metadaqta.gid;
        self.save_record(newleft_right, &value,false).unwrap();
    }


    // New left for old right
    if oldright_left_right !=0 {
        let mut value = match self.record(oldright_left_right) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => panic!(" record not found"),
            },
            Err(err) => panic!(" record not found {}",err)
        };
        value.metadata.status_flag = valueoldright_left.metadata.status_flag;
        value.metadata.parent = oldparent_right;
        value.metadata.right_node = valueoldright_left.metadata.right_node;
        value.metadata.left_node = valueoldright_left.metadata.left_node;
        value.metadata.height = valueoldright_left.metadata.height;
        value.metadata.gid = valueoldright_left.metadata.gid;  //  --> Ale metadaqta.gid;
        self.save_record(oldright_left_right, &value,false).unwrap();
    }


    // old parent left  // B
    let valueoldparent_right = match self.record(oldparent_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    let mut value = valueoldparent_right.clone();
    value.metadata.status_flag = valueoldparent_right.metadata.status_flag;
    value.metadata.parent = newparent;
    value.metadata.left_node = oldright_left_right;
    value.metadata.right_node = valueoldparent_right.metadata.right_node;
    value.metadata.height = valueoldparent_right.metadata.height;
    value.metadata.gid = valueoldparent_right.metadata.gid;  //  --> Ale metadaqta.gid;
    self.save_record(oldparent_right, &value,false).unwrap();

    let height= self.calc_height(oldparent_right).0;
    
    let mut value = match self.record(oldparent_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" record not found"),
        },
        Err(err) => panic!(" record not found {}",err)
    };
    value.metadata.height = height;
    self.save_record(oldparent_right, &value,false).unwrap();

    self.rebalance(index_id);

}
}

#[cfg(test)]
pub mod test_helper {
    use super::*;
    use crate::test_helper::*;
    use crate::db::field::{Value, FieldType, Field, Header as RecordHeader};
    use crate::db::table::header::test_helper::build_header_bytes;
    use tempfile::TempDir;

    /// It's the size of a record header without any field.
    pub const EMPTY_RECORD_HEADER_BYTES: usize = u32::BYTES;

    /// Record header size generated by add_fields function.
    pub const ADD_FIELDS_HEADER_BYTES: usize = Field::BYTES * 2 + u32::BYTES;

    /// Record size generated by aADD_FIELDS_RECORD_BYTESdd_fields function.
    pub const ADD_FIELDS_RECORD_BYTES: usize = 13;

    /// Fake records bytes size generated by fake_records.
    pub const FAKE_RECORDS_BYTES: usize = ADD_FIELDS_RECORD_BYTES * 3;

    /// Fake index with fields byte size.
    pub const FAKE_INDEX_BYTES: usize = Header::BYTES + ADD_FIELDS_HEADER_BYTES + FAKE_RECORDS_BYTES;

    /// Byte slice that represents an empty record header.
    pub const EMPTY_RECORD_HEADER_BYTE_SLICE: [u8; EMPTY_RECORD_HEADER_BYTES] = [
        // field count
        0, 0, 0, 0u8
    ];

    /// Byte slice to be generated by the record header generated by add_fields_function.
    pub const ADD_FIELDS_HEADER_BYTE_SLICE: [u8; ADD_FIELDS_HEADER_BYTES] = [
        // field count
        0, 0, 0, 2u8,

        // foo field name value size
        0, 0, 0, 3u8,
        // foo field name value
        102u8, 111u8, 111u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
        // foo field type
        4u8, 0, 0, 0, 0,

        // bar field name value size
        0, 0, 0, 3u8,
        // bar field name value
        98u8, 97u8, 114u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
        // bar field type
        12u8, 0, 0, 0, 5u8
    ];

    pub const FAKE_RECORDS_BYTE_SLICE: [u8; FAKE_RECORDS_BYTES] = [
        // first record
        // foo field
        13u8, 246u8, 33u8, 122u8,
        // bar field
        0, 0, 0, 3u8, 97u8, 98u8, 99u8, 0, 0,

        // second record
        // foo field
        20u8, 149u8, 141u8, 65u8,
        // bar field
        0, 0, 0, 4u8, 100u8, 102u8, 101u8, 103u8, 0,

        // third record
        // foo field
        51u8, 29u8, 39u8, 30u8,
        // bar field
        0, 0, 0, 5u8, 104u8, 105u8, 49u8, 50u8, 51u8
    ];

    /// Add test fields into record header.
    /// 
    /// # Arguments
    /// 
    /// * `header` - Record header to add fields into.
    pub fn add_fields(header: &mut RecordHeader) -> Result<()> {
        header.add("foo", FieldType::I32)?;
        header.add("bar", FieldType::Str(5))?;

        Ok(())
    }

    /// Create fake records based on the fields added by add_fields.
    /// 
    /// # Arguments
    /// 
    /// * `records` - Record vector to add records into.
    pub fn fake_records() -> Result<Vec<Record>> {
        let mut header = RecordHeader::new();
        add_fields(&mut header)?;
        let mut records = Vec::new();

        // add first record
        let mut record = header.new_record()?;
        record.set_by_index(0, Value::I32(234234234i32));
        record.set_by_index(1, Value::Str("abc".to_string()));
        records.push(record);

        // add second record
        let mut record = header.new_record()?;
        record.set_by_index(0, Value::I32(345345345i32));
        record.set_by_index(1, Value::Str("dfeg".to_string()));
        records.push(record);

        // add third record
        let mut record = header.new_record()?;
        record.set_by_index(0, Value::I32(857548574i32));
        record.set_by_index(1, Value::Str("hi123".to_string()));
        records.push(record);

        Ok(records)
    }

    /// Resturn a fake table uuid.
    pub fn fake_table_uuid() -> Uuid {
        Uuid::from_bytes([0u8; Uuid::BYTES])
    }

    /// Return a fake table file with fields as byte slice and the record count.
    pub fn fake_table_with_fields() -> Result<([u8; FAKE_INDEX_BYTES], u64)> {
        // init buffer
        let mut buf = [0u8; FAKE_INDEX_BYTES];
        let header_buf = build_header_bytes("my_table", 3245634545244324234u64, Some(fake_table_uuid()));
        copy_bytes(&mut buf, &header_buf, 0)?;
        copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, Header::BYTES)?;
        copy_bytes(&mut buf, &FAKE_RECORDS_BYTE_SLICE, Header::BYTES + ADD_FIELDS_HEADER_BYTES)?;
        Ok((buf, 3))
    }

    /// Write a fake table bytes into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `unprocessed` - If `true` then build all records with MatchFlag::None.
    pub fn write_fake_table(writer: &mut (impl Seek + Write), unprocessed: bool) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        // write table header
        let mut header = Header::new("my_table", Some(fake_table_uuid()))?;
        header.record_count = 4;
        header.write_to(writer)?;

        // write record header
        add_fields(&mut header.record)?;
        header.record.write_to(writer)?;
        
        // write first record
        let mut record = header.record.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(111i32));
            record.set("bar", Value::Str("first".to_string()));
        }
        header.record.write_record(writer, &record)?;
        records.push(record);
        
        // write second record date
        let mut record = header.record.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(222i32));
            record.set("bar", Value::Str("2th".to_string()));
        }
        header.record.write_record(writer, &record)?;
        records.push(record);
        
        // write third record date
        let mut record = header.record.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(333i32));
            record.set("bar", Value::Str("3rd".to_string()));
        }
        header.record.write_record(writer, &record)?;
        records.push(record);

        // write fourth record date
        let mut record = header.record.new_record()?;
        if !unprocessed {
            record.set("foo", Value::I32(444i32));
            record.set("bar", Value::Str("4th".to_string()));
        }
        header.record.write_record(writer, &record)?;
        records.push(record);

        Ok(records)
    }

    /// Create a fake table file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Table file path.
    /// * `empty` - If `true` then build all records as empty records.
    pub fn create_fake_table(path: &PathBuf, unprocessed: bool) -> Result<Vec<Record>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        let records = write_fake_table(&mut writer, unprocessed)?;
        writer.flush()?;

        Ok(records)
    }

    /// Execute a function with both a temp directory and a new table.
    /// 
    /// # Arguments
    /// 
    /// * `f` - Function to execute.
    pub fn with_tmpdir_and_table(f: &impl Fn(&TempDir, &mut Table) -> Result<()>) {
        let sub = |dir: &TempDir| -> Result<()> {
            // create Table and execute
            let mut table = Table::new(
                dir.path().join("t.fmtable"),
                "my_table",
                Some(fake_table_uuid())
            )?;

            // execute function
            match f(&dir, &mut table) {
                Ok(_) => Ok(()),
                Err(e) => bail!(e)
            }
        };
        with_tmpdir(&sub)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helper::*;
    use std::io::Cursor;
    use crate::test_helper::*;
    use crate::db::field::{Value, Header as RecordHeader};
    use crate::db::table::header::test_helper::build_header_bytes;

    #[test]
    fn file_extension_regex() {
        let rx = Table::file_extension_regex();
        assert!(rx.is_match("hello.fmtable"), "expected to match \"hello.fmtable\" but got false");
        assert!(rx.is_match("/path/to/hello.fmtable"), "expected to match \"/path/to/hello.fmtable\" but got false");
        assert!(!rx.is_match("hello.table"), "expected to not match \"hello.table\" but got true");
    }

    #[test]
    fn new() {
        let header = Header::new("my_table", Some(fake_table_uuid())).unwrap();
        let expected = Table{
            path: "my_table.fmtable".into(),
            header,
        };
        match Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())) {
            Ok(v) => assert_eq!(expected, v),
            Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
        }
    }

    #[test]
    fn calc_record_pos_with_fields() {
        let mut table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();

        // add fields
        if let Err(e) = add_fields(&mut table.header.record) {
            assert!(false, "expected to add fields, but got error: {:?}", e);
        }
        assert_eq!(241, table.calc_record_pos(2));
        assert_eq!(254, table.calc_record_pos(3));
    }

    #[test]
    fn calc_record_pos_without_fields() {
        let table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();
        let pos = Header::BYTES as u64 + table.header.record.size_as_bytes();
        assert_eq!(pos, table.calc_record_pos(1));
        assert_eq!(pos, table.calc_record_pos(2));
        assert_eq!(pos, table.calc_record_pos(3));
    }

    #[test]
    fn load_headers_from() {
        // create buffer
        let mut buf = [0u8; Header::BYTES + ADD_FIELDS_HEADER_BYTES];
        let index_header_buf = build_header_bytes("my_table", 3245634545244324234u64, Some(fake_table_uuid()));
        if let Err(e) = copy_bytes(&mut buf, &index_header_buf, 0) {
            assert!(false, "{:?}", e);
        }
        if let Err(e) = copy_bytes(&mut buf, &ADD_FIELDS_HEADER_BYTE_SLICE, Header::BYTES) {
            assert!(false, "{:?}", e);
        }
        let mut reader = Cursor::new(buf.to_vec());

        // test load_headers
        let mut table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();
        if let Err(e) = table.load_headers_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
        }

        // check expected index header
        let mut expected = Header::new("my_table", Some(fake_table_uuid())).unwrap();
        expected.record_count = 3245634545244324234u64;
        assert_eq!(expected, table.header);

        // check expected record header
        let mut expected = RecordHeader::new();
        if let Err(e) = add_fields(&mut expected) {
            assert!(false, "expected to add fields, but got error: {:?}", e);
        }
        assert_eq!(expected, table.header.record);
    }

    #[test]
    fn seek_record_from_with_fields() {
        // init buffer
        let (buf, record_count) = match fake_table_with_fields() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init table and expected records
        let mut table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();
        table.header.record_count = record_count;
        if let Err(e) = add_fields(&mut table.header.record) {
            assert!(false, "{:?}", e);
        }
        let expected = match fake_records() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test first record
        let record = match table.seek_record_from(&mut reader, 0) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => {
                    assert!(false, "expected {:?} but got None", expected[0]);
                    return;
                }
            },
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[0], record);

        // test second record
        let record = match table.seek_record_from(&mut reader, 1) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => {
                    assert!(false, "expected {:?} but got None", expected[0]);
                    return;
                }
            },
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[1], record);

        // test third record
        let record = match table.seek_record_from(&mut reader, 2) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => {
                    assert!(false, "expected {:?} but got None", expected[0]);
                    return;
                }
            },
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert_eq!(expected[2], record);
    }

    #[test]
    fn seek_record_from_without_fields() {
        // init buffer
        let buf = [0u8];
        let mut reader = Cursor::new(buf.to_vec());

        // init table
        let mut table = Table::new("my_table.fmtable".into(), "my_table", Some(fake_table_uuid())).unwrap();
        table.header.record_count = 4;

        // test
        match table.seek_record_from(&mut reader, 0) {
            Ok(v) => assert!(false, "expected TableError::NoFields but got {:?}", v),
            Err(e) => match e.downcast::<TableError>() {
                Ok(ex) => match ex {
                    TableError::NoFields => {},
                    te => assert!(false, "expected TableError::NoFields but got TableError::{:?}", te)
                },
                Err(ex) => assert!(false, "expected TableError::NoFields but got error: {:?}", ex)
            }
        }
    }

    #[test]
    fn record_with_fields() {
        with_tmpdir_and_table(&|_, table| {
            // init buffer
            let (buf, record_count) = match fake_table_with_fields() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            create_file_with_bytes(&table.path, &buf)?;

            // init table and expected records
            table.header.record_count = record_count;
            if let Err(e) = add_fields(&mut table.header.record) {
                assert!(false, "{:?}", e);
            }
            let expected = match fake_records() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };

            // test first record
            let data = match table.record(0) {
                Ok(opt) => match opt {
                    Some(v) => v,
                    None => {
                        assert!(false, "expected {:?} but got None", expected[0]);
                        bail!("");
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e);
                }
            };
            assert_eq!(expected[0], data);

            // test second record
            let data = match table.record(1) {
                Ok(opt) => match opt {
                    Some(v) => v,
                    None => {
                        assert!(false, "expected {:?} but got None", expected[0]);
                        bail!("")
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            assert_eq!(expected[1], data);

            // test third record
            let data = match table.record(2) {
                Ok(opt) => match opt {
                    Some(v) => v,
                    None => {
                        assert!(false, "expected {:?} but got None", expected[0]);
                        bail!("")
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            assert_eq!(expected[2], data);
            Ok(())
        });
    }

    #[test]
    fn record_without_fields() {
        with_tmpdir_and_table(&|_, table| {
            // init buffer
            let buf = [0u8];
            create_file_with_bytes(&table.path, &buf)?;

            // init table
            table.header.record_count = 4;

            // test
            match table.record(0) {
                Ok(v) => assert!(false, "expected TableError::NoFields but got {:?}", v),
                Err(e) => match e.downcast::<TableError>() {
                    Ok(ex) => match ex {
                        TableError::NoFields => {},
                        te => assert!(false, "expected TableError::NoFields but got TableError::{:?}", te)
                    },
                    Err(ex) => assert!(false, "expected TableError::NoFields but got error: {:?}", ex)
                }
            }

            Ok(())
        });
    }

    #[test]
    fn save_record_into_smaller_file() {
        with_tmpdir_and_table(&|_, table| {
            // create table
            let mut records = create_fake_table(&table.path, false)?;
            add_fields(&mut table.header.record)?;

            // set record count to trigger the error
            table.header.record_count = 1;

            // test
            let expected = "can't write or append the record, the table file is too small";
            records[2].set("foo", Value::I32(11));
            records[2].set("bar", Value::Str("hello".to_string()));
            match table.save_record(2, &records[2], true) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }
            
            Ok(())
        });
    }

    #[test]
    fn save_record_into_with_fields() {
        with_tmpdir_and_table(&|_, table| {
            // create table and check original value
            let mut records = create_fake_table(&table.path, false)?;
            add_fields(&mut table.header.record)?;
            table.header.record_count = records.len() as u64;

            // read old record value
            let pos = table.calc_record_pos(2);
            let mut buf = [0u8; ADD_FIELDS_RECORD_BYTES];
            let file = File::open(&table.path)?;
            let mut reader = BufReader::new(file);
            let mut old_bytes_before = vec!(0u8; pos as usize);
            let mut old_bytes_after = vec!(0u8; ADD_FIELDS_RECORD_BYTES);
            reader.read_exact(&mut old_bytes_before)?;
            reader.read_exact(&mut buf)?;
            reader.read_exact(&mut old_bytes_after)?;
            let expected = [
                // foo field
                0, 0, 1u8, 77u8,
                // bar field
                0, 0, 0, 3u8, 51u8, 114u8, 100u8, 0, 0
            ];
            assert_eq!(expected, buf);

            // save record and check saved record value
            let expected = [
                // foo field
                0, 0, 0, 11u8,
                // bar field
                0, 0, 0, 5u8, 104u8, 101u8, 108u8, 108u8, 111u8
            ];
            records[2].set("foo", Value::I32(11));
            records[2].set("bar", Value::Str("hello".to_string()));
            if let Err(e) = table.save_record(2, &records[2], true) {
                assert!(false, "expected success but got error: {:?}", e)
            }
            reader.seek(SeekFrom::Start(0))?;
            let mut new_bytes_before = vec!(0u8; pos as usize);
            let mut new_bytes_after = vec!(0u8; ADD_FIELDS_RECORD_BYTES);
            reader.read_exact(&mut new_bytes_before)?;
            reader.read_exact(&mut buf)?;
            reader.read_exact(&mut new_bytes_after)?;
            assert_eq!(old_bytes_before, new_bytes_before);
            assert_eq!(expected, buf);
            assert_eq!(old_bytes_after, new_bytes_after);

            Ok(())
        });
    }

    #[test]
    fn save_record_into_without_fields() {
        with_tmpdir_and_table(&|_, table| {
            // create table and create expected table file contents
            let mut records = create_fake_table(&table.path, true)?;
            let mut expected = Vec::new();
            let file = File::open(&table.path)?;
            let mut reader = BufReader::new(file);
            reader.read_to_end(&mut expected)?;

            // test
            records[2].set("foo", Value::I32(11));
            records[2].set("bar", Value::Str("hello".to_string()));
            match table.save_record(2, &records[2], true) {
                Ok(()) => assert!(false, "expected TableError::NoFields but got success"),
                Err(e) => match e.downcast::<TableError>() {
                    Ok(ex) => match ex {
                        TableError::NoFields => {},
                        te => assert!(false, "expected TableError::NoFields but got TableError::{:?}", te)
                    },
                    Err(ex) => assert!(false, "expected TableError::NoFields but got error: {:?}", ex)
                }
            }

            // check file after invalid save, it shouldn't change
            let mut buf = Vec::new();
            let file = File::open(&table.path)?;
            let mut reader = BufReader::new(file);
            reader.read_to_end(&mut buf)?;
            assert_eq!(expected, buf);

            Ok(())
        });
    }

    #[test]
    fn healthcheck_new_table() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            // test healthcheck status
            let expected = Status::New;
            match table.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_new_index_with_empty_file() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            // test healthcheck status
            table.new_writer(true)?;
            let expected = Status::New;
            match table.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_corrupted_headers() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            let buf = [0u8; 5];
            create_file_with_bytes(&table.path, &buf)?;
            let expected = Status::Corrupted;
            match table.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_corrupted() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            let mut buf = [0u8; Header::BYTES+EMPTY_RECORD_HEADER_BYTES+5];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new("my_table", Some(fake_table_uuid()))?;
            header.record_count = 10;
            header.write_to(&mut writer)?;

            create_file_with_bytes(&table.path, &buf)?;
            add_fields(&mut table.header.record)?;
            assert_eq!(Status::Corrupted, table.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_good() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            create_fake_table(&table.path, false)?;
            assert_eq!(Status::Good, table.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_no_fields() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            let mut writer = table.new_writer(true)?;
            table.save_headers_into(&mut writer)?;
            assert_eq!(Status::NoFields, table.healthcheck()?);
            Ok(())
        });
    }

    #[test]
    fn save_headers_into() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            // create table file and read table header data
            create_fake_table(&table.path, false)?;
            let mut reader = table.new_reader()?;
            let size = Header::BYTES + 122;
            let mut expected = vec![0u8; size];
            reader.read_exact(&mut expected)?;
            reader.rewind()?;
            table.header.load_from(&mut reader)?;
            table.header.record.load_from(&mut reader)?;

            // test save table header
            let mut buf = vec![0u8; size];
            let wrt = &mut buf as &mut [u8];
            let mut writer = Cursor::new(wrt);
            if let Err(e) = table.save_headers_into(&mut writer) {
                assert!(false, "expected success but got error: {:?}", e);
            };
            assert_eq!(expected, buf);
            
            Ok(())
        });
    }

    #[test]
    fn save_headers() {
        with_tmpdir_and_table(&|_, table| -> Result<()> {
            // create table file and read table header data
            create_fake_table(&table.path, false)?;
            let mut reader = table.new_reader()?;
            let size = Header::BYTES + 122;
            let mut expected = vec![0u8; size];
            reader.read_exact(&mut expected)?;
            reader.rewind()?;
            table.header.load_from(&mut reader)?;
            table.header.record.load_from(&mut reader)?;

            // test save table header
            assert_eq!(4, table.header.record_count);
            table.header.record_count = 5;
            if let Err(e) = table.save_headers() {
                assert!(false, "expected success but got error: {:?}", e);
            };
            table.header.record_count = 4;
            assert_eq!(4, table.header.record_count);
            reader.rewind()?;
            table.header.load_from(&mut reader)?;
            assert_eq!(5, table.header.record_count);
            
            Ok(())
        });
    }
}