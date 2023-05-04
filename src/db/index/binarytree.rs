pub mod header;
pub mod value;

use anyhow::{bail, Result};
use regex::Regex;
use uuid::Uuid;
//use std::f32::consts::E;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write, BufReader, BufWriter};
use std::path::PathBuf;
//use std::str::pattern::Pattern;
use crate::error::DbIndexError;
use crate::file_size;
use crate::traits::{ByteSized, LoadFrom, ReadFrom, WriteTo};
use header::Header;
use value::{StatusFlag, Data, Value, Gid};

/// BinaryTree version.
pub const VERSION: u32 = 1;

/// Index file extension.
pub const FILE_EXTENSION: &str = "fmbindex";

// /// Default indexing batch size before updating headers.
// const DEFAULT_BATCH_SIZE: u64 = 100;

/// index healthcheck status.
#[derive(Debug, PartialEq)]
pub enum Status {
    New,
    Indexed,
    Incomplete,
    Corrupted,
    // Indexing,
    // WrongInputFile
}

impl Display for Status{
    fn fmt(&self, f: &mut Formatter) -> FmtResult { 
        write!(f, "{}", match self {
            Self::New => "new",
            Self::Indexed => "indexed",
            Self::Incomplete => "incomplete",
            Self::Corrupted => "corrupted",
            //Self::Indexing => "indexing",
            //Self::WrongInputFile => "wrong input file"
        })
    }
}

/// BinaryTree engine.
#[derive(Debug, PartialEq, Clone)]
pub struct BinaryTree {
    /// Index file path.
    pub index_path: PathBuf,

    /// Index header data.
    pub header: Header
}

impl BinaryTree {
    /// Generates a regex expression to validate the index file extension.
    pub fn file_extension_regex() -> Regex {
        let expression = format!(r"(?i)\.{}$", FILE_EXTENSION);
        Regex::new(&expression).unwrap()
    }

    /// Calculate the target value position at the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn calc_value_pos(index: u64) -> u64 {
        Header::BYTES as u64 + index * Value::BYTES as u64
    }

    /// Create a new indexer instance.
    /// 
    /// # Arguments
    /// 
    /// * `table` - Table instance.
    /// * `index_path` - Target index file path.
    pub fn new(index_path: PathBuf, uuid: Option<Uuid>) -> Self {
        let header = Header::new(uuid);
        Self{
            index_path,
            header,
        }
    }

    /// Returns an index file buffered reader.
    pub fn new_index_reader(&self) -> Result<BufReader<File>> {
        let file = File::open(&self.index_path)?;
        Ok(BufReader::new(file))
    }

    /// Returns an index file buffered writer.
    /// 
    /// # Arguments
    /// 
    /// * `create` - Set to `true` when the file should be created.
    pub fn new_index_writer(&self, create: bool) -> Result<BufWriter<File>> {
        let mut options = OpenOptions::new();
        options.write(true);
        if create {
            options.create(true);
        }
        let file = options.open(&self.index_path)?;
        Ok(BufWriter::new(file))
    }

    /// Move to the index header position and then loads it.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    pub fn load_header_from(&mut self, reader: &mut (impl Read + Seek)) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        self.header.load_from(reader)?;
        Ok(())
    }

    /// Move to index position on a reader and returns `true` if a value
    /// exists on that index or would be an append, or `false` if doesn't.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    /// * `force` - Skips indexed file validation when true.
    pub fn seek_value_pos_from(&self, reader: &mut (impl Read + Seek), index: u64, force: bool) -> Result<bool> {
        if !force && !self.header.indexed {
            bail!("input file must be indexed before reading values")
        }
        if self.header.indexed_count > index {
            let pos = Self::calc_value_pos(index);
            reader.seek(SeekFrom::Start(pos))?;
            return Ok(true)
        }
        Ok(false)
    }

    /// Move to index position and then read the index value from a reader.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `index` - Record index.
    /// * `force` - Skips indexed file validation when true.
    pub fn seek_value_from(&self, reader: &mut (impl Read + Seek), index: u64, force: bool) -> Result<Option<Value>> {
        if self.seek_value_pos_from(reader, index, force)? {
            return Ok(Some(Value::read_from(reader)?));
        }
        Ok(None)
    }

    /// Read the index value from the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    pub fn value(&self, index: u64) -> Result<Option<Value>> {
        let mut reader = self.new_index_reader()?;
        return self.seek_value_from(&mut reader, index, false)
    }

    /// Reads a batch of index values from a reader at it's current position
    /// and return a the value list whenever a read value is returned.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `size` - Batch size to read. Use 0 to read all index values from the current reader position.
    /// * `f` - Function to filter batch values with expected result tuple `(value, break_loop)`. Return `None` to exlude a value.
    pub fn scan_from<F>(
        &self,
        reader: &mut (impl Read + Seek),
        size: u64, f: F
    ) -> Result<Vec<Value>>
    where F: Fn(Value) -> Result<(Option<Value>, bool)> {
        let mut list = Vec::new();
        let mut counter = 0;
        while size < 1 || counter < size {
            counter += 1;

            // read and process value
            let value = match Value::read_from(reader) {
                Ok(v) => v,
                Err(e) => match e.downcast::<std::io::Error>() {
                    Ok(err) => match err.kind() {
                        std::io::ErrorKind::UnexpectedEof => return Ok(list),
                        _ => bail!(err)
                    }
                    Err(err) => bail!(err)
                }
            };
            let (value, break_loop) = f(value)?;

            // add the value to list when required
            if let Some(v) = value {
                list.push(v);
            }

            // break loop when required
            if break_loop {
                break;
            }
        }
        Ok(list)
    }

    /// Reads a batch of index values from the index file.
    /// and return a the value list whenever a read value is returned.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    /// * `size` - Batch size to read. Use 0 to read all index values.
    /// * `f` - Function to filter batch values with expected result tuple `(value, break_loop)`. Return `None` to exlude a value.
    pub fn scan<F>(
        &self,
        index: u64,
        size: u64,
        f: F
    ) -> Result<Vec<Value>>
    where F: Fn(Value) -> Result<(Option<Value>, bool)> {
        let mut reader = self.new_index_reader()?;
        if !self.seek_value_pos_from(&mut reader, index, false)? {
            return Ok(Vec::new())
        }
        Ok(self.scan_from(&mut reader, size, f)?)
    }

    /// Process a batch of index values from a reader at it's current position.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - Byte reader.
    /// * `writer` - Byte writer.
    /// * `size` - Batch size to read. Use 0 to process every value from the current reader position.
    /// * `f` - Function to execute for each index value with expected result tuple `(value, break_loop)`. Return a value to update it.
    pub fn process_from<F>(
        &self,
        reader: &mut (impl Read + Seek),
        writer: &mut (impl Write + Seek),
        size: u64,
        f: F
    ) -> Result<()>
    where F: Fn(Value) -> Result<(Option<Value>, bool)> {
        let mut pos = reader.stream_position()?;
        let mut last_write_pos = 0;
        let value_size = Value::BYTES as u64;
        let mut counter = 0;
        while size < 1 || counter < size {
            counter += 1;

            // read and process value
            let value = match Value::read_from(reader) {
                Ok(v) => v,
                Err(e) => match e.downcast::<std::io::Error>() {
                    Ok(err) => match err.kind() {
                        std::io::ErrorKind::UnexpectedEof => return Ok(()),
                        _ => bail!(err)
                    }
                    Err(err) => bail!(err)
                }
            };
            let (value, break_loop) = f(value)?;

            // write value changes when required
            pos += value_size;
            if let Some(v) = value {
                if pos - last_write_pos > value_size {
                    writer.flush()?;
                    writer.seek(SeekFrom::Start(pos - value_size))?;
                }
                last_write_pos = pos;
                v.write_to(writer)?;
            }

            // break loop when required
            if break_loop {
                break;
            }
        }

        // flush last writer changes
        writer.flush()?;
        Ok(())
    }

    /// Process a batch of index values from the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Record index.
    /// * `size` - Batch size to read. Use 0 to process all values from the index provided.
    /// * `f` - Function to execute for each index value with expected result tuple `(value, break_loop)`. Return a value to update it.
    pub fn process<F>(
        &self,
        index: u64,
        size: u64,
        f: F
    ) -> Result<()>
    where F: Fn(Value) -> Result<(Option<Value>, bool)> {
        let mut reader = self.new_index_reader()?;
        let mut writer = self.new_index_writer(false)?;
        if self.seek_value_pos_from(&mut reader, index, false)? {
            let pos = reader.stream_position()?;
            writer.seek(SeekFrom::Start(pos))?;
            self.process_from(&mut reader, &mut writer, size, f)?;
        }
        Ok(())
    }

    /// Updates or append an index value into the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Value index.
    /// * `value` - Index value data to save.
    pub fn save_value(&self, index: u64, value: &Value) -> Result<()> {
        let pos = Self::calc_value_pos(index);
        let mut writer = self.new_index_writer(false)?;
        writer.seek(SeekFrom::Start(pos))?;
        value.write_to(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    /// Updates or append an index value data into the index file.
    /// 
    /// # Arguments
    /// 
    /// * `index` - Value index.
    /// * `data` - Index value data to save.
    pub fn save_data(&self, index: u64, data: &Data) -> Result<()> {
        let pos = Self::calc_value_pos(index) + Value::DATA_OFFSET as u64;
        let mut writer = self.new_index_writer(false)?;
        writer.seek(SeekFrom::Start(pos))?;
        data.write_to(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    /// Perform a healthckeck over the index file by reading
    /// the headers and checking the file size.
    pub fn healthcheck(&mut self) -> Result<Status> {
        // check whenever index file exists
        match self.new_index_reader() {
            // try to load the index headers
            Ok(mut reader) => if let Err(e) = self.load_header_from(&mut reader) {
                match e.downcast::<std::io::Error>() {
                    Ok(ex) => match ex.kind() {
                        std::io::ErrorKind::NotFound => {
                            // File not found so the index is new
                            return Ok(Status::New);
                        }
                        std::io::ErrorKind::UnexpectedEof => {
                            // if the file is empty then is new
                            let real_size = file_size(&self.index_path)?;
                            if real_size < 1 {
                                // return as new index
                                return Ok(Status::New);
                            }

                            // EOF eror means the index is corrupted
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
                        // return as new index
                        return Ok(Status::New)
                    },
                    _ => bail!(ex)
                },
                Err(ex) => bail!(ex)
            }
        };

        // validate corrupted index
        let real_size = file_size(&self.index_path)?;
        let expected_size = Self::calc_value_pos(self.header.indexed_count);
        if self.header.indexed {
            if real_size != expected_size {
                // sizes don't match, the file is corrupted
                return Ok(Status::Corrupted);
            }
        } else {
            if real_size < expected_size {
                // sizes is smaller, the file is corrupted
                return Ok(Status::Corrupted);
            }
            // index is incomplete
            return Ok(Status::Incomplete);
        }

        // all good, the index is indexed
        Ok(Status::Indexed)
    }

    /// Saves the index header and then jump back to the last writer stream position.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    pub fn save_header_into(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.flush()?;
        let old_pos = writer.stream_position()?;
        writer.rewind()?;
        self.header.write_to(writer)?;
        writer.flush()?;
        writer.seek(SeekFrom::Start(old_pos))?;
        Ok(())
    }

    /// Saves the index header and then jump back to the last writer stream position.
    pub fn save_header(&self) -> Result<()> {
        let mut writer = self.new_index_writer(false)?;
        self.save_header_into(&mut writer)
    }

    pub fn insertNewNode_v0 (&mut self, index_nuevo:u64) -> Result<()> {
        let indexPadre = index_nuevo/2;
//        println!("{} {}",indexPadre*2 , index_nuevo);

            // modificar la data del indice nuevo
            let mut value = self.value(index_nuevo).unwrap().unwrap();
            
            value.data.spent_time = value.data.spent_time;
            value.data.parent = indexPadre;
            value.data.left_node = 0;
            value.data.right_node = 0;
            value.data.gid = value.data.gid;
            self.save_value(index_nuevo, &value).unwrap();

            // modificar la data del padre del nuevo indice
            let mut value = self.value(indexPadre).unwrap().unwrap();

            
            value.data.spent_time = value.data.spent_time;
            value.data.parent = value.data.parent;
            if indexPadre*2 == index_nuevo{
                value.data.left_node = index_nuevo;
                value.data.right_node = value.data.right_node;
            } else {
                value.data.left_node = value.data.left_node;
                value.data.right_node = index_nuevo;
            }
            value.data.gid = value.data.gid;
            self.save_value(indexPadre, &value).unwrap();

        Ok(())
    }

    pub fn printIndex(&mut self){
        let mut counter = 0;
    println!("--- {}",self.header.indexed_count);
        for i in 0..self.header.indexed_count {
            counter = i;
            let value = self.value(i).unwrap();
            println!("{0} - {1} - {2} - {3} - {4} - {5} - {6} - {7}",i,self.value(i).unwrap().unwrap().data.height,self.value(i).unwrap().unwrap().data.spent_time,self.value(i).unwrap().unwrap().data.status_flag,self.value(i).unwrap().unwrap().data.parent,self.value(i).unwrap().unwrap().data.left_node,self.value(i).unwrap().unwrap().data.right_node,self.value(i).unwrap().unwrap().data.gid);
        }
    }

    pub fn sortingIndex_v0(&mut self, index_nuevo:u64) -> Result<()> {      
        
        let mut value = self.value(index_nuevo).unwrap().unwrap();
        let nuevospendTime = value.data.spent_time;
        let nuevoStatus = value.data.status_flag;
        let nuevoparent = value.data.parent;
        let nuevoIzquierda = value.data.left_node;
        let nuevoDerecha = value.data.right_node;
        let nuevoGid = value.data.gid;
        
        
        let padre = value.data.parent;
        
        let mut valuePadre = self.value(padre).unwrap().unwrap();
        let padrespendTime = valuePadre.data.spent_time;
        let padreStatus = valuePadre.data.status_flag;
        let padreparent = valuePadre.data.parent;
        let padreIzquierda = valuePadre.data.left_node;
        let padreDerecha = valuePadre.data.right_node;
        let padreGid = valuePadre.data.gid;
        
        
        if nuevoGid.get().lt(padreGid.get()) { //} < padreGid {

            // NuevoHijo
            value.data.status_flag = padreStatus;
            value.data.spent_time = padrespendTime;
            value.data.parent = padre;
            value.data.left_node = nuevoIzquierda;
            value.data.right_node = nuevoDerecha;  
            value.data.gid = padreGid;
            self.save_value(index_nuevo, &value).unwrap();
            
            
            let indexPadre = index_nuevo/2;
                    
            // NuevoPadre
            value.data.status_flag = nuevoStatus;
            value.data.spent_time = nuevospendTime;
            value.data.parent = padreparent;
            if indexPadre*2 == index_nuevo{
                value.data.left_node = index_nuevo;
                value.data.right_node = padreDerecha;
            } else {
                value.data.left_node = padreIzquierda;
                value.data.right_node = index_nuevo;
            }
            value.data.gid = nuevoGid.clone();
            self.save_value(padre, &value).unwrap();

        }

        let mut valueGrandpa = self.value(padreparent).unwrap().unwrap();
        let mut grandpaGid = valueGrandpa.data.gid;

        if nuevoGid.get().lt(grandpaGid.get()) {
            self.sortingIndex_v0(padre);
 
    }

        


        Ok(())
    }


    pub fn insertIndex (&mut self, data: Gid) -> Result<()> {
        

println!("Insert {}",self.header.indexed_count);

        let last_index = self.header.indexed_count;
        let newNodeWasInserted = self.insertNewIndex(1, data, 0, last_index)?;
        Ok(())
    }

    pub fn insertNewIndex (&mut self, i: u64, data: Gid, prev_node: u64, last_index: u64) -> Result<()> {
        println!("insertando");
        let newNodeWasInserted = self.insertNewAVLNode(i, data, prev_node, last_index);
println!("debe rebalancear {}",newNodeWasInserted);
        if newNodeWasInserted{
//self.header.indexed_count += 1;
            let (desbalanceado, height_diff) = self.rebalance(last_index);

            let height = match self.value(last_index)? {               
                Some(v) => v.data.height,
                None => bail!(DbIndexError::NoLeftNode),
            }; //self.value(last_index).unwrap().unwrap().data.height;
            

            let desbalanceado_values =  match self.value(desbalanceado)? {
                    Some(v) => v,
                    None => panic!(" no hay valor")
                }; //i  - 2 -- A



            let mut L = desbalanceado_values.data.left_node;  // B
            let mut R = desbalanceado_values.data.right_node;  // D

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
                let L_v = match self.value(L)? {
                    Some(v) => v,
                    None => panic!(" no hay valor")
                };  // B
                L_H = L_v.data.height;       

                LL = L_v.data.left_node;  //C
                if LL !=0{
                    LL_v = match self.value(LL)? {
                        Some(v) => v,
                        None => panic!(" no hay valor")
                    };  // C
                    LL_H = LL_v.data.height;   
                } else {
                    LL_H = -1;    
                }

                LR = L_v.data.right_node;  //E
                if LR !=0{
                    LR_v = match self.value(LR)? {
                        Some(v) => v,
                        None => panic!(" no hay valor")
                    };  // E
                    LR_H = LR_v.data.height;   
                } else {
                    LR_H = -1;            
                }
            }

            if R != 0{ // exists  D
                let R_v =  match self.value(R)? {
                    Some(v) => v,
                    None => panic!(" no hay valor")
                };  // D
                R_H = R_v.data.height;       

                RR = R_v.data.right_node;  //G
                if RR !=0{
                    RR_v = match self.value(RR)? {
                        Some(v) => v,
                        None => panic!(" no hay valor")
                    };  // G
                    RR_H = RR_v.data.height;
                } else {
                    RR_H = -1;
                }

                RL = R_v.data.left_node;  //F
                if RL !=0{
                    RL_v = match self.value(RL)? {
                        Some(v) => v,
                        None => panic!(" no hay valor")
                    };  // F
                    RL_H = RL_v.data.height;
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

    pub fn insertNewAVLNode (&mut self, i: u64, data: Gid, prev_node: u64, last_index: u64) -> bool{//-> Result<()> {
        //let value = self.value(i).unwrap(); // lo quite al final
println!("data a insertar {}, del nodo {}", data.get(),last_index);
        //let mut index_nuevo=0;
        let mut newNodeWasInserted = false;
        let mut next_node =0;
        let mut parentToStartBalance=0;

        let actual_node = match self.value(i) {
            Ok(opt) => match opt {
                Some(existing_node) => existing_node,
                None => panic!(" no hay valor -"),
            },
            Err(err) => panic!(" no hay valor --{}",err)
        };
        let gid = &actual_node.data.gid;

        let mut should_be_left: bool = false;
        let mut should_be_right: bool = false;
        let mut should_create_node = true;

        if data.get().lt(gid.get()){ //data < gid{
            
            next_node = actual_node.data.left_node;
            //index_nuevo = existing_node.data.left_node;
            should_be_left = true;
        
        } 
        if data.get().gt(gid.get()){ //data > gid{
            next_node = actual_node.data.right_node;
            //index_nuevo = existing_node.data.right_node;
            should_be_right = true;
        } 
        if data.get().eq(gid.get()){
            should_create_node = false;
        } 

        if next_node == 0 { // Is Empty
            if should_be_left || should_be_right{    
println!("Encontro el nodo a actualizar");
                /* If we already have de Index records and we are reindexing
                // New node
                let mut value = self.value(last_index).unwrap().unwrap();
                value.data.status_flag = value.data.status_flag;
                value.data.spent_time = value.data.spent_time;
                value.data.parent = i;
                value.data.left_node = value.data.left_node;
                value.data.right_node = value.data.right_node;
                value.data.height = value.data.height;
                value.data.gid = value.data.gid;
                self.save_value(last_index, &value).unwrap();
                */

                // New node
                let mut value = Value::new();
                value.data.status_flag = StatusFlag::Yes;
                value.data.spent_time = 0;
                value.data.parent = i;
                value.data.left_node = 0;
                value.data.right_node = 0;
                value.data.height = 1;
                value.data.gid = data;
                self.save_value(last_index, &value).unwrap();

                // Actual node
                let mut value = actual_node.clone();

                if should_be_left{
                    value.data.left_node = last_index;
                    value.data.right_node = value.data.right_node;
                    value.data.height = value.data.height;
                    //println!("Inserted left");
                } else {
                    value.data.left_node = value.data.left_node;
                    value.data.right_node = last_index;
                    value.data.height = value.data.height;
                    //println!("Inserted right");
                }
                self.save_value(i, &value).unwrap();

                newNodeWasInserted = true;
                parentToStartBalance = value.data.parent;
                //self.printIndex();                
            }
        } else {
            if should_create_node{
                println!("New iteration id:{} data:{} last id:{} newNodeWasInserted {}",prev_node, data, i,newNodeWasInserted);
                newNodeWasInserted = self.insertNewAVLNode(next_node, data, i,last_index);
            }
        }
println!("{}",newNodeWasInserted);
        return newNodeWasInserted;
        //Ok(())
    }

    pub fn searchKey (&mut self, i: u64, data: Gid) -> Option<u64>{
       
        let mut index_id = 0;
        let mut next_node =0;

        let existing_node = match self.value(i) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => panic!(" no hay valor"),
            },
            Err(err) => panic!(" no hay valor {}",err)
        };

        let gid = existing_node.data.gid;

        let mut keep_searching = true;
println!("data{} git{}",data.get(),gid.get());
        if data.get().lt(gid.get()){//data < gid{
            next_node = existing_node.data.left_node;
          
        } 
        if data.get().gt(gid.get()){//data > gid{
            if i !=0 {
                next_node = existing_node.data.right_node;
            } else {
                next_node = existing_node.data.left_node;
            }
        } 
    println!("3333333");
        if data.get().eq(gid.get()){
            println!("44444-1");
            keep_searching = false;
            index_id = i;
            println!("444444");
        } else {     
            if keep_searching && next_node != 0 {
                println!("5555555");
                println!("{} - {}",keep_searching, next_node);
                index_id = self.searchKey(next_node, data)?;
                println!("6666666666 - {}",index_id);
            } else {
                print!("sssssssssss");
                index_id =0; //return None;
            }
        }
println!("salioooo");

        return Some(index_id);
        //Ok(())
    }

fn rebalance(&self,i:u64) -> (u64, i64){
println!("rebalanceo");
    let mut counter = i;
    let mut last_counter = i;
    let mut height = 0;
    let mut calculated;
    let mut dif=0;

    while (counter != 0) && (dif > -2) && (dif < 2) {
        calculated= self.calc_height(counter);

        height= calculated.0;
        dif =calculated.1;
        
        let mut value = match self.value(counter) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => panic!(" no hay valor"),
            },
            Err(err) => panic!(" no hay valor {}",err)
        };
        value.data.height = height;
        self.save_value(counter, &value).unwrap();

        last_counter = counter;
        //println!("      rebalanced {} parent: {} height: {} dif: {}",counter,value.data.parent,height,dif);
        counter = value.data.parent;

        if (dif < -1) || (dif > 1) || (counter== last_counter) {
            //println!("      Last reviewed: {} D: {} H: {}", last_counter, dif, height);
            counter = 0;
        } 
    }
    return (last_counter,dif);

}

fn calc_height (&self,i:u64) -> (i64, i64){
println!("recalculando height");
    let mut existing_node = match self.value(i) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    let left_node_id = existing_node.data.left_node;
    let right_node_id = existing_node.data.right_node;

    let mut last_left_height = match self.value(left_node_id) {
        Ok(opt) => match opt {
            Some(v) => v.data.height,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    let mut last_right_height = match self.value(right_node_id) {
        Ok(opt) => match opt {
            Some(v) => v.data.height,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };

    let last_left_height= if left_node_id != 0 {last_left_height+1} else{0};
    let last_right_height= if right_node_id != 0 {last_right_height+1} else{0};

    let recalculated_height = i64::max(last_left_height, last_right_height);
    let height_dif = last_left_height- last_right_height;

    return (recalculated_height,height_dif);
}

fn ll_rotation(&self, index_id: u64){
println!("ll");
    let oldparent = index_id;
    let newright = oldparent;
    let oldparent_values = match self.value(oldparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    let newparent = oldparent_values.data.left_node;
    let newparent_right = 0;
    // New parent
    let valuenewparent = match self.value(newparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    let newright_left = valuenewparent.data.right_node;
    let newparent_parent = oldparent_values.data.parent;
    let oldparent_parent = oldparent_values.data.parent;

    
    // Old parent of parent
    let valueoldparent_parent = match self.value(oldparent_parent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New right
    let valuenewright = match self.value(newright) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New left for right
    let valuenewright_left = match self.value(newright_left) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };

    //println!("      LL id: {}",index_id);

    // New parent
    let mut value = valuenewparent.clone();
    value.data.parent = newparent_parent;
    value.data.right_node = oldparent;
    self.save_value(newparent, &value).unwrap();

    // Old parent of parent
    let mut value = valueoldparent_parent.clone();
    if value.data.left_node == oldparent {
        value.data.left_node = newparent;
        value.data.right_node = valueoldparent_parent.data.right_node;
    } else {
        value.data.left_node = valueoldparent_parent.data.left_node;
        value.data.right_node = newparent;
    }
    self.save_value(oldparent_parent, &value).unwrap();


    // New right
    let mut value = valuenewright.clone();
    value.data.parent = newparent;
    value.data.left_node = newright_left;
    value.data.height = valuenewright.data.height - valuenewparent.data.height;

    self.save_value(newright, &value).unwrap();

    // New left for right
    if newright_left !=0 {
        let mut value = valuenewright_left.clone();
        value.data.parent = newright;
        self.save_value(newright_left, &value).unwrap();
    }

    self.rebalance(index_id);

}

fn lr_rotation(&self, index_id: u64){
    let oldparent = index_id;  // A
    let newright = oldparent;  // A
    let valueoldparent= match self.value(oldparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    let oldparent_left = valueoldparent.data.left_node;   // B
    let newparent= match self.value(oldparent_left) {
        Ok(opt) => match opt {
            Some(v) => v.data.right_node,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };  // C

    let newparent_right = oldparent;
    let oldparent_parent = valueoldparent.data.parent;
    let newparent_parent = valueoldparent.data.parent;


    let valuenewparent = match self.value(newparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    let newright_left = valuenewparent.data.right_node; // CR
    let oldleft_right_left = valuenewparent.data.left_node; //CL
    
    // Old parent of parent
    let valueoldparent_parent = match self.value(oldparent_parent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New right
    let valuenewright = match self.value(newright) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New left for right
    let valuenewright_left = match self.value(newright_left) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New right for Old left
    let valueoldleft_right = match self.value(oldleft_right_left) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // Old left
    let valueoldparent_left = match self.value(oldparent_left) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };

    //println!("      LR id: {}",index_id);

    // New parent
    let mut value = valuenewparent.clone();
    value.data.parent = newparent_parent;
    value.data.left_node = oldparent_left;  // B
    value.data.right_node = newright; // A
    self.save_value(newparent, &value).unwrap();


    // Old parent of parent
    let mut value = valueoldparent_parent.clone();
    if value.data.left_node == oldparent {
        value.data.left_node = newparent;
        value.data.right_node = valueoldparent_parent.data.right_node;
    } else {
        value.data.left_node = valueoldparent_parent.data.left_node;
        value.data.right_node = newparent;
    }
    self.save_value(oldparent_parent, &value).unwrap();


    // New right
    let mut value = valuenewright.clone();
    value.data.parent = newparent;
    value.data.left_node = newright_left;
    self.save_value(newright, &value).unwrap();


    // New left for right
    if newright_left !=0 {        
        let mut value = valuenewright_left.clone() ;
        value.data.parent = newright;
        self.save_value(newright_left, &value).unwrap();
    }


    // New right for old left
    if oldleft_right_left !=0 {        
        let mut value = match self.value(oldleft_right_left) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => panic!(" no hay valor"),
            },
            Err(err) => panic!(" no hay valor {}",err)
        };
        value.data.status_flag = valueoldleft_right.data.status_flag;
        value.data.spent_time = valueoldleft_right.data.spent_time;
        value.data.parent = oldparent_left;
        value.data.left_node = valueoldleft_right.data.left_node;
        value.data.right_node = valueoldleft_right.data.right_node;
        value.data.height = valueoldleft_right.data.height;
        value.data.gid = valueoldleft_right.data.gid;
        self.save_value(oldleft_right_left, &value).unwrap();
    }

    // old parent left  // B
    let mut value = valueoldparent_left.clone();
    value.data.parent = newparent;
    value.data.right_node = oldleft_right_left;

    self.save_value(oldparent_left, &value).unwrap();

    let height= self.calc_height(oldparent_left).0;
    
    let mut value = valueoldparent_left;
    value.data.height = height;
    self.save_value(oldparent_left, &value).unwrap();

    self.rebalance(index_id);

}

fn rr_rotation(&self, index_id: u64){
    let oldparent = index_id; // A
    let newleft = oldparent; // A
    let valueoldparent = match self.value(oldparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    let newparent = valueoldparent.data.right_node; // B
    let newparent_left = oldparent; // A
    // New parent
    let valuenewparent = match self.value(newparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    let newleft_right = valuenewparent.data.left_node; // BL
    let newparent_parent = valueoldparent.data.parent; // 0
    let oldparent_parent = valueoldparent.data.parent; // 0 -- 3

    // Old parent of parent
    let valueoldparent_parent = match self.value(oldparent_parent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New left
    let valuenewleft = match self.value(newleft) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New right for left
    let valuenewleft_right = match self.value(newleft_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };

    //println!("      RR id: {}",index_id);

    // New parent
    let mut value = valuenewparent.clone();
    value.data.status_flag = valuenewparent.data.status_flag;
    value.data.spent_time = valuenewparent.data.spent_time;
    value.data.parent = newparent_parent;
    value.data.right_node = valuenewparent.data.right_node;
    value.data.left_node = oldparent;
    value.data.gid = valuenewparent.data.gid;
    self.save_value(newparent, &value).unwrap();


    // Old parent of parent
    let mut value = valueoldparent_parent.clone();
    value.data.status_flag = valueoldparent_parent.data.status_flag;
    value.data.spent_time = valueoldparent_parent.data.spent_time;
    value.data.parent = valueoldparent_parent.data.parent;
    if value.data.right_node == oldparent {
        value.data.right_node = newparent;
        value.data.left_node = valueoldparent_parent.data.left_node;
    } else {
        value.data.right_node = valueoldparent_parent.data.right_node;
        value.data.left_node = newparent;
    }
    value.data.height = valueoldparent_parent.data.height;
    value.data.gid = valueoldparent_parent.data.gid;
    self.save_value(oldparent_parent, &value).unwrap();


    // New left
    let mut value = valuenewleft.clone();
    value.data.status_flag = valuenewleft.data.status_flag;
    value.data.spent_time = valuenewleft.data.spent_time;
    value.data.parent = newparent;
    value.data.right_node = newleft_right;
    value.data.left_node = valuenewleft.data.left_node;
    value.data.height = valuenewleft.data.height - valuenewparent.data.height;
    value.data.gid = valuenewleft.data.gid;
    self.save_value(newleft, &value).unwrap();


    // New right for left
    if newleft_right !=0 {
        let mut value = valuenewleft_right.clone();
        value.data.status_flag = valuenewleft_right.data.status_flag;
        value.data.spent_time = valuenewleft_right.data.spent_time;
        value.data.parent = newleft;
        value.data.right_node = valuenewleft_right.data.right_node;
        value.data.left_node = valuenewleft_right.data.left_node;
        value.data.height = valuenewleft_right.data.height;
        value.data.gid = valuenewleft_right.data.gid;
        self.save_value(newleft_right, &value).unwrap();
    }

    self.rebalance(index_id);

}

fn rl_rotation(&self, index_id: u64){
    let oldparent = index_id;  // A
    let newleft = oldparent;

    let valueoldparent =match self.value(oldparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };

    let oldparent_right = valueoldparent.data.right_node;
    let newparent= match self.value(oldparent_right) {
        Ok(opt) => match opt {
            Some(v) => v.data.left_node,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };

    let newparent_left = oldparent;
    let oldparent_parent = valueoldparent.data.parent;
    let newparent_parent = valueoldparent.data.parent;

     
    let valuenewparent = match self.value(newparent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    let newleft_right = valuenewparent.data.left_node;
    let oldright_left_right = valuenewparent.data.right_node;
   
    // Old parent of parent
    let valueoldparent_parent = match self.value(oldparent_parent) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New left
    let valuenewleft = match self.value(newleft) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New right for left
    let valuenewleft_right = match self.value(newleft) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // New left for Old right
    let valueoldright_left = match self.value(oldright_left_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    // Old right
    let valueoldparent_right = match self.value(oldparent_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };

    //println!("      RL id: {}",index_id);

    // New parent
    let mut value = valuenewparent.clone();
    value.data.status_flag = valuenewparent.data.status_flag;
    value.data.spent_time = valuenewparent.data.spent_time;
    value.data.parent = newparent_parent;
    value.data.right_node = oldparent_right;
    value.data.left_node = newleft;
    value.data.gid = valuenewparent.data.gid;
    value.data.height = valuenewparent.data.height;
    self.save_value(newparent, &value).unwrap();


    // Old parent of parent
    let mut value = valueoldparent_parent.clone();
    value.data.status_flag = valueoldparent_parent.data.status_flag;
    value.data.spent_time = valueoldparent_parent.data.spent_time;
    value.data.parent = valueoldparent_parent.data.parent;
    if value.data.right_node == oldparent {
        value.data.right_node = newparent;
        value.data.left_node = valueoldparent_parent.data.left_node;
    } else {
        value.data.right_node = valueoldparent_parent.data.right_node;
        value.data.left_node = newparent;
    }
    value.data.height = valueoldparent_parent.data.height;
    value.data.gid = valueoldparent_parent.data.gid;
    self.save_value(oldparent_parent, &value).unwrap();


    // New left
    let mut value = valuenewleft.clone();
    value.data.status_flag = valuenewleft.data.status_flag;
    value.data.spent_time = valuenewleft.data.spent_time;
    value.data.parent = newparent;
    value.data.right_node = newleft_right;
    value.data.left_node = valuenewleft.data.left_node;
    value.data.height = valuenewleft.data.height;
    value.data.gid = valuenewleft.data.gid;
    self.save_value(newleft, &value).unwrap();


    // New right for left
    if newleft_right !=0 {
        let mut value = valuenewleft_right.clone();
        value.data.status_flag = valuenewleft_right.data.status_flag;
        value.data.spent_time = valuenewleft_right.data.spent_time;
        value.data.parent = newleft;
        value.data.right_node = valuenewleft_right.data.right_node;
        value.data.left_node = valuenewleft_right.data.left_node;
        value.data.height = valuenewleft_right.data.height;
        value.data.gid = valuenewleft_right.data.gid;
        self.save_value(newleft_right, &value).unwrap();
    }


    // New left for old right
    if oldright_left_right !=0 {
        let mut value = match self.value(oldright_left_right) {
            Ok(opt) => match opt {
                Some(v) => v,
                None => panic!(" no hay valor"),
            },
            Err(err) => panic!(" no hay valor {}",err)
        };
        value.data.status_flag = valueoldright_left.data.status_flag;
        value.data.spent_time = valueoldright_left.data.spent_time;
        value.data.parent = oldparent_right;
        value.data.right_node = valueoldright_left.data.right_node;
        value.data.left_node = valueoldright_left.data.left_node;
        value.data.height = valueoldright_left.data.height;
        value.data.gid = valueoldright_left.data.gid;
        self.save_value(oldright_left_right, &value).unwrap();
    }


    // old parent left  // B
    let mut value = valueoldparent_right.clone();
    value.data.status_flag = valueoldparent_right.data.status_flag;
    value.data.spent_time = valueoldparent_right.data.spent_time;
    value.data.parent = newparent;
    value.data.left_node = oldright_left_right;
    value.data.right_node = valueoldparent_right.data.right_node;
    value.data.height = valueoldparent_right.data.height;
    value.data.gid = valueoldparent_right.data.gid;
    self.save_value(oldparent_right, &value).unwrap();

    let height= self.calc_height(oldparent_right).0;
    
    let mut value = match self.value(oldparent_right) {
        Ok(opt) => match opt {
            Some(v) => v,
            None => panic!(" no hay valor"),
        },
        Err(err) => panic!(" no hay valor {}",err)
    };
    value.data.height = height;
    self.save_value(oldparent_right, &value).unwrap();

    self.rebalance(index_id);

}


}

#[cfg(test)]
pub mod test_helper {
    use super::*;
    use crate::test_helper::*;
    use crate::db::dbindex::header::{HASH_SIZE};
    use crate::db::dbindex::header::test_helper::{random_hash, build_header_bytes};
    use tempfile::TempDir;

    /// Fake records without fields bytes.
    pub const FAKE_VALUES_BYTES: usize = Value::BYTES * 3;

    /// Fake index with fields byte size.
    pub const FAKE_INDEX_BYTES: usize = Header::BYTES + FAKE_VALUES_BYTES;

    /// Fake values only byte slice.
    pub const FAKE_VALUES_BYTE_SLICE: [u8; FAKE_VALUES_BYTES] = [
        // first record
        // start_pos
        0, 0, 0, 0, 0, 0, 0, 50u8,
        // end_pos
        0, 0, 0, 0, 0, 0, 0, 100u8,
        // status flag
        b'Y',
        // spent_time
        0, 0, 0, 0, 0, 0, 0, 150u8,
        // parent  // --> Ale
        0, 0, 0, 0, 0, 0, 0, 150u8,   // --> Ale
        // left_node  // --> Ale
        0, 0, 0, 0, 0, 0, 0, 150u8,   // --> Ale
        // right_node  // --> Ale
        0, 0, 0, 0, 0, 0, 0, 150u8,   // --> Ale
        // height
        0, 0, 0, 0, 0, 0, 0,0,   // --> Ale
        // gid  // --> Ale
        0, 0, 0, 0, 0, 0, 0,0
        ,0, 0, 0, 0, 0, 0, 0,0,
        0, 0, 0, 0, 0, 0, 0,0
        ,0, 0, 0, 0, 0, 0, 0,0,
        0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 130u8,   // --> Ale

        // second record
        // start_pos
        0, 0, 0, 0, 0, 0, 0, 200u8,
        // end_pos
        0, 0, 0, 0, 0, 0, 0, 250u8,
        // status flag
        0,
        // spent_time
        0, 0, 0, 0, 0, 0, 1u8, 44u8,
        // parent  // --> Ale
        0, 0, 0, 0, 0, 0, 0, 170u8,   // --> Ale
        // left_node  // --> Ale
        0, 0, 0, 0, 0, 0, 0, 170u8,   // --> Ale
        // right_node  // --> Ale
        0, 0, 0, 0, 0, 0, 0, 170u8,   // --> Ale
        // height
        0, 0, 0, 0, 0, 0, 0,0,   // --> Ale
        // gid  // --> Ale
        0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0,
                0, 0, 0, 0, 0, 0, 0,0,
                0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 133u8,   // --> Ale

        // third record
        // start_pos
        0, 0, 0, 0, 0, 0, 1u8, 94u8,
        // end_pos
        0, 0, 0, 0, 0, 0, 1u8, 144u8,
        // status flag
        b'S',
        // spent_time
        0, 0, 0, 0, 0, 0, 1u8, 194u8
        // parent  // --> Ale
        ,0, 0, 0, 0, 0, 0, 0, 180u8   // --> Ale
        // left_node  // --> Ale
        ,0, 0, 0, 0, 0, 0, 0, 180u8   // --> Ale
        // right_node  // --> Ale
        ,0, 0, 0, 0, 0, 0, 0, 180u8   // --> Ale
        // height
        ,0, 0, 0, 0, 0, 0, 0,0   // --> Ale
        // gid  // --> Ale
        ,0, 0, 0, 0, 0, 0, 0,0
        ,0, 0, 0, 0, 0, 0, 0,0
        ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0
        ,0, 0, 0, 0, 0, 0, 0, 180u8   // --> Ale
    ];

    /// Create fake records without fields.
    /// 
    /// # Arguments
    /// 
    /// * `records` - Record vector to add records into.
    pub fn fake_values() -> Result<Vec<Value>> {
        let mut values = Vec::new();

        // add first value
        values.push(Value{
            input_start_pos: 50,
            input_end_pos: 100,
            data: Data{
                status_flag: StatusFlag::Yes,
                spent_time: 150
                ,parent: 150 // --> Ale
                ,left_node: 150 // --> Ale
                ,right_node: 150 // --> Ale
                ,gid:Gid::new("130") // --> Ale
                ,height:0 // --> Ale
            }
        });

        // add second value
        values.push(Value{
            input_start_pos: 200,
            input_end_pos: 250,
            data: Data{
                status_flag: StatusFlag::None,
                spent_time: 300
                ,parent: 170 // --> Ale
                ,left_node: 170 // --> Ale
                ,right_node: 170 // --> Ale
                ,gid:Gid::new("133") // --> Ale
                ,height:0 // --> Ale
            }
        });

        // add third value
        values.push(Value{
            input_start_pos: 350,
            input_end_pos: 400,
            data: Data{
                status_flag: StatusFlag::Skip,
                spent_time: 450
                ,parent: 180 // --> Ale
                ,left_node: 180 // --> Ale
                ,right_node: 180 // --> Ale
                ,gid:Gid::new("180") // --> Ale
                ,height:0 // --> Ale
            }
        });

        Ok(values)
    }

    /// Return a fake index file without fields as byte slice.
    pub fn fake_index() -> Result<([u8; FAKE_INDEX_BYTES], u64)> {
        // init buffer
        let mut buf = [0u8; FAKE_INDEX_BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 3245634545244324234u64, InputType::CSV);
        copy_bytes(&mut buf, &index_header_buf, 0)?;
        copy_bytes(&mut buf, &FAKE_VALUES_BYTE_SLICE, Header::BYTES)?;
        Ok((buf, 3))
    }

    /// Returns the fake input content as bytes.
    pub fn fake_input_bytes() -> Vec<u8> {
        "\
            name,size,price,color\n\
            fork,\"1 inch\",12.34,red\n\
            keyboard,medium,23.45,\"black\nwhite\"\n\
            mouse,\"12 cm\",98.76,white\n\
            \"rust book\",500 pages,1,\"orange\"\
        ".as_bytes().to_vec()
    }

    /// Returns the fake input hash value.
    pub fn fake_input_hash() -> [u8; HASH_SIZE] {
        [ 47, 130, 231, 73, 14, 84, 144, 114, 198, 155, 94, 35, 15,
          101, 71, 156, 48, 113, 13, 217, 129, 108, 130, 240, 24, 19,
          159, 141, 205, 59, 71, 227]
    }

    /// Create a fake input file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Input file path.
    pub fn create_fake_input(path: &PathBuf) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&fake_input_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Write a fake index bytes into a writer.
    /// 
    /// # Arguments
    /// 
    /// * `writer` - Byte writer.
    /// * `unprocessed` - If `true` then build all values with StatusFlag::None.
    pub fn write_fake_index(writer: &mut (impl Seek + Write), unprocessed: bool) -> Result<Vec<Value>> {
        let mut values = Vec::new();

        // write index header
        let mut header = Header::new();
        header.indexed = true;
        header.indexed_count = 4;
        header.input_type = InputType::CSV;
        header.hash = Some(fake_input_hash());
        header.write_to(writer)?;
        
        // write first value
        let mut value = Value::new();
        value.input_start_pos = 22;
        value.input_end_pos = 44;
        if !unprocessed {
            value.data.status_flag = StatusFlag::Yes;
            value.data.spent_time = 23;
            value.data.parent = 34;  // --> Ale
            value.data.left_node = 35;
            value.data.right_node = 36;
            value.data.height =0;
            value.data.gid = Gid::new("37");
        }
        value.write_to(writer)?;
        values.push(value);
        
        // write second value
        let mut value = Value::new();
        value.input_start_pos = 46;
        value.input_end_pos = 80;
        if !unprocessed {
            value.data.status_flag = StatusFlag::No;
            value.data.spent_time = 25;
            value.data.parent = 44;  // --> Ale
            value.data.left_node = 45;
            value.data.right_node = 46;
            value.data.height =0;
            value.data.gid = Gid::new("47");
        }
        value.write_to(writer)?;
        values.push(value);
        
        // write third value
        let mut value = Value::new();
        value.input_start_pos = 82;
        value.input_end_pos = 106;
        if !unprocessed {
            value.data.status_flag = StatusFlag::None;
            value.data.spent_time = 30;
            value.data.parent = 54;  // --> Ale
            value.data.left_node = 55;
            value.data.right_node = 56;
            value.data.height =0;
            value.data.gid = Gid::new("57");
        }
        value.write_to(writer)?;
        values.push(value);

        // write fourth value
        let mut value = Value::new();
        value.input_start_pos = 108;
        value.input_end_pos = 139;
        if !unprocessed {
            value.data.status_flag = StatusFlag::Skip;
            value.data.spent_time = 41;
            value.data.parent = 64;  // --> Ale
            value.data.left_node = 65;
            value.data.right_node = 66;
            value.data.height =0;
            value.data.gid = Gid::new("67");
        }
        value.write_to(writer)?;
        values.push(value);

        Ok(values)
    }

    /// Create a fake index file based on the default fake input file.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Index file path.
    /// * `empty` - If `true` then build all values with StatusFlag::None.
    pub fn create_fake_index(path: &PathBuf, unprocessed: bool) -> Result<Vec<Value>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        let values = write_fake_index(&mut writer, unprocessed)?;
        writer.flush()?;

        Ok(values)
    }

    /// Execute a function with both a temp directory and a new BinaryTree.
    /// 
    /// # Arguments
    /// 
    /// * `f` - Function to execute.
    pub fn with_tmpdir_and_indexer(f: &impl Fn(&TempDir, &mut BinaryTree) -> Result<()>) {
        let sub = |dir: &TempDir| -> Result<()> {
            // generate default file names for files
            let input_path = dir.path().join("i.csv");
            let index_path = dir.path().join("i.fmindex");

            // create BinaryTree and execute
            let mut indexer = BinaryTree::new(
                input_path,
                index_path,
                InputType::Unknown
            );

            // execute function
            match f(&dir, &mut indexer) {
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
    use serde_json::Number as JSNumber;
    use std::io::Cursor;
    use std::sync::Mutex;
    use crate::test_helper::*;
    use crate::db::dbindex::header::{HASH_SIZE};
    use crate::db::dbindex::header::test_helper::{random_hash, build_header_bytes};

    #[test]
    fn file_extension_regex() {
        let rx = BinaryTree::file_extension_regex();
        assert!(rx.is_match("hello.fmindex"), "expected to match \"hello.fmindex\" but got false");
        assert!(rx.is_match("/path/to/hello.fmindex"), "expected to match \"/path/to/hello.fmindex\" but got false");
        assert!(!rx.is_match("hello.index"), "expected to not match \"hello.index\" but got true");
    }

    #[test]
    fn new() {
        let mut header = Header::new();
        header.input_type = InputType::JSON;
        let expected = BinaryTree{
            input_path: "my_input.csv".into(),
            index_path: "my_index.fmidx".into(),
            header,
            batch_size: DEFAULT_BATCH_SIZE,
            input_fields: Vec::new()
        };
        let indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::JSON);
        assert_eq!(expected, indexer);
    }

    #[test]
    fn calc_record_pos() {
        assert_eq!(264, BinaryTree::calc_value_pos(2));  // --> Ale assert_eq!(108, BinaryTree::calc_value_pos(2));
    }

    #[test]
    fn load_header_from() {
        // create buffer
        let mut buf = [0u8; Header::BYTES + Header::BYTES];
        let hash_buf = random_hash();
        let index_header_buf = build_header_bytes(true, &hash_buf, true, 5245634545244324234u64, InputType::CSV);
        if let Err(e) = copy_bytes(&mut buf, &index_header_buf, 0) {
            assert!(false, "{:?}", e);
        }
        let mut reader = Cursor::new(buf.to_vec());

        // test load_headers
        let mut indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        if let Err(e) = indexer.load_header_from(&mut reader) {
            assert!(false, "expected success but got error: {:?}", e);
        }

        // check expected index header
        let mut expected = Header::new();
        expected.indexed = true;
        expected.hash = Some(hash_buf);
        expected.indexed_count = 5245634545244324234u64;
        expected.input_type = InputType::CSV;
        assert_eq!(expected, indexer.header);
    }

    #[test]
    fn seek_value_from() {
        // init buffer
        let (buf, record_count) = match fake_index() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());

        // init indexer and expected records
        let mut indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        indexer.header.indexed = true;
        indexer.header.indexed_count = record_count;
        let expected = match fake_values() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test first value
        let value = match indexer.seek_value_from(&mut reader, 0, false) {
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
        assert_eq!(expected[0], value);

        // test second value
        let value = match indexer.seek_value_from(&mut reader, 1, false) {
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
        assert_eq!(expected[1], value);

        // test third value
        let value = match indexer.seek_value_from(&mut reader, 2, false) {
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
        assert_eq!(expected[2], value);
    }

    #[test]
    fn value() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // init buffer
            let (buf, value_count) = match fake_index() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            create_file_with_bytes(&indexer.index_path, &buf)?;

            // init indexer and expected records
            indexer.header.indexed = true;
            indexer.header.indexed_count = value_count;
            let expected = match fake_values() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };

            // test first value
            let value = match indexer.value(0) {
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
            assert_eq!(expected[0], value);

            // test second value
            let value = match indexer.value(1) {
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
            assert_eq!(expected[1], value);

            // test third value
            let value = match indexer.value(2) {
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
            assert_eq!(expected[2], value);

            Ok(())
        });
    }

    #[test]
    fn scan_from_filter() {
        // init buffer
        let (buf, record_count) = match fake_index() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());
        if let Err(e) = reader.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };

        // init indexer and expected records
        let mut indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        indexer.header.indexed = true;
        indexer.header.indexed_count = record_count;
        let all_values = match fake_values() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let expected_filtered = vec![
            all_values[0].clone(),
            all_values[2].clone()
        ];
        let expected_read = all_values;
        
        // filter values
        let read_values = Mutex::<Vec<Value>>::new(Vec::new());
        let filtered = match indexer.scan_from(&mut reader, 0, |value| {
            let mut list = read_values.lock().unwrap();
            (*list).push(value.clone());
            if value.input_end_pos % 100 < 1 {
                return Ok((Some(value), false));
            }
            Ok((None, false))
        }) {
            Ok(list) => list,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test results
        assert_eq!(expected_read, (*read_values.lock().unwrap()));
        assert_eq!(expected_filtered, filtered);
    }

    #[test]
    fn scan_from_size() {
        // init buffer
        let (buf, record_count) = match fake_index() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());
        if let Err(e) = reader.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };

        // init indexer and expected records
        let mut indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        indexer.header.indexed = true;
        indexer.header.indexed_count = record_count;
        let all_values = match fake_values() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        assert!(all_values.len() > 2, "this test requires 3 sample values");
        let expected_filtered = vec![
            all_values[0].clone(),
            all_values[1].clone()
        ];
        
        // filter values
        let filtered = match indexer.scan_from(&mut reader, 2, |value| {
            Ok((Some(value), false))
        }) {
            Ok(list) => list,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test results
        assert_eq!(expected_filtered, filtered);
    }

    #[test]
    fn scan_from_break() {
        // init buffer
        let (buf, record_count) = match fake_index() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());
        if let Err(e) = reader.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };

        // init indexer and expected records
        let mut indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        indexer.header.indexed = true;
        indexer.header.indexed_count = record_count;
        let all_values = match fake_values() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let expected_filtered = vec![
            all_values[0].clone()
        ];
        let expected_read = vec![
            all_values[0].clone()
        ];
        
        // filter values
        let read_values = Mutex::<Vec<Value>>::new(Vec::new());
        let filtered = match indexer.scan_from(&mut reader, 3, |value| {
            let mut list = read_values.lock().unwrap();
            (*list).push(value.clone());
            Ok((Some(value), true))
        }) {
            Ok(list) => list,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // test results
        assert_eq!(expected_read, (*read_values.lock().unwrap()));
        assert_eq!(expected_filtered, filtered);
    }

    #[test]
    fn scan_by_index() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // init buffer
            let (buf, value_count) = match fake_index() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            create_file_with_bytes(&indexer.index_path, &buf)?;

            // init indexer and expected records
            indexer.header.indexed = true;
            indexer.header.indexed_count = value_count;
            let all_values = match fake_values() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            let expected_filtered = vec![
                all_values[2].clone()
            ];
            let expected_read = vec![
                all_values[1].clone(),
                all_values[2].clone()
            ];
            
            // filter values
            let read_values = Mutex::<Vec<Value>>::new(Vec::new());
            let filtered = match indexer.scan(1, 0, |value| {
                let mut list = read_values.lock().unwrap();
                (*list).push(value.clone());
                if value.input_end_pos % 100 < 1 {
                    return Ok((Some(value), false));
                }
                Ok((None, false))
            }) {
                Ok(list) => list,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e);
                }
            };

            // test results
            assert_eq!(expected_read, (*read_values.lock().unwrap()));
            assert_eq!(expected_filtered, filtered);

            Ok(())
        });
    }

    #[test]
    fn process_from_update() {
        // init buffer
        let (buf, record_count) = match fake_index() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());
        let mut writer = Cursor::new(buf.to_vec());
        if let Err(e) = reader.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };

        // init indexer and expected records
        let mut indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        indexer.header.indexed = true;
        indexer.header.indexed_count = record_count;
        let all_values = match fake_values() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut updated_value = Value::new();
        updated_value.data.status_flag = StatusFlag::Yes;
        updated_value.data.spent_time = 123;
        updated_value.data.parent = 180;  // --> Ale
        updated_value.data.left_node = 180;  // --> Ale
        updated_value.data.right_node = 188;  // --> Ale
        updated_value.data.gid = Gid::new("199");  // --> Ale
        updated_value.input_start_pos = 234;
        updated_value.input_end_pos = 345;
        let expected_updated = vec![
            all_values[0].clone(),
            all_values[1].clone(),
            updated_value
        ];
        let limit = all_values.len() as u64;
        let expected_read = all_values;
        
        // filter values
        let read_values = Mutex::<Vec<Value>>::new(Vec::new());
        match indexer.process_from(&mut reader, &mut writer, 0, |mut value| {
            let mut list = read_values.lock().unwrap();
            (*list).push(value.clone());
            if let StatusFlag::Skip = value.data.status_flag {
                value.data.status_flag = StatusFlag::Yes;
                value.data.spent_time = 123;
                value.data.parent = 180;  // --> Ale
                value.data.left_node = 180;  // --> Ale
                value.data.right_node = 188;  // --> Ale
                value.data.gid = Gid::new("199");  // --> Ale
                value.input_start_pos = 234;
                value.input_end_pos = 345;
                return Ok((Some(value), false))
            }
            Ok((None, false))
        }) {
            Ok(list) => list,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // read updated values
        if let Err(e) = writer.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };
        let mut list = Vec::new();
        for i in 0..limit {
            let value = match indexer.seek_value_from(&mut writer, i, false) {
                Ok(v) => match v {
                    Some(w) => w,
                    None => {
                        assert!(false, "expected a value");
                        return
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    return
                }
            };
            list.push(value);
        }

        // test results
        assert_eq!(expected_read, (*read_values.lock().unwrap()));
        assert_eq!(expected_updated, list);
    }

    #[test]
    fn process_from_update_all() {
        // init buffer
        let (buf, record_count) = match fake_index() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());
        let mut writer = Cursor::new(buf.to_vec());
        if let Err(e) = reader.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };

        // init indexer and expected records
        let mut indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        indexer.header.indexed = true;
        indexer.header.indexed_count = record_count;
        let all_values = match fake_values() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut expected_updated = vec![
            all_values[0].clone(),
            all_values[1].clone(),
            all_values[2].clone()
        ];
        expected_updated[0].input_end_pos = 555;
        expected_updated[0].data.status_flag = StatusFlag::None;
        expected_updated[1].input_end_pos = 555;
        expected_updated[1].data.status_flag = StatusFlag::None;
        expected_updated[2].input_end_pos = 555;
        expected_updated[2].data.status_flag = StatusFlag::None;
        let expected_read = all_values;
        
        // filter values
        let read_values = Mutex::<Vec<Value>>::new(Vec::new());
        match indexer.process_from(&mut reader, &mut writer, 0, |mut value| {
            let mut list = read_values.lock().unwrap();
            (*list).push(value.clone());
            value.data.status_flag = StatusFlag::None;
            value.input_end_pos = 555;
            Ok((Some(value), false))
        }) {
            Ok(list) => list,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // read updated values
        if let Err(e) = writer.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };
        let mut list = Vec::new();
        let limit = read_values.lock().unwrap().len() as u64;
        for i in 0..limit {
            let value = match indexer.seek_value_from(&mut writer, i, false) {
                Ok(v) => match v {
                    Some(w) => w,
                    None => {
                        assert!(false, "expected a value");
                        return
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    return
                }
            };
            list.push(value);
        }

        // test results
        assert_eq!(expected_read, (*read_values.lock().unwrap()));
        assert_eq!(expected_updated, list);
    }

    #[test]
    fn process_from_size() {
        // init buffer
        let (buf, record_count) = match fake_index() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());
        let mut writer = Cursor::new(buf.to_vec());
        if let Err(e) = reader.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };

        // init indexer and expected records
        let mut indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        indexer.header.indexed = true;
        indexer.header.indexed_count = record_count;
        let all_values = match fake_values() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut expected_updated = vec![
            all_values[0].clone(),
            all_values[1].clone(),
            all_values[2].clone()
        ];
        expected_updated[0].input_end_pos = 555;
        expected_updated[0].data.status_flag = StatusFlag::None;
        expected_updated[1].input_end_pos = 555;
        expected_updated[1].data.status_flag = StatusFlag::None;
        let expected_read = vec![
            all_values[0].clone(),
            all_values[1].clone()
        ];
        
        // filter values
        let read_values = Mutex::<Vec<Value>>::new(Vec::new());
        match indexer.process_from(&mut reader, &mut writer, 2, |mut value| {
            let mut list = read_values.lock().unwrap();
            (*list).push(value.clone());
            value.data.status_flag = StatusFlag::None;
            value.input_end_pos = 555;
            Ok((Some(value), false))
        }) {
            Ok(list) => list,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // read updated values
        if let Err(e) = writer.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };
        let mut list = Vec::new();
        let limit = all_values.len() as u64;
        for i in 0..limit {
            let value = match indexer.seek_value_from(&mut writer, i, false) {
                Ok(v) => match v {
                    Some(w) => w,
                    None => {
                        assert!(false, "expected a value");
                        return
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    return
                }
            };
            list.push(value);
        }

        // test results
        assert_eq!(expected_read, (*read_values.lock().unwrap()));
        assert_eq!(expected_updated, list);
    }

    #[test]
    fn process_from_break() {
        // init buffer
        let (buf, record_count) = match fake_index() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut reader = Cursor::new(buf.to_vec());
        let mut writer = Cursor::new(buf.to_vec());
        if let Err(e) = reader.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };

        // init indexer and expected records
        let mut indexer = BinaryTree::new("my_input.csv".into(), "my_index.fmidx".into(), InputType::Unknown);
        indexer.header.indexed = true;
        indexer.header.indexed_count = record_count;
        let all_values = match fake_values() {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };
        let mut expected_updated = vec![
            all_values[0].clone(),
            all_values[1].clone(),
            all_values[2].clone()
        ];
        expected_updated[0].input_end_pos = 555;
        expected_updated[0].data.status_flag = StatusFlag::None;
        let expected_read = vec![
            all_values[0].clone(),
        ];
        
        // filter values
        let read_values = Mutex::<Vec<Value>>::new(Vec::new());
        match indexer.process_from(&mut reader, &mut writer, 2, |mut value| {
            let mut list = read_values.lock().unwrap();
            (*list).push(value.clone());
            value.data.status_flag = StatusFlag::None;
            value.input_end_pos = 555;
            Ok((Some(value), true))
        }) {
            Ok(list) => list,
            Err(e) => {
                assert!(false, "{:?}", e);
                return;
            }
        };

        // read updated values
        if let Err(e) = writer.seek(SeekFrom::Start(BinaryTree::calc_value_pos(0))) {
            assert!(false, "{:?}", e);
        };
        let mut list = Vec::new();
        let limit = all_values.len() as u64;
        for i in 0..limit {
            let value = match indexer.seek_value_from(&mut writer, i, false) {
                Ok(v) => match v {
                    Some(w) => w,
                    None => {
                        assert!(false, "expected a value");
                        return
                    }
                },
                Err(e) => {
                    assert!(false, "{:?}", e);
                    return
                }
            };
            list.push(value);
        }

        // test results
        assert_eq!(expected_read, (*read_values.lock().unwrap()));
        assert_eq!(expected_updated, list);
    }

    #[test]
    fn process_index() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // init buffer
            let (buf, value_count) = match fake_index() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            create_file_with_bytes(&indexer.index_path, &buf)?;

            // init indexer and expected records
            indexer.header.indexed = true;
            indexer.header.indexed_count = value_count;
            let all_values = match fake_values() {
                Ok(v) => v,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e)
                }
            };
            let mut expected_updated = vec![
                all_values[0].clone(),
                all_values[1].clone(),
                all_values[2].clone()
            ];
            expected_updated[1].input_end_pos = 555;
            expected_updated[1].data.status_flag = StatusFlag::None;
            expected_updated[2].input_end_pos = 555;
            expected_updated[2].data.status_flag = StatusFlag::None;
            let expected_read = vec![
                all_values[1].clone(),
                all_values[2].clone()
            ];
            
            // filter values
            let read_values = Mutex::<Vec<Value>>::new(Vec::new());
            match indexer.process(1, 0, |mut value| {
                let mut list = read_values.lock().unwrap();
                (*list).push(value.clone());
                value.data.status_flag = StatusFlag::None;
                value.input_end_pos = 555;
                Ok((Some(value), false))
            }) {
                Ok(list) => list,
                Err(e) => {
                    assert!(false, "{:?}", e);
                    bail!(e);
                }
            };

            // read updated values
            let mut list = Vec::new();
            let limit = all_values.len() as u64;
            for i in 0..limit {
                let value = match indexer.value(i) {
                    Ok(v) => match v {
                        Some(w) => w,
                        None => {
                            assert!(false, "expected a value");
                            bail!("expected value");
                        }
                    },
                    Err(e) => {
                        assert!(false, "{:?}", e);
                        bail!(e);
                    }
                };
                list.push(value);
            }

            // test results
            assert_eq!(expected_read, (*read_values.lock().unwrap()));
            assert_eq!(expected_updated, list);

            Ok(())
        });
    }

    #[test]
    fn parse_csv_input() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create input and setup indexer
            create_fake_input(&indexer.input_path)?;
            indexer.input_fields = vec![
                "name".to_string(),
                "size".to_string(),
                "price".to_string(),
                "color".to_string()
            ];
            let value = Value{
                input_start_pos: 46,
                input_end_pos: 80,
                data: Data{
                    spent_time: 0,
                    status_flag: StatusFlag::None
                    ,parent: 0 // --> Ale
                    ,left_node: 0 // --> Ale
                    ,right_node: 0 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            
            // test
            let mut expected = JSMap::new();
            expected.insert("name".to_string(), JSValue::String("keyboard".to_string()));
            expected.insert("size".to_string(), JSValue::String("medium".to_string()));
            expected.insert("price".to_string(), JSValue::Number(JSNumber::from_f64(23.45f64).unwrap()));
            expected.insert("color".to_string(), JSValue::String("black\nwhite".to_string()));
            match indexer.parse_csv_input(&value) {
                Ok(v) => assert_eq!(expected, v),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            Ok(())
        });
    }

    #[test]
    fn parse_csv_input_without_input_fields() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create input
            create_fake_input(&indexer.input_path)?;
            let value = Value{
                input_start_pos: 46,
                input_end_pos: 80,
                data: Data{
                    spent_time: 0,
                    status_flag: StatusFlag::None
                    ,parent: 0 // --> Ale
                    ,left_node: 0 // --> Ale
                    ,right_node: 0 // --> Ale
                    ,gid: Gid::new("") // --> Ale
                    ,height:0 // --> Ale
                }
            };
            
            // test
            let expected = "the input doesn't have any fields";
            match indexer.parse_csv_input(&value) {
                Ok(v) => assert!(false, "expected error but got {:?}", v),
                Err(e) => assert_eq!(expected, e.to_string())
            }

            Ok(())
        });
    }

    #[test]
    fn save_value() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            let mut values = create_fake_index(&indexer.index_path, true)?;
            let pos = BinaryTree::calc_value_pos(2);
            let mut buf = [0u8; Value::BYTES];
            let file = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(file);
            let mut old_bytes_before = vec!(0u8; pos as usize);
            let mut old_bytes_after = [0u8; Value::BYTES];
            reader.read_exact(&mut old_bytes_before)?;
            reader.read_exact(&mut buf)?;
            reader.read_exact(&mut old_bytes_after)?;
            let expected = [
                // start_pos
                0, 0, 0, 0, 0, 0, 0, 82u8,
                // end_pos
                0, 0, 0, 0, 0, 0, 0, 106u8,
                // spent_time
                0, 0, 0, 0, 0, 0, 0, 0,
                // status flag
                0
                // parent  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // left_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // right_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // height
                ,0, 0, 0, 0, 0, 0, 0,0   // --> Ale
                // gid  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
            ];
            assert_eq!(expected, buf);

            // save value and check value
            let expected = [
                // start_pos
                0, 0, 0, 0, 0, 0, 0, 10u8,
                // end_pos
                0, 0, 0, 0, 0, 0, 0, 27u8,
                // spent_time
                0, 0, 0, 0, 0, 0, 0, 93u8,
                // status flag
                b'Y'
                // parent  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 80u8   // --> Ale
                // left_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 10u8   // --> Ale
                // right_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 14u8   // --> Ale
                // height
                ,0, 0, 0, 0, 0, 0, 0,0   // --> Ale
                // gid  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0,2
                ,49, 52, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0
                ,0, 0, 0, 0, 0, 0, 0, 0u8   // --> Ale
                
            ];
            values[2].input_start_pos = 10;
            values[2].input_end_pos = 27;
            values[2].data.status_flag = StatusFlag::Yes;
            values[2].data.spent_time = 93;
            values[2].data.parent = 80;   // --> Ale
            values[2].data.left_node = 10;   // --> Ale
            values[2].data.right_node = 14;   // --> Ale
            values[2].data.gid = Gid::new("14");   // --> Ale
            if let Err(e) = indexer.save_value(2, &values[2]) {
                assert!(false, "expected success but got error: {:?}", e)
            }
            reader.seek(SeekFrom::Start(0))?;
            let mut new_bytes_before = vec!(0u8; pos as usize);
            let mut new_bytes_after = [0u8; Value::BYTES];
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
    fn save_data() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            let mut values = create_fake_index(&indexer.index_path, true)?;
            let pos = BinaryTree::calc_value_pos(2);
            let mut buf = [0u8; Value::BYTES];
            let file = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(file);
            let mut old_bytes_before = vec!(0u8; pos as usize);
            let mut old_bytes_after = [0u8; Value::BYTES];
            reader.read_exact(&mut old_bytes_before)?;
            reader.read_exact(&mut buf)?;
            reader.read_exact(&mut old_bytes_after)?;
            let expected = [
                // start_pos
                0, 0, 0, 0, 0, 0, 0, 82u8,
                // end_pos
                0, 0, 0, 0, 0, 0, 0, 106u8,
                // status flag
                0,
                // spent_time
                0, 0, 0, 0, 0, 0, 0, 0
                
                // parent  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // left_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // right_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
                // height
                ,0, 0, 0, 0, 0, 0, 0,0   // --> Ale
                // gid  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0
                ,0, 0, 0, 0, 0, 0, 0, 0   // --> Ale
            ];
            assert_eq!(expected, buf);

            // save value and check value
            let expected = [
                // start_pos
                0, 0, 0, 0, 0, 0, 0, 82u8,
                // end_pos
                0, 0, 0, 0, 0, 0, 0, 106u8,
                // spent_time
                0, 0, 0, 0, 0, 0, 0, 93u8,
                // status flag
                b'Y'
                // parent  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 88u8   // --> Ale
                // left_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 22u8   // --> Ale
                // right_node  // --> Ale
                ,0, 0, 0, 0, 0, 0, 0, 33u8   // --> Ale
                // height
                ,0, 0, 0, 0, 0, 0, 0,2   // --> Ale
                // gid  // --> Ale
                ,49, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0, 0,0
                ,0, 0, 0, 0, 0, 0
                ,0, 0, 0, 0, 0, 0, 0, 0u8   // --> Ale


            ];
            values[2].data.status_flag = StatusFlag::Yes;
            values[2].data.spent_time = 93;
            values[2].data.parent = 88; // --> Ale
            values[2].data.left_node = 22; // --> Ale
            values[2].data.right_node = 33; // --> Ale
            values[2].data.height = 2; // --> Ale
            values[2].data.gid = Gid::new("1"); // --> Ale
            if let Err(e) = indexer.save_data(2, &values[2].data) {
                assert!(false, "expected success but got error: {:?}", e)
            }
            reader.seek(SeekFrom::Start(0))?;
            let mut new_bytes_before = vec!(0u8; pos as usize);
            let mut new_bytes_after = [0u8; Value::BYTES];
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
    fn find_pending() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index
            let mut values = create_fake_index(&indexer.index_path, false)?;
            indexer.header.indexed = true;
            indexer.header.indexed_count = 4;

            // find existing unmatched from start position
            match indexer.find_pending(0) {
                Ok(opt) => match opt {
                    Some(v) => assert_eq!(2, v),
                    None => assert!(false, "expected 2 but got None")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            // find non-existing unmatched from starting point
            values[2].data.status_flag = StatusFlag::Yes;
            indexer.save_value(2, &values[2])?;
            match indexer.find_pending(3) {
                Ok(opt) => match opt {
                    Some(v) => assert!(false, "expected None but got {:?}", v),
                    None => assert!(true, "")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            Ok(())
        });
    }

    #[test]
    fn find_pending_with_offset() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            create_fake_index(&indexer.index_path, false)?;
            indexer.header.indexed = true;
            indexer.header.indexed_count = 4;

            // find existing unmatched with offset
            match indexer.find_pending(1) {
                Ok(opt) => match opt {
                    Some(v) => assert_eq!(2, v),
                    None => assert!(false, "expected 2 but got None")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            // find non-existing unmatched with offset
            match indexer.find_pending(3) {
                Ok(opt) => match opt {
                    Some(v) => assert!(false, "expected None but got {:?}", v),
                    None => assert!(true, "")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            Ok(())
        });
    }

    #[test]
    fn find_pending_with_non_indexed() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            create_fake_index(&indexer.index_path, false)?;
            indexer.header.indexed_count = 4;

            // find existing unmatched with offset
            match indexer.find_pending(1) {
                Ok(opt) => assert!(false, "expected error but got {:?}", opt),
                Err(e) => match e.downcast::<IndexError<Status>>(){
                    Ok(ex) => match ex {
                        IndexError::Unavailable(status) => match status {
                            Status::Incomplete => {},
                            s => assert!(false, "expected ParseError::Unavailable(Incomplete) but got: {:?}", s)
                        },
                        err => assert!(false, "{:?}", err)
                    },
                    Err(ex) => assert!(false, "{:?}", ex)
                }
            }

            Ok(())
        });
    }

    #[test]
    fn find_pending_with_offset_overflow() {
        with_tmpdir_and_indexer(&|_, indexer| {
            // create index and check original value
            create_fake_index(&indexer.index_path, false)?;
            indexer.header.indexed = true;
            indexer.header.indexed_count = 2;

            // find existing unmatched with offset
            match indexer.find_pending(5) {
                Ok(opt) => match opt {
                    Some(v) => assert!(false, "expected None but got {:?}", v),
                    None => assert!(true, "")
                },
                Err(e) => assert!(false, "{:?}", e)
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_new_index() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            create_fake_input(&indexer.input_path)?;

            // test index status
            let expected = Status::New;
            match indexer.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test fake hash
            let expected = fake_input_hash();
            match indexer.header.hash {
                Some(hash) => assert_eq!(expected, hash),
                None => assert!(false, "expected a hash but got None")
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_new_index_with_empty_file() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            create_fake_input(&indexer.input_path)?;

            // test index status
            indexer.new_index_writer(true)?;
            let expected = Status::New;
            match indexer.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }

            // test fake hash
            let expected = fake_input_hash();
            match indexer.header.hash {
                Some(hash) => assert_eq!(expected, hash),
                None => assert!(false, "expected a hash but got None")
            }

            Ok(())
        });
    }

    #[test]
    fn healthcheck_corrupted_headers() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let buf = [0u8; 5];
            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            let expected = Status::Corrupted;
            match indexer.healthcheck() {
                Ok(status) => assert_eq!(expected , status),
                Err(e) => assert!(false, "expected {:?} but got error: {:?}", expected, e)
            }
            Ok(())
        });
    }

    #[test]
    fn healthcheck_hash_mismatch() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; Header::BYTES];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new();
            header.hash = Some([3u8; HASH_SIZE]);
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::WrongInputFile, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_incomplete_corrupted() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; Header::BYTES+Header::BYTES+5];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new();
            header.indexed_count = 10;
            header.hash = Some(fake_input_hash());
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_incomplete_valid() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; Header::BYTES+Header::BYTES+FAKE_VALUES_BYTES];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new();
            header.indexed_count = 3;
            header.hash = Some(fake_input_hash());
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::Incomplete, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_indexed_corrupted() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let mut buf = [0u8; Header::BYTES+Header::BYTES+5];
            let mut writer = &mut buf as &mut [u8];
            let mut header = Header::new();
            header.indexed = true;
            header.indexed_count = 8;
            header.hash = Some(fake_input_hash());
            header.write_to(&mut writer)?;

            create_file_with_bytes(&indexer.index_path, &buf)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::Corrupted, indexer.healthcheck()?);
            Ok(())
        });
    }
    
    #[test]
    fn healthcheck_indexed_valid() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            create_fake_index(&indexer.index_path, false)?;
            create_fake_input(&indexer.input_path)?;
            assert_eq!(Status::Indexed, indexer.healthcheck()?);
            Ok(())
        });
    }

    #[test]
    fn save_header_into() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            // create index file and read index header data
            create_fake_index(&indexer.index_path, false)?;
            let mut reader = indexer.new_index_reader()?;
            let mut expected = [0u8; Header::BYTES];
            reader.read_exact(&mut expected)?;
            reader.rewind()?;
            indexer.header.load_from(&mut reader)?;

            // test save index header
            let mut buf = [0u8; Header::BYTES];
            let wrt = &mut buf as &mut [u8];
            let mut writer = Cursor::new(wrt);
            if let Err(e) = indexer.save_header_into(&mut writer) {
                assert!(false, "expected success but got error: {:?}", e);
            };
            assert_eq!(expected, buf);
            
            Ok(())
        });
    }

    #[test]
    fn save_header() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            // create index file and read index header data
            create_fake_index(&indexer.index_path, false)?;
            let mut reader = indexer.new_index_reader()?;
            let mut expected = [0u8; Header::BYTES];
            reader.read_exact(&mut expected)?;
            reader.rewind()?;
            indexer.header.load_from(&mut reader)?;

            // test save index header
            assert_eq!(4, indexer.header.indexed_count);
            indexer.header.indexed_count = 5;
            if let Err(e) = indexer.save_header() {
                assert!(false, "expected success but got error: {:?}", e);
            };
            indexer.header.indexed_count = 4;
            assert_eq!(4, indexer.header.indexed_count);
            reader.rewind()?;
            indexer.header.load_from(&mut reader)?;
            assert_eq!(5, indexer.header.indexed_count);
            
            Ok(())
        });
    }

    #[test]
    fn load_input_csv_fields() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let expected = vec![
                "name".to_string(),
                "size".to_string(),
                "price".to_string(),
                "color".to_string()
            ];
            create_fake_input(&indexer.input_path)?;
            if let Err(e) = indexer.load_input_csv_fields() {
                assert!(false, "expected success but got error: {:?}", e)
            }
            assert_eq!(expected, indexer.input_fields);

            Ok(())
        });
    }

    #[test]
    fn load_input_fields_as_csv() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            let expected = vec![
                "name".to_string(),
                "size".to_string(),
                "price".to_string(),
                "color".to_string()
            ];
            create_fake_input(&indexer.input_path)?;
            if let Err(e) = indexer.load_input_csv_fields() {
                assert!(false, "expected success but got error: {:?}", e)
            }
            assert_eq!(expected, indexer.input_fields);
            Ok(())
        });
    }

    #[test]
    fn index_new() {
        with_tmpdir_and_indexer(&|dir, indexer| -> Result<()> {
            create_fake_input(&indexer.input_path)?;
            indexer.header.input_type = InputType::CSV;

            // index input file
            if let Err(e) = indexer.index() {
                assert!(false, "expected success but got error: {:?}", e);
            }

            // create expected index
            let tmp_path = dir.path().join("test.fmindex");
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            write_fake_index(&mut writer, true)?;
            writer.flush()?;

            // read expected index bytes
            let file = File::open(&tmp_path)?;
            let mut reader = BufReader::new(file);
            let mut expected = Vec::new();
            reader.read_to_end(&mut expected)?;
            
            // validate index bytes
            let file = File::open(&indexer.index_path)?;
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf)?;
            assert_eq!(expected, buf);

            // validate input fields
            let expected = vec![
                "name".to_string(),
                "size".to_string(),
                "price".to_string(),
                "color".to_string()
            ];
            assert_eq!(expected, indexer.input_fields);
            
            Ok(())
        });
    }

    #[test]
    fn index_existing() {
        with_tmpdir_and_indexer(&|_, indexer| -> Result<()> {
            create_fake_input(&indexer.input_path)?;
            create_fake_index(&indexer.index_path, true)?;

            // index input file
            if let Err(e) = indexer.index() {
                assert!(false, "expected success but got error: {:?}", e);
            }

            // create expected index
            let mut expected = BinaryTree::new(
                indexer.input_path.clone(),
                indexer.index_path.clone(),
                InputType::CSV
            );
            expected.input_fields.push("name".to_string());
            expected.input_fields.push("size".to_string());
            expected.input_fields.push("price".to_string());
            expected.input_fields.push("color".to_string());
            expected.header.indexed = true;
            expected.header.hash = Some(fake_input_hash());
            expected.header.indexed_count = 4;
            assert_eq!(&mut expected, indexer);
            
            Ok(())
        });
    }
}