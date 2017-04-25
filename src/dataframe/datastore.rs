use std::cmp::max;
use std::collections::HashMap;
use std::hash::Hash;

use dataframe::{DataFrameError};
use dataframe::config::FieldType;


#[derive(Debug)]
pub struct DataStore {
    unsigned: HashMap<String, Vec<u64>>,
    signed: HashMap<String, Vec<i64>>,
    string: HashMap<String, Vec<String>>,
    boolean: HashMap<String, Vec<bool>>,
    float: HashMap<String, Vec<f64>>,
}
fn max_len<K, T>(h: &HashMap<K, Vec<T>>) -> usize where K: Eq + Hash {
    h.values().fold(0, |acc, v| max(acc, v.len()))
}
fn is_hm_homogeneous<K, T>(h: &HashMap<K, Vec<T>>) -> Option<usize> where K: Eq + Hash {
    let mut all_same_len = true;
    let mut target_len = 0;
    let mut first = true;
    for (_, v) in h {
        if first {
            target_len = v.len();
            first = false;
        }
        all_same_len &= v.len() == target_len;
    }
    if all_same_len { Some(target_len) } else { None }
}
fn is_hm_homogeneous_with<K, T>(h: &HashMap<K, Vec<T>>, value: usize) -> Option<usize>
        where K: Eq + Hash {
    is_hm_homogeneous(h).and_then(|x| {
        if x == 0 && value != 0 {
            Some(value)
        } else if (value == 0 && x != 0) || x == value {
            Some(x)
        } else { None }
    })
}
fn insert_value<T>(h: &mut HashMap<String, Vec<T>>, k: String, v: T) {
    if h.contains_key(&k) {
        h.get_mut(&k).unwrap().push(v);
    } else {
        h.insert(k, vec!(v));
    }
}
impl DataStore {
    pub fn empty() -> DataStore {
        DataStore {
            unsigned: HashMap::new(),
            signed: HashMap::new(),
            string: HashMap::new(),
            boolean: HashMap::new(),
            float: HashMap::new(),
        }
    }

    pub fn insert_unsigned(&mut self, field_name: String, value: u64) {
        insert_value(&mut self.unsigned, field_name, value);
    }
    pub fn insert_signed(&mut self, field_name: String, value: i64) {
        insert_value(&mut self.signed, field_name, value);
    }
    pub fn insert_string(&mut self, field_name: String, value: String) {
        insert_value(&mut self.string, field_name, value);
    }
    pub fn insert_boolean(&mut self, field_name: String, value: bool) {
        insert_value(&mut self.boolean, field_name, value);
    }
    pub fn insert_float(&mut self, field_name: String, value: f64) {
        insert_value(&mut self.float, field_name, value);
    }

    pub fn insert(&mut self, field_name: String, field_type: FieldType, value_str: String)
            -> Result<(), DataFrameError> {
        match field_type {
            FieldType::Unsigned => self.insert_unsigned(field_name, try!(value_str.parse())),
            FieldType::Signed   => self.insert_signed(field_name, try!(value_str.parse())),
            FieldType::Str      => self.insert_string(field_name, try!(value_str.parse())),
            FieldType::Bool     => self.insert_boolean(field_name, try!(value_str.parse())),
            FieldType::Float    => self.insert_float(field_name, try!(value_str.parse())),
        }
        Ok(())
    }


    pub fn merge_unsigned(&mut self, field_name: &String, v: Vec<u64>)
            -> Result<(), DataFrameError> {
        match self.unsigned.insert(field_name.clone(), v) {
            Some(_) => { Err(DataFrameError::new(
                &format!("merging field {} clobbered existing field", field_name)[..])) },
            None    => { Ok(()) }
        }
    }
    pub fn merge_signed(&mut self, field_name: &String, v: Vec<i64>)
            -> Result<(), DataFrameError> {
        match self.signed.insert(field_name.clone(), v) {
            Some(_) => { Err(DataFrameError::new(
                &format!("merging field {} clobbered existing field", field_name)[..])) },
            None    => { Ok(()) }
        }
    }
    pub fn merge_string(&mut self, field_name: &String, v: Vec<String>)
            -> Result<(), DataFrameError> {
        match self.string.insert(field_name.clone(), v) {
            Some(_) => { Err(DataFrameError::new(
                &format!("merging field {} clobbered existing field", field_name)[..])) },
            None    => { Ok(()) }
        }
    }
    pub fn merge_boolean(&mut self, field_name: &String, v: Vec<bool>)
            -> Result<(), DataFrameError> {
        match self.boolean.insert(field_name.clone(), v) {
            Some(_) => { Err(DataFrameError::new(
                &format!("merging field {} clobbered existing field", field_name)[..])) },
            None    => { Ok(()) }
        }
    }
    pub fn merge_float(&mut self, field_name: &String, v: Vec<f64>)
            -> Result<(), DataFrameError> {
        match self.float.insert(field_name.clone(), v) {
            Some(_) => { Err(DataFrameError::new(
                &format!("merging field {} clobbered existing field", field_name)[..])) },
            None    => { Ok(()) }
        }
    }

    pub fn merge_field(&mut self, field_name: &String, field_type: &FieldType, src: &DataStore)
            -> Result<(), DataFrameError> {
        Ok(match *field_type {
            FieldType::Unsigned => try!(self.merge_unsigned(field_name,
                try!(src.unsigned.get(field_name)
                .ok_or(format!("unable to merge field_name {}: does not exist", field_name)))
                    .clone())),
            FieldType::Signed   => try!(self.merge_signed(field_name,
                try!(src.signed.get(field_name)
                .ok_or(format!("unable to merge field_name {}: does not exist", field_name)))
                    .clone())),
            FieldType::Str   => try!(self.merge_string(field_name,
                try!(src.string.get(field_name)
                .ok_or(format!("unable to merge field_name {}: does not exist", field_name)))
                    .clone())),
            FieldType::Bool     => try!(self.merge_boolean(field_name,
                try!(src.boolean.get(field_name)
                .ok_or(format!("unable to merge field_name {}: does not exist", field_name)))
                    .clone())),
            FieldType::Float    => try!(self.merge_float(field_name,
                try!(src.float.get(field_name)
                .ok_or(format!("unable to merge field_name {}: does not exist", field_name)))
                    .clone())),
        })
    }

    pub fn get_unsigned_field(&self, field_name: &String) -> Option<&Vec<u64>> {
        self.unsigned.get(field_name)
    }
    pub fn get_signed_field(&self, field_name: &String) -> Option<&Vec<i64>> {
        self.signed.get(field_name)
    }
    pub fn get_string_field(&self, field_name: &String) -> Option<&Vec<String>> {
        self.string.get(field_name)
    }
    pub fn get_boolean_field(&self, field_name: &String) -> Option<&Vec<bool>> {
        self.boolean.get(field_name)
    }
    pub fn get_float_field(&self, field_name: &String) -> Option<&Vec<f64>> {
        self.float.get(field_name)
    }

    pub fn is_homogeneous(&self) -> bool {
        is_hm_homogeneous(&self.unsigned)
            .and_then(|x| is_hm_homogeneous_with(&self.signed, x))
            .and_then(|x| is_hm_homogeneous_with(&self.string, x))
            .and_then(|x| is_hm_homogeneous_with(&self.boolean, x))
            .and_then(|x| is_hm_homogeneous_with(&self.float, x))
            .is_some()
    }
    pub fn nrows(&self) -> usize {
        [max_len(&self.unsigned), max_len(&self.signed), max_len(&self.string),
            max_len(&self.boolean), max_len(&self.float)].iter().fold(0, |acc, l| max(acc, *l))
    }
}
