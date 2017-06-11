use std::cmp::max;
use std::collections::HashMap;
use std::hash::Hash;

use errors::*;

use dataframe::config::FieldType;

#[derive(Debug, Clone)]
pub struct FieldInfo {
    pub index: usize,
    pub name: String,
    pub ty: FieldType,
}
impl FieldInfo {
    pub fn new(index: usize, name: String, ty: FieldType) -> FieldInfo {
        FieldInfo {
            index: index,
            name: name,
            ty: ty,
        }
    }
}

#[derive(Debug)]
pub struct DataStore {
    // store field list in both ordered (by index) and searchable (by name) form
    pub fields: Vec<FieldInfo>,
    pub field_map: HashMap<String, usize>,

    pub unsigned: HashMap<String, Vec<u64>>,
    pub signed: HashMap<String, Vec<i64>>,
    pub string: HashMap<String, Vec<String>>,
    pub boolean: HashMap<String, Vec<bool>>,
    pub float: HashMap<String, Vec<f64>>,
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
            fields: Vec::new(),
            field_map: HashMap::new(),

            unsigned: HashMap::new(),
            signed: HashMap::new(),
            string: HashMap::new(),
            boolean: HashMap::new(),
            float: HashMap::new(),
        }
    }

    fn add_field(&mut self, field_name: String, field_type: FieldType) {
        if !self.field_map.contains_key(&field_name) {
            let index = self.fields.len();
            self.fields.push(FieldInfo::new(index, field_name.clone(), field_type));
            self.field_map.insert(field_name, index);
        }
    }
    pub fn insert_unsigned(&mut self, field_name: String, value: u64) {
        self.add_field(field_name.clone(), FieldType::Unsigned);
        insert_value(&mut self.unsigned, field_name, value);
    }
    pub fn insert_signed(&mut self, field_name: String, value: i64) {
        self.add_field(field_name.clone(), FieldType::Signed);
        insert_value(&mut self.signed, field_name, value);
    }
    pub fn insert_string(&mut self, field_name: String, value: String) {
        self.add_field(field_name.clone(), FieldType::Str);
        insert_value(&mut self.string, field_name, value);
    }
    pub fn insert_boolean(&mut self, field_name: String, value: bool) {
        self.add_field(field_name.clone(), FieldType::Bool);
        insert_value(&mut self.boolean, field_name, value);
    }
    pub fn insert_float(&mut self, field_name: String, value: f64) {
        self.add_field(field_name.clone(), FieldType::Float);
        insert_value(&mut self.float, field_name, value);
    }

    pub fn insert(&mut self, field_name: String, field_type: FieldType, value_str: String)
            -> Result<()> {
        match field_type {
            FieldType::Unsigned => self.insert_unsigned(field_name,
                value_str.parse().chain_err(|| "unsigned integer parse error")?),
            FieldType::Signed   => self.insert_signed(field_name,
                value_str.parse().chain_err(|| "signed integer parse error")?),
            FieldType::Str      => self.insert_string(field_name, value_str),
            FieldType::Bool     => self.insert_boolean(field_name,
                value_str.parse().chain_err(|| "boolean parse error")?),
            FieldType::Float    => self.insert_float(field_name,
                value_str.parse().chain_err(|| "floating point parse error")?),
        }
        Ok(())
    }


    pub fn merge_unsigned(&mut self, field_name: &String, v: Vec<u64>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Unsigned);
        match self.unsigned.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }
    pub fn merge_signed(&mut self, field_name: &String, v: Vec<i64>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Signed);
        match self.signed.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }
    pub fn merge_string(&mut self, field_name: &String, v: Vec<String>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Str);
        match self.string.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }
    pub fn merge_boolean(&mut self, field_name: &String, v: Vec<bool>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Bool);
        match self.boolean.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }
    pub fn merge_float(&mut self, field_name: &String, v: Vec<f64>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Float);
        match self.float.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }

    pub fn merge_fields(&mut self, field_names: Vec<&String>, field_type: &FieldType,
            src: &DataStore) -> Result<()> {
        for field_name in field_names {
            match *field_type {
                FieldType::Unsigned => try!(self.merge_unsigned(field_name,
                    try!(src.unsigned.get(field_name)
                    .ok_or(format!("unable to merge field_name {}: does not exist", field_name)))
                        .clone())),
                FieldType::Signed   => try!(self.merge_signed(field_name,
                    try!(src.signed.get(field_name)
                    .ok_or(format!("unable to merge field_name {}: does not exist", field_name)))
                        .clone())),
                FieldType::Str      => try!(self.merge_string(field_name,
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
            }
        }
        Ok(())
    }

    pub fn merge_field(&mut self, field_name: &String, field_type: &FieldType, src: &DataStore)
            -> Result<()> {
        self.merge_fields(vec![field_name], field_type, src)
    }

    pub fn merge(&mut self, other: DataStore) -> Result<()> {
        for field in &other.fields {
            self.merge_field(&field.name, &field.ty, &other)?;
        }
        Ok(())
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

    pub fn get_fieldinfo(&self, field_name: &String) -> Option<&FieldInfo> {
        self.field_map.get(field_name).and_then(|&index| self.fields.get(index))
    }

    pub fn fields(&self) -> Vec<&FieldInfo> {
        self.fields.iter().map(|&ref s| s).collect()
    }
    pub fn fieldnames(&self) -> Vec<&String> {
        self.fields.iter().map(|ref s| &s.name).collect()
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
