use std::cmp::max;
use std::collections::HashMap;
use std::hash::Hash;

use errors::*;

use dataframe::config::FieldType;

/// Field information for a field within a data store
#[derive(Debug, Clone)]
pub struct FieldInfo {
    /// Index of the field within the data store
    pub index: usize,
    /// Field name
    pub name: String,
    /// Field type
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

/// Data storage underlying a dataframe. Data is retrievable both by index (of the fields vector)
/// and by field name.
#[derive(Debug)]
pub struct DataStore {
    /// List of fields within the data store
    pub fields: Vec<FieldInfo>,
    /// Map of field names to index of the fields vector
    pub field_map: HashMap<String, usize>,

    /// Storage for unsigned integers
    pub unsigned: HashMap<String, Vec<u64>>,
    /// Storage for signed integers
    pub signed: HashMap<String, Vec<i64>>,
    /// Storage for strings
    pub text: HashMap<String, Vec<String>>,
    /// Storage for booleans
    pub boolean: HashMap<String, Vec<bool>>,
    /// Storage for floating-point numbers
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
    /// Generate and return an empty data store
    pub fn empty() -> DataStore {
        DataStore {
            fields: Vec::new(),
            field_map: HashMap::new(),

            unsigned: HashMap::new(),
            signed: HashMap::new(),
            text: HashMap::new(),
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
    /// Insert an unsigned integer with provided field name
    pub fn insert_unsigned(&mut self, field_name: String, value: u64) {
        self.add_field(field_name.clone(), FieldType::Unsigned);
        insert_value(&mut self.unsigned, field_name, value);
    }
    /// Insert a signed integer with provided field name
    pub fn insert_signed(&mut self, field_name: String, value: i64) {
        self.add_field(field_name.clone(), FieldType::Signed);
        insert_value(&mut self.signed, field_name, value);
    }
    /// Insert a string with provided field name
    pub fn insert_text(&mut self, field_name: String, value: String) {
        self.add_field(field_name.clone(), FieldType::Text);
        insert_value(&mut self.text, field_name, value);
    }
    /// Insert a boolean with provided field name
    pub fn insert_boolean(&mut self, field_name: String, value: bool) {
        self.add_field(field_name.clone(), FieldType::Boolean);
        insert_value(&mut self.boolean, field_name, value);
    }
    /// Insert a floating-point number with provided field name
    pub fn insert_float(&mut self, field_name: String, value: f64) {
        self.add_field(field_name.clone(), FieldType::Float);
        insert_value(&mut self.float, field_name, value);
    }

    /// Insert a value (in unparsed string form) of given field type with specified field name
    pub fn insert(&mut self, field_name: String, field_type: FieldType, value_str: String)
            -> Result<()> {
        match field_type {
            FieldType::Unsigned => self.insert_unsigned(field_name,
                value_str.parse().chain_err(|| "unsigned integer parse error")?),
            FieldType::Signed   => self.insert_signed(field_name,
                value_str.parse().chain_err(|| "signed integer parse error")?),
            FieldType::Text     => self.insert_text(field_name, value_str),
            FieldType::Boolean  => self.insert_boolean(field_name,
                value_str.parse().chain_err(|| "boolean parse error")?),
            FieldType::Float    => self.insert_float(field_name,
                value_str.parse().chain_err(|| "floating point parse error")?),
        }
        Ok(())
    }

    /// Merge unsigned integer vector into data store under specified field name
    pub fn merge_unsigned(&mut self, field_name: &String, v: Vec<u64>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Unsigned);
        match self.unsigned.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }
    /// Merge signed integer vector into data store under specified field name
    pub fn merge_signed(&mut self, field_name: &String, v: Vec<i64>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Signed);
        match self.signed.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }
    /// Merge string vector into data store under specified field name
    pub fn merge_text(&mut self, field_name: &String, v: Vec<String>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Text);
        match self.text.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }
    /// Merge boolean vector into data store under specified field name
    pub fn merge_boolean(&mut self, field_name: &String, v: Vec<bool>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Boolean);
        match self.boolean.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }

    /// Merge floating-point vector into data store under specified field name
    pub fn merge_float(&mut self, field_name: &String, v: Vec<f64>) -> Result<()> {
        self.add_field(field_name.clone(), FieldType::Float);
        match self.float.insert(field_name.clone(), v) {
            Some(_) => { Err(Error::from_kind(ErrorKind::DataFrameError(
                format!("merging field {} clobbered existing field", field_name)))) },
            None    => { Ok(()) }
        }
    }

    /// Merge the fields of a given field type with specified field names from source datastore
    /// into this data store
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
                FieldType::Text     => try!(self.merge_text(field_name,
                    try!(src.text.get(field_name)
                    .ok_or(format!("unable to merge field_name {}: does not exist", field_name)))
                        .clone())),
                FieldType::Boolean  => try!(self.merge_boolean(field_name,
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

    /// Merge single field of the given field type and specified field name from source data store
    /// into this data store
    pub fn merge_field(&mut self, field_name: &String, field_type: &FieldType, src: &DataStore)
            -> Result<()> {
        self.merge_fields(vec![field_name], field_type, src)
    }

    /// Merge an entire source data store into this data store
    pub fn merge(&mut self, other: DataStore) -> Result<()> {
        for field in &other.fields {
            self.merge_field(&field.name, &field.ty, &other)?;
        }
        Ok(())
    }
    /// Retrieve an unsigned integer field
    pub fn get_unsigned_field(&self, field_name: &String) -> Option<&Vec<u64>> {
        self.unsigned.get(field_name)
    }
    /// Retrieve a signed integer field
    pub fn get_signed_field(&self, field_name: &String) -> Option<&Vec<i64>> {
        self.signed.get(field_name)
    }
    /// Retrieve a string field
    pub fn get_text_field(&self, field_name: &String) -> Option<&Vec<String>> {
        self.text.get(field_name)
    }
    /// Retrieve a boolean field
    pub fn get_boolean_field(&self, field_name: &String) -> Option<&Vec<bool>> {
        self.boolean.get(field_name)
    }
    /// Retrieve a floating-point field
    pub fn get_float_field(&self, field_name: &String) -> Option<&Vec<f64>> {
        self.float.get(field_name)
    }

    /// Get the field information struct for a given field name
    pub fn get_fieldinfo(&self, field_name: &String) -> Option<&FieldInfo> {
        self.field_map.get(field_name).and_then(|&index| self.fields.get(index))
    }

    /// Get the list of field information structs for this data store
    pub fn fields(&self) -> Vec<&FieldInfo> {
        self.fields.iter().map(|&ref s| s).collect()
    }
    /// Get the field names in this data store
    pub fn fieldnames(&self) -> Vec<&String> {
        self.fields.iter().map(|ref s| &s.name).collect()
    }

    /// Check if datastore is "homogenous": all columns (regardless of field type) are the same
    /// length
    pub fn is_homogeneous(&self) -> bool {
        is_hm_homogeneous(&self.unsigned)
            .and_then(|x| is_hm_homogeneous_with(&self.signed, x))
            .and_then(|x| is_hm_homogeneous_with(&self.text, x))
            .and_then(|x| is_hm_homogeneous_with(&self.boolean, x))
            .and_then(|x| is_hm_homogeneous_with(&self.float, x))
            .is_some()
    }
    /// Retrieve number of rows for this data store
    pub fn nrows(&self) -> usize {
        [max_len(&self.unsigned), max_len(&self.signed), max_len(&self.text),
            max_len(&self.boolean), max_len(&self.float)].iter().fold(0, |acc, l| max(acc, *l))
    }
}
