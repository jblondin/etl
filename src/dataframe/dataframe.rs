use std::borrow::Borrow;
use std::io::{Read};
use std::path::{Path};
use std::collections::HashMap;

use csv;
use encoding::{Encoding, DecoderTrap};
use encoding::all::{ISO_8859_1, WINDOWS_1252};

use matrix::Matrix;

use errors::*;

use dataframe::config::{self, DataConfig, SourceFile, Field, FieldType, Filter};
use dataframe::datastore::DataStore;

/// Primary dataframe structure
#[derive(Debug)]
pub struct DataFrame {
    data: DataStore,
}
impl DataFrame {
    /// Number of rows in dataframe
    pub fn nrows(&self) -> usize {
        self.data.nrows()
    }
    /// Create a new DataConfig and DataFrame from the configuration file specified
    pub fn load(config_file_path: &Path) -> Result<(DataConfig, DataFrame)> {
        let config = config::DataConfig::from_config(config_file_path)?;
        let mut untransformed_data = DataStore::empty();

        for source_file in &config.source_files {
            let data_file_path = Path::new(&source_file.name[..]);

            let mut reader = csv::ReaderBuilder::new()
                .delimiter(source_file.delimiter()?)
                .from_path(data_file_path).chain_err(|| "error reading CSV file")?;
            let used_fields = parse_headers(&mut reader, &source_file)?;
            if used_fields.is_empty() {
                return Err(Error::from_kind(ErrorKind::DataFrameError(
                    format!("error parsing headers for file {}", source_file.name))));
            }
            let unt = extract_data(&mut reader, &used_fields)?;
            untransformed_data.merge(unt)?;
        }
        let (transformed_data, generated_field_names) =
            transform_data(&untransformed_data, &config)?;
        let mut df = DataFrame { data: DataStore::empty() };
        df.merge_datastore(finalize_data(untransformed_data, transformed_data, &config,
            &generated_field_names)?)?;
        Ok((config, df))
    }

    fn merge_datastore(&mut self, other_ds: DataStore) -> Result<()> {
        self.data.merge(other_ds)
    }
    /// Merge dataframe with another dataframe
    pub fn merge(&mut self, other: DataFrame) -> Result<()> {
        self.merge_datastore(other.data)
    }

    /// List of the field names for this dataframe
    pub fn fieldnames(&self) -> Vec<&String> {
        self.data.fieldnames()
    }

    /// Get an unsigned integer field from the dataframe (if exists for given field name)
    pub fn get_unsigned_field<T: ?Sized + Borrow<str>>(&self, field_name: &T) -> Option<&Vec<u64>> {
        self.data.get_unsigned_field(&field_name.borrow().to_string())
    }
    /// Get an signed integer field from the dataframe (if exists for given field name)
    pub fn get_signed_field<T: ?Sized + Borrow<str>>(&self, field_name: &T) -> Option<&Vec<i64>> {
        self.data.get_signed_field(&field_name.borrow().to_string())
    }
    /// Get an string field from the dataframe (if exists for given field name)
    pub fn get_text_field<T: ?Sized + Borrow<str>>(&self, field_name: &T) -> Option<&Vec<String>> {
        self.data.get_text_field(&field_name.borrow().to_string())
    }
    /// Get an boolean field from the dataframe (if exists for given field name)
    pub fn get_boolean_field<T: ?Sized + Borrow<str>>(&self, field_name: &T) -> Option<&Vec<bool>> {
        self.data.get_boolean_field(&field_name.borrow().to_string())
    }
    /// Get an floating point field from the dataframe (if exists for given field name)
    pub fn get_float_field<T: ?Sized + Borrow<str>>(&self, field_name: &T) -> Option<&Vec<f64>> {
        self.data.get_float_field(&field_name.borrow().to_string())
    }

    /// Generate a matrix from the dataframe as well as the field names for the columns of that
    /// matrix. String fields are ignored. Integer and boolean fields are transformed into floating
    /// point numbers.
    pub fn as_matrix(&self) -> Result<(Vec<String>, Matrix)> {
        if !self.data.is_homogeneous() {
            return Err(Error::from_kind(ErrorKind::DataFrameError(
                "DataFrame columns are not same length".to_string())));
        }
        let mut fieldnames: Vec<String> = Vec::new();
        let mut data_vec: Vec<f64> = Vec::new();

        for f in &self.data.fields {
            if f.ty == FieldType::Text {
                // no conversion for string fields
                continue;
            }
            match f.ty {
                FieldType::Unsigned => {
                    data_vec.append(&mut self.data.get_unsigned_field(&f.name)
                        .expect("datastore inconsistent").iter().map(|&u| u as f64).collect());
                },
                FieldType::Signed   => {
                    data_vec.append(&mut self.data.get_signed_field(&f.name)
                        .expect("datastore inconsistent").iter().map(|&s| s as f64).collect());
                },
                FieldType::Boolean  => {
                    data_vec.append(&mut self.data.get_boolean_field(&f.name)
                        .expect("datastore inconsistent").iter()
                        .map(|&b| if b { 1.0 } else { 0.0 }).collect());
                },
                FieldType::Float    => {
                    data_vec.append(&mut self.data.get_float_field(&f.name)
                        .expect("datastore inconsistent").clone());
                },
                _                   => { unreachable!() }
            }
            fieldnames.push(f.name.clone());
        }

        Ok((fieldnames, Matrix::from_vec(data_vec, self.data.nrows(), self.data.fields.len())))
    }

    /// Generate a sub-dataframe consisting of the columns specified
    pub fn sub<T>(&self, cols: Vec<T>) -> Result<DataFrame> where T: Borrow<str> {
        let mut subds = DataStore::empty();
        for field_name in cols {
            let field_name = field_name.borrow().to_string();
            if let Some(fi) = self.data.get_fieldinfo(&field_name) {
                let found = match fi.ty {
                    FieldType::Unsigned => {
                        match self.data.get_unsigned_field(&field_name) {
                            Some(v) => { subds.merge_unsigned(&field_name, v.clone())?; Some(()) },
                            None    => None
                        }
                    },
                    FieldType::Signed => {
                        match self.data.get_signed_field(&field_name) {
                            Some(v) => { subds.merge_signed(&field_name, v.clone())?; Some(()) },
                            None    => None
                        }
                    },
                    FieldType::Text => {
                        match self.data.get_text_field(&field_name) {
                            Some(v) => { subds.merge_text(&field_name, v.clone())?; Some(()) },
                            None    => None
                        }
                    },
                    FieldType::Boolean => {
                        match self.data.get_boolean_field(&field_name) {
                            Some(v) => { subds.merge_boolean(&field_name, v.clone())?; Some(()) },
                            None    => None
                        }
                    },
                    FieldType::Float => {
                        match self.data.get_float_field(&field_name) {
                            Some(v) => { subds.merge_float(&field_name, v.clone())?; Some(()) },
                            None    => None
                        }
                    },
                };
                if found.is_none() {
                    return Err(Error::from_kind(ErrorKind::DataFrameError(
                        "Datastore inconsistent".to_string())));
                }
            } else {
                return Err(Error::from_kind(ErrorKind::DataFrameError(
                    format!("Unknown field name: {}", field_name))));
            }
        }
        Ok(DataFrame { data: subds })
    }
}

fn parse_headers<'a, R>(reader: &mut csv::Reader<R>, source_file: &'a SourceFile)
        -> Result<Vec<FieldSled<'a>>> where R: Read {
    let headers = reader.headers().chain_err(|| "unable to parse CSV headers")?;
    let mut field_sleds = vec!();
    let mut filter_map: HashMap<String, &'a Filter> = HashMap::new();
    if let Some(ref filters) = source_file.filters {
        for filter in filters {
            filter_map.insert(filter.source_field.clone(), &filter);
        }
    }
    for (i, field_name) in headers.iter().enumerate() {
        if let Some(field) = source_file.get_source_field(&field_name.to_string()) {
            field_sleds.push(FieldSled::new(field,
                filter_map.get(&field.source_name).map(|&filter| filter) ,i));
        }
    }
    Ok(field_sleds)
}

#[inline]
fn decode(bytes: &[u8], linenum: usize, fieldnum: usize) -> Result<String> {
    ::std::str::from_utf8(bytes).map(|s| s.to_string()).chain_err(|| "UTF-8 Error:")
        .or_else(|_| -> Result<String> {
            // fallback to ISO-8859-1 encoding
            ISO_8859_1.decode(bytes, DecoderTrap::Strict)
                .map_err(|e| format!("ISO_8859_1 Error: {}", e.into_owned()).into())
        })
        .or_else(|_| -> Result<String> {
            // fallback to WINDOWS-1252 encoding
            WINDOWS_1252.decode(bytes, DecoderTrap::Strict)
                .map_err(|e| format!("WINDOWS_1252 Error: {}", e.into_owned()).into())
        }).chain_err(|| format!("Unable to parse line {}, field {}", linenum, fieldnum))
}

struct FieldSled<'a> {
    field: &'a Field,
    filter: Option<&'a Filter>,
    index: usize,
}
impl<'a> FieldSled<'a> {
    fn new(field: &'a Field, filter: Option<&'a Filter>, index: usize) -> FieldSled<'a> {
        FieldSled {
            field: field,
            filter: filter,
            index: index,
        }
    }
}

fn extract_data<R>(reader: &mut csv::Reader<R>, field_sleds: &Vec<FieldSled>)
        -> Result<DataStore> where R: Read {
    let mut data = DataStore::empty();
    for (rownum, row) in reader.byte_records().enumerate() {
        let record = row.chain_err(|| format!("error reading file line {}", rownum + 2))?;

        // TODO: see if this could be sped up by storing decoded field in HashMap

        // loop through once to check filters
        let mut use_record = true;
        for sled in field_sleds {
            if let Some(ref filter) = sled.filter {
                let decoded_field = decode(record.get(sled.index).ok_or(ErrorKind::DataFrameError(
                        "field index out of bounds".to_string()))?, rownum + 1, sled.index)?;
                if !filter.apply(&decoded_field)? {
                    // move on to next record
                    use_record = false;
                    break;
                }
            }
        }

        // loop through again to store
        if use_record {
            for sled in field_sleds {
                let decoded_field = decode(record.get(sled.index).ok_or(ErrorKind::DataFrameError(
                        "field index out of bounds".to_string()))?, rownum + 1, sled.index)?;

                data.insert(
                    sled.field.target_name().clone(),
                    sled.field.field_type,
                    decoded_field
                ).chain_err(|| "data insertion error")?;
            }
        }
    }
    if !data.is_homogeneous() {
        return Err(Error::from_kind(ErrorKind::DataFrameError(
            "error loading data: inconsistent field lengths".to_string())));
    }
    Ok(data)
}

fn transform_data<'a>(untransformed_data: &DataStore, config: &DataConfig)
        -> Result<(DataStore, Vec<Vec<String>>)> {

    if let Some(ref transforms) = config.transforms {
        let mut tf_data = DataStore::empty();
        let mut generated_field_names: Vec<Vec<String>> = vec![Vec::new(); transforms.len()];

        let mut work: Vec<usize> = Vec::new();
        for i in 0..transforms.len() { work.push(i); }

        while !work.is_empty() {
            let mut more_work: Vec<usize> = Vec::new();
            let mut anything_done_this_loop = false;
            while let Some(index) = work.pop() {
                let transform = &transforms[index];
                if transform.source_exists(untransformed_data) {
                    let transformed_data = transform.transform(untransformed_data)?;
                    generated_field_names[index] = transformed_data.fieldnames()
                        .iter().map(|&s| s.clone()).collect();
                    tf_data.merge_fields(transformed_data.fieldnames(), &transform.target_type(),
                        &transformed_data)?;
                    anything_done_this_loop = true;
                } else if transform.source_exists(&tf_data) {
                    let transformed_data = transform.transform(&tf_data)?;
                    generated_field_names[index] = transformed_data.fieldnames()
                        .iter().map(|&s| s.clone()).collect();
                    tf_data.merge_fields(transformed_data.fieldnames(), &transform.target_type(),
                        &transformed_data)?;
                    anything_done_this_loop = true;
                } else {
                    more_work.push(index);
                }
            }

            if !anything_done_this_loop {
                return Err(Error::from_kind(ErrorKind::DataConfigError(
                    format!("no source exists for following transforms: {}",
                        more_work.iter().fold(String::new(),
                            |acc, &i| acc + &transforms[i].target_name[..] + " ")))));
            }
            work.append(&mut more_work);
        }
        Ok((tf_data, generated_field_names))
    } else {
        Ok((DataStore::empty(), Vec::new()))
    }
}

fn finalize_data(untransformed_data: DataStore, transformed_data: DataStore, config: &DataConfig,
        generated_field_names: &Vec<Vec<String>>) -> Result<DataStore> {
    let mut finalized_data = DataStore::empty();
    for source_file in &config.source_files {
        for field in &source_file.fields {
            if field.add_to_frame() {
                finalized_data.merge_field(field.target_name(), &field.field_type,
                    &untransformed_data)?;
            }
        }
    }
    if let Some(ref transforms) = config.transforms {
        for (i, transform) in transforms.iter().enumerate() {
            if transform.add_to_frame() {
                finalized_data.merge_fields(
                    generated_field_names[i].iter().map(|&ref s| s).collect(),
                    &transform.target_type(), &transformed_data)?;
            }
        }
    }
    Ok(finalized_data)
}
