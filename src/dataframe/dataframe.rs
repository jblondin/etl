use std::io::{Read};
use std::path::{Path};

use csv;

use matrix::Matrix;

use dataframe::datastore::DataStore;
use dataframe::config::{self, Config, FieldType};
use dataframe::error::DataFrameError;
use dataframe::transform::{TransformType};

#[derive(Debug)]
struct FieldInfo {
    index: usize,
    name: String,
    ty: FieldType,
}
impl FieldInfo {
    fn new(index: usize, name: String, ty: FieldType) -> FieldInfo {
        FieldInfo {
            index: index,
            name: name,
            ty: ty,
        }
    }
}

#[derive(Debug)]
pub struct DataFrame {
    data: DataStore,
}
impl DataFrame {
    pub fn nrows(&self) -> usize {
        self.data.nrows()
    }

    //TODO: remove 'Config' from return (I don't think I need it)
    pub fn load(config_file_path: &Path, data_file_path: &Path)
            -> Result<(Config, DataFrame), DataFrameError> {
        let config = try!(config::load_configfile(config_file_path));

        let mut reader = try!(csv::Reader::from_file(data_file_path))
                .delimiter(b'\t');
        let used_fields = try!(parse_headers(&mut reader, &config));

        let untransformed_data = try!(extract_data(&mut reader, &used_fields));
        let transformed_data = try!(transform_data(&untransformed_data, &config));

        return Ok((config, DataFrame { data: transformed_data } as DataFrame))
    }

    pub fn as_matrix(&self) -> Result<(Vec<String>, Matrix), DataFrameError> {
        if !self.data.is_homogeneous() {
            return Err(DataFrameError::new("DataFrame columns are not same length"))
        }
        let mut fieldnames: Vec<String> = Vec::new();
        let mut data_vec: Vec<f64> = Vec::new();
        let mut ncols = 0;

        // floating point values
        for (k, v) in self.data.float.iter() {
            fieldnames.push(k.clone());
            data_vec.append(&mut v.clone());
            ncols += 1;
        }
        // signed integer values
        for (k, v) in self.data.signed.iter() {
            fieldnames.push(k.clone());
            data_vec.append(&mut v.iter().map(|&s| s as f64).collect());
            ncols += 1;
        }
        // unsigned integer values
        for (k, v) in self.data.unsigned.iter() {
            fieldnames.push(k.clone());
            data_vec.append(&mut v.iter().map(|&u| u as f64).collect());
            ncols += 1;
        }
        // boolean values
        for (k, v) in self.data.boolean.iter() {
            fieldnames.push(k.clone());
            data_vec.append(&mut v.iter().map(|&b| if b { 1.0 } else { 0.0 }).collect());
            ncols += 1;
        }
        Ok((fieldnames, Matrix::from_vec(data_vec, self.data.nrows(), ncols)))
    }
}

fn parse_headers<R>(reader: &mut csv::Reader<R>, config: &Config)
        -> Result<Vec<FieldInfo>, DataFrameError> where R: Read {
    let headers = try!(reader.headers());
    let mut used_fields = vec!();
    for (i, field_name) in headers.iter().enumerate() {
        if config.using_field(field_name) {
            used_fields.push(FieldInfo::new(i, field_name.clone(),
                config.get_source_type(field_name)));
        }
    }
    Ok(used_fields)
}

fn extract_data<R>(reader: &mut csv::Reader<R>, used_fields: &Vec<FieldInfo>)
        -> Result<DataStore, DataFrameError> where R: Read {
    let mut data = DataStore::empty();
    for row in reader.records() {
        let row = try!(row);
        for ref finfo in used_fields {
            try!(data.insert(finfo.name.clone(), finfo.ty,
                try!(row.get(finfo.index).ok_or("field index out of bounds")).clone()));
        }
        if !data.is_homogeneous() {
            return Err(DataFrameError::new("error loading data: inconsistent field lengths"))
        }
    }
    Ok(data)
}

macro_rules! transform {
    ($field_name:expr, $dest_name:expr, $tf_merge_f:expr, $src_f:expr, $tf:expr) => {
        try!($tf_merge_f($dest_name, try!($src_f(&$field_name).ok_or(
            format!("untransformed field name '{}' not found", $field_name)))
            .iter().map(|v| $tf(v)).collect()
        ));

    }
}

fn transform_data(untransformed_data: &DataStore, config: &Config)
        -> Result<DataStore, DataFrameError> {
    let mut transformed_data = DataStore::empty();

    for field_name in config.field_names() {
        let source_type = config.get_source_type(&field_name);
        match config.get_transforms_for_field(&field_name) {
            Some(transforms) => {
                let mut keep_source = false;
                for transform in transforms {
                    keep_source |= transform.keep_source();
                    match transform.trtype {
                        TransformType::UnsignedToUnsigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_unsigned(dname, src),
                                |field| untransformed_data.get_unsigned_field(field),
                                t
                            )
                        },
                        TransformType::UnsignedToSigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_signed(dname, src),
                                |field| untransformed_data.get_unsigned_field(field),
                                t
                            )
                        },
                        TransformType::UnsignedToStr(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_string(dname, src),
                                |field| untransformed_data.get_unsigned_field(field),
                                t
                            )
                        },
                        TransformType::UnsignedToBool(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_boolean(dname, src),
                                |field| untransformed_data.get_unsigned_field(field),
                                t
                            )
                        },
                        TransformType::UnsignedToFloat(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_float(dname, src),
                                |field| untransformed_data.get_unsigned_field(field),
                                t
                            )
                        },

                        TransformType::SignedToUnsigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_unsigned(dname, src),
                                |field| untransformed_data.get_signed_field(field),
                                t
                            )
                        },
                        TransformType::SignedToSigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_signed(dname, src),
                                |field| untransformed_data.get_signed_field(field),
                                t
                            )
                        },
                        TransformType::SignedToStr(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_string(dname, src),
                                |field| untransformed_data.get_signed_field(field),
                                t
                            )
                        },
                        TransformType::SignedToBool(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_boolean(dname, src),
                                |field| untransformed_data.get_signed_field(field),
                                t
                            )
                        },
                        TransformType::SignedToFloat(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_float(dname, src),
                                |field| untransformed_data.get_signed_field(field),
                                t
                            )
                        },

                        TransformType::StrToUnsigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_unsigned(dname, src),
                                |field| untransformed_data.get_string_field(field),
                                t
                            )
                        },
                        TransformType::StrToSigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_signed(dname, src),
                                |field| untransformed_data.get_string_field(field),
                                t
                            )
                        },
                        TransformType::StrToStr(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_string(dname, src),
                                |field| untransformed_data.get_string_field(field),
                                t
                            )
                        },
                        TransformType::StrToBool(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_boolean(dname, src),
                                |field| untransformed_data.get_string_field(field),
                                t
                            )
                        },
                        TransformType::StrToFloat(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_float(dname, src),
                                |field| untransformed_data.get_string_field(field),
                                t
                            )
                        },

                        TransformType::BoolToUnsigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_unsigned(dname, src),
                                |field| untransformed_data.get_boolean_field(field),
                                t
                            )
                        },
                        TransformType::BoolToSigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_signed(dname, src),
                                |field| untransformed_data.get_boolean_field(field),
                                t
                            )
                        },
                        TransformType::BoolToStr(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_string(dname, src),
                                |field| untransformed_data.get_boolean_field(field),
                                t
                            )
                        },
                        TransformType::BoolToBool(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_boolean(dname, src),
                                |field| untransformed_data.get_boolean_field(field),
                                t
                            )
                        },
                        TransformType::BoolToFloat(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_float(dname, src),
                                |field| untransformed_data.get_boolean_field(field),
                                t
                            )
                        },

                        TransformType::FloatToUnsigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_unsigned(dname, src),
                                |field| untransformed_data.get_float_field(field),
                                t
                            )
                        },
                        TransformType::FloatToSigned(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_signed(dname, src),
                                |field| untransformed_data.get_float_field(field),
                                t
                            )
                        },
                        TransformType::FloatToStr(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_string(dname, src),
                                |field| untransformed_data.get_float_field(field),
                                t
                            )
                        },
                        TransformType::FloatToBool(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_boolean(dname, src),
                                |field| untransformed_data.get_float_field(field),
                                t
                            )
                        },
                        TransformType::FloatToFloat(ref t) => {
                            transform!(
                                field_name, transform.dest_name(),
                                |dname, src| transformed_data.merge_float(dname, src),
                                |field| untransformed_data.get_float_field(field),
                                t
                            )
                        },
                    }
                }
                if keep_source {
                    try!(transformed_data.merge_field(&field_name, &source_type,
                        untransformed_data));
                }
            },
            None => {
                try!(transformed_data.merge_field(&field_name, &source_type, untransformed_data));
            }
        }
    }
    Ok(transformed_data)
}
