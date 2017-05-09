use std::io::{Read};
use std::path::{Path};

use csv;

use matrix::Matrix;

use dataframe::datastore::{DataStore, FieldInfo};
use dataframe::config::{self, Config, FieldType};
use dataframe::error::DataFrameError;
use dataframe::transform::{TransformType};

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

        return Ok((config, DataFrame { data: transformed_data }))
    }

    pub fn fieldnames(&self) -> Vec<String> {
        self.data.fields.iter().map(|fi| fi.name.clone()).collect()
    }

    pub fn as_matrix(&self) -> Result<(Vec<String>, Matrix), DataFrameError> {
        if !self.data.is_homogeneous() {
            return Err(DataFrameError::new("DataFrame columns are not same length"))
        }
        let mut fieldnames: Vec<String> = Vec::new();
        let mut data_vec: Vec<f64> = Vec::new();

        for f in &self.data.fields {
            if f.ty == FieldType::Str {
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
                FieldType::Bool     => {
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
    ($field_name:expr, $tf_merge_f:expr, $src:expr, $tf:expr) => {
        try!($tf_merge_f(try!($src.ok_or(
            format!("untransformed field name '{}' not found", $field_name)))
            .iter().map(|v| $tf(v)).collect()
        ));

    }
}

fn transform_data(untransformed_data: &DataStore, config: &Config)
        -> Result<DataStore, DataFrameError> {
    let mut tf_data = DataStore::empty();

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
                                field_name,
                                |tr_src| tf_data.merge_unsigned(transform.dest_name(), tr_src),
                                untransformed_data.get_unsigned_field(&field_name),
                                t
                            )
                        },
                        TransformType::UnsignedToSigned(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_signed(transform.dest_name(), tr_src),
                                untransformed_data.get_unsigned_field(&field_name),
                                t
                            )
                        },
                        TransformType::UnsignedToStr(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_string(transform.dest_name(), tr_src),
                                untransformed_data.get_unsigned_field(&field_name),
                                t
                            )
                        },
                        TransformType::UnsignedToBool(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_boolean(transform.dest_name(), tr_src),
                                untransformed_data.get_unsigned_field(&field_name),
                                t
                            )
                        },
                        TransformType::UnsignedToFloat(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_float(transform.dest_name(), tr_src),
                                untransformed_data.get_unsigned_field(&field_name),
                                t
                            )
                        },

                        TransformType::SignedToUnsigned(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_unsigned(transform.dest_name(), tr_src),
                                untransformed_data.get_signed_field(&field_name),
                                t
                            )
                        },
                        TransformType::SignedToSigned(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_signed(transform.dest_name(), tr_src),
                                untransformed_data.get_signed_field(&field_name),
                                t
                            )
                        },
                        TransformType::SignedToStr(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_string(transform.dest_name(), tr_src),
                                untransformed_data.get_signed_field(&field_name),
                                t
                            )
                        },
                        TransformType::SignedToBool(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_boolean(transform.dest_name(), tr_src),
                                untransformed_data.get_signed_field(&field_name),
                                t
                            )
                        },
                        TransformType::SignedToFloat(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_float(transform.dest_name(), tr_src),
                                untransformed_data.get_signed_field(&field_name),
                                t
                            )
                        },

                        TransformType::StrToUnsigned(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_unsigned(transform.dest_name(), tr_src),
                                untransformed_data.get_string_field(&field_name),
                                t
                            )
                        },
                        TransformType::StrToSigned(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_signed(transform.dest_name(), tr_src),
                                untransformed_data.get_string_field(&field_name),
                                t
                            )
                        },
                        TransformType::StrToStr(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_string(transform.dest_name(), tr_src),
                                untransformed_data.get_string_field(&field_name),
                                t
                            )
                        },
                        TransformType::StrToBool(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_boolean(transform.dest_name(), tr_src),
                                untransformed_data.get_string_field(&field_name),
                                t
                            )
                        },
                        TransformType::StrToFloat(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_float(transform.dest_name(), tr_src),
                                untransformed_data.get_string_field(&field_name),
                                t
                            )
                        },

                        TransformType::BoolToUnsigned(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_unsigned(transform.dest_name(), tr_src),
                                untransformed_data.get_boolean_field(&field_name),
                                t
                            )
                        },
                        TransformType::BoolToSigned(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_signed(transform.dest_name(), tr_src),
                                untransformed_data.get_boolean_field(&field_name),
                                t
                            )
                        },
                        TransformType::BoolToStr(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_string(transform.dest_name(), tr_src),
                                untransformed_data.get_boolean_field(&field_name),
                                t
                            )
                        },
                        TransformType::BoolToBool(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_boolean(transform.dest_name(), tr_src),
                                untransformed_data.get_boolean_field(&field_name),
                                t
                            )
                        },
                        TransformType::BoolToFloat(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_float(transform.dest_name(), tr_src),
                                untransformed_data.get_boolean_field(&field_name),
                                t
                            )
                        },

                        TransformType::FloatToUnsigned(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_unsigned(transform.dest_name(), tr_src),
                                untransformed_data.get_float_field(&field_name),
                                t
                            )
                        },
                        TransformType::FloatToSigned(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_signed(transform.dest_name(), tr_src),
                                untransformed_data.get_float_field(&field_name),
                                t
                            )
                        },
                        TransformType::FloatToStr(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_string(transform.dest_name(), tr_src),
                                untransformed_data.get_float_field(&field_name),
                                t
                            )
                        },
                        TransformType::FloatToBool(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_boolean(transform.dest_name(), tr_src),
                                untransformed_data.get_float_field(&field_name),
                                t
                            )
                        },
                        TransformType::FloatToFloat(ref t) => {
                            transform!(
                                field_name,
                                |tr_src| tf_data.merge_float(transform.dest_name(), tr_src),
                                untransformed_data.get_float_field(&field_name),
                                t
                            )
                        },
                    }
                }
                if keep_source {
                    try!(tf_data.merge_field(&field_name, &source_type,
                        untransformed_data));
                }
            },
            None => {
                try!(tf_data.merge_field(&field_name, &source_type, untransformed_data));
            }
        }
    }
    Ok(tf_data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    macro_rules! test_data_path {
        () => {{
            PathBuf::from(file!()) // current file
                .parent().unwrap() // "dataframe" directory
                .parent().unwrap() // "src" directory
                .parent().unwrap() // etl crate root directory;
                .join("test_data")
        }}
    }

    #[test]
    fn basic_test() {
        let data_dir_pathbuf = test_data_path!();
        let data_file_path = data_dir_pathbuf.join("people.csv");
        let config_file_path = data_dir_pathbuf.join("people.yaml");

        let (config, df) = DataFrame::load(&config_file_path, &data_file_path).unwrap();
        println!("{:#?}", config);
        println!("{:#?}", df.data.fields);
        assert_eq!(df.nrows(), 99);
        let fns = df.fieldnames();
        assert_eq!(fns.len(), 10);
        println!("{:#?}", fns);
    }

    #[test]
    fn matrix_test() {
        let data_dir_pathbuf = test_data_path!();
        let data_file_path = data_dir_pathbuf.join("matrix_test.csv");
        let config_file_path = data_dir_pathbuf.join("matrix_test.yaml");

        let (config, df) = DataFrame::load(&config_file_path, &data_file_path).unwrap();
        println!("{:#?}", config);
        assert_eq!(df.nrows(), 100);

        let (fieldnames, mat) = df.as_matrix().unwrap();
        println!("{:#?}", fieldnames);
        println!("{:#?}", mat);
        assert_eq!(fieldnames.len(), 2);
        assert_eq!(mat.nrows(), 100);
        assert_eq!(mat.ncols(), 2);
    }
}
