use std::io::{Read};
use std::path::{Path};

use csv;

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

macro_rules! trnsfrm {
    () => ()
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
                            try!(transformed_data.merge_unsigned(transform.dest_name(),
                                try!(untransformed_data.get_unsigned_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::UnsignedToSigned(ref t) => {
                            try!(transformed_data.merge_signed(transform.dest_name(),
                                try!(untransformed_data.get_unsigned_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::UnsignedToStr(ref t) => {
                            try!(transformed_data.merge_string(transform.dest_name(),
                                try!(untransformed_data.get_unsigned_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::UnsignedToBool(ref t) => {
                            try!(transformed_data.merge_boolean(transform.dest_name(),
                                try!(untransformed_data.get_unsigned_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::UnsignedToFloat(ref t) => {
                            try!(transformed_data.merge_float(transform.dest_name(),
                                try!(untransformed_data.get_unsigned_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },

                        TransformType::SignedToUnsigned(ref t) => {
                            try!(transformed_data.merge_unsigned(transform.dest_name(),
                                try!(untransformed_data.get_signed_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::SignedToSigned(ref t) => {
                            try!(transformed_data.merge_signed(transform.dest_name(),
                                try!(untransformed_data.get_signed_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::SignedToStr(ref t) => {
                            try!(transformed_data.merge_string(transform.dest_name(),
                                try!(untransformed_data.get_signed_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::SignedToBool(ref t) => {
                            try!(transformed_data.merge_boolean(transform.dest_name(),
                                try!(untransformed_data.get_signed_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::SignedToFloat(ref t) => {
                            try!(transformed_data.merge_float(transform.dest_name(),
                                try!(untransformed_data.get_signed_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },

                        TransformType::StrToUnsigned(ref t) => {
                            try!(transformed_data.merge_unsigned(transform.dest_name(),
                                try!(untransformed_data.get_string_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::StrToSigned(ref t) => {
                            try!(transformed_data.merge_signed(transform.dest_name(),
                                try!(untransformed_data.get_string_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::StrToStr(ref t) => {
                            try!(transformed_data.merge_string(transform.dest_name(),
                                try!(untransformed_data.get_string_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::StrToBool(ref t) => {
                            try!(transformed_data.merge_boolean(transform.dest_name(),
                                try!(untransformed_data.get_string_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::StrToFloat(ref t) => {
                            try!(transformed_data.merge_float(transform.dest_name(),
                                try!(untransformed_data.get_string_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },

                        TransformType::BoolToUnsigned(ref t) => {
                            try!(transformed_data.merge_unsigned(transform.dest_name(),
                                try!(untransformed_data.get_boolean_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::BoolToSigned(ref t) => {
                            try!(transformed_data.merge_signed(transform.dest_name(),
                                try!(untransformed_data.get_boolean_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::BoolToStr(ref t) => {
                            try!(transformed_data.merge_string(transform.dest_name(),
                                try!(untransformed_data.get_boolean_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::BoolToBool(ref t) => {
                            try!(transformed_data.merge_boolean(transform.dest_name(),
                                try!(untransformed_data.get_boolean_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::BoolToFloat(ref t) => {
                            try!(transformed_data.merge_float(transform.dest_name(),
                                try!(untransformed_data.get_boolean_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },

                        TransformType::FloatToUnsigned(ref t) => {
                            try!(transformed_data.merge_unsigned(transform.dest_name(),
                                try!(untransformed_data.get_float_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::FloatToSigned(ref t) => {
                            try!(transformed_data.merge_signed(transform.dest_name(),
                                try!(untransformed_data.get_float_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::FloatToStr(ref t) => {
                            try!(transformed_data.merge_string(transform.dest_name(),
                                try!(untransformed_data.get_float_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::FloatToBool(ref t) => {
                            try!(transformed_data.merge_boolean(transform.dest_name(),
                                try!(untransformed_data.get_float_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
                        },
                        TransformType::FloatToFloat(ref t) => {
                            try!(transformed_data.merge_float(transform.dest_name(),
                                try!(untransformed_data.get_float_field(&field_name).ok_or(
                                format!("untransformed field name '{}' not found", field_name)))
                                .iter().map(|v| t(v)).collect()
                            ));
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
