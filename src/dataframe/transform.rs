use std::f64;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Shl;

use errors::*;

use dataframe::{DataStore, FieldType};
use dataframe::config::{ConvertConfig, MapConfig, ConcatenateConfig, VecOneHotConfig, VecHashConfig,
    NormalizeConfig, ScaleConfig};
use dataframe::convert::convert_field;

pub trait TransformFields {
    fn transform_fields(&self, orig_ds: &DataStore, source_fields: &Vec<String>,
            target_name: &String) -> Result<DataStore>;
}

impl TransformFields for ConvertConfig {
    fn transform_fields(&self, orig_ds: &DataStore, source_fields: &Vec<String>,
            target_name: &String) -> Result<DataStore> {
        if !source_fields.len() == 1 {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: conversion expects only 1 source field".to_string())));
        }
        let source_field = source_fields.first().unwrap();
        let source_finfo = orig_ds.get_fieldinfo(source_field)
            .ok_or(Error::from_kind(ErrorKind::DataConfigError("bad transform call".to_string())))?;

        Ok(convert_field(&source_field, source_finfo.ty, target_name, self.target_type(),
            &orig_ds)?)
    }
}

impl TransformFields for MapConfig {
    fn transform_fields(&self, orig_ds: &DataStore, source_fields: &Vec<String>,
            target_name: &String) -> Result<DataStore> {
        if !source_fields.len() == 1 {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: map expects only 1 source field".to_string())));
        }
        let source_field = source_fields.first().unwrap();
        let source_finfo = orig_ds.get_fieldinfo(source_field)
            .ok_or(Error::from_kind(ErrorKind::DataConfigError("bad transform call".to_string())))?;
        if source_finfo.ty != FieldType::Text {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: map transform only maps between strings".to_string())));
        }

        let mut tf_data = DataStore::empty();
        tf_data.merge_text(target_name, orig_ds.get_text_field(source_field).unwrap().iter()
            .map(|&ref s| self.map.get(s).unwrap_or(&self.default_value).clone()).collect())?;
        Ok(tf_data)
    }
}

impl TransformFields for ConcatenateConfig {
    fn transform_fields(&self, orig_ds: &DataStore, source_fields: &Vec<String>,
            target_name: &String) -> Result<DataStore> {
        if source_fields.is_empty() {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: concatenate expects at least 1 source field".to_string())));
        }

        let mut field_data: Vec<&Vec<String>> = Vec::new();
        let mut nrows = 0;
        for source_field in source_fields {
            // verify that all sources are strings
            let source_finfo = orig_ds.get_fieldinfo(source_field).ok_or(
                Error::from_kind(ErrorKind::DataConfigError("bad transform call".to_string())))?;
            if source_finfo.ty != FieldType::Text {
                return Err(Error::from_kind(ErrorKind::DataConfigError(
                    "transform: concatenate can only concatenate strings".to_string())));
            }

            let field_data_vec = orig_ds.get_text_field(source_field).unwrap();
            nrows = field_data_vec.len();
            field_data.push(field_data_vec);
        }

        let mut tf_data_vec: Vec<String> = Vec::new();
        let sep = self.separator();
        for j in 0..nrows {
            tf_data_vec.push(field_data[0][j].clone() +
                &(1..source_fields.len()).map(|i| field_data[i][j].clone())
                    .fold(String::new(), |acc, s| acc + &sep[..] + &s[..])[..]);
        }

        let mut tf_data = DataStore::empty();
        tf_data.merge_text(target_name, tf_data_vec)?;
        Ok(tf_data)
    }
}

impl TransformFields for VecOneHotConfig {
    fn transform_fields(&self, orig_ds: &DataStore, source_fields: &Vec<String>,
            target_name: &String) -> Result<DataStore> {
        if !source_fields.len() == 1 {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: one-hot vectorization expects only 1 source field".to_string())));
        }

        let source_field = source_fields.first().unwrap();
        let source_finfo = orig_ds.get_fieldinfo(source_field)
            .ok_or(Error::from_kind(ErrorKind::DataConfigError("bad transform call".to_string())))?;
        if source_finfo.ty != FieldType::Text {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: vectorize one-hot transform requires string source values".to_string()
            )));
        }

        let data_vec = orig_ds.get_text_field(source_field).unwrap();
        let mut assignments: HashMap<String, usize> = HashMap::new();
        let mut unique_values: Vec<String> = Vec::new();
        for s in data_vec {
            if !assignments.contains_key(s) {
                assignments.insert(s.clone(), unique_values.len());
                unique_values.push(s.clone());
            }
        }
        let (off_value, on_value) = self.binary_scaling().values();
        let mut onehots: Vec<Vec<f64>> = vec![vec![off_value; data_vec.len()]; unique_values.len()];
        for (i, s) in data_vec.iter().enumerate() {
            onehots[assignments[s]][i] = on_value;
        }

        let mut tf_data = DataStore::empty();
        for (i, val) in unique_values.iter().enumerate() {
            tf_data.merge_float(&(target_name.clone() + &format!("_{}", val)[..]),
                onehots[i].clone())?;
        }
        Ok(tf_data)
    }
}

impl TransformFields for VecHashConfig {
    fn transform_fields(&self, orig_ds: &DataStore, source_fields: &Vec<String>,
            target_name: &String) -> Result<DataStore> {
        if !source_fields.len() == 1 {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: hashing vectorization expects only 1 source field".to_string())));
        }

        let source_field = source_fields.first().unwrap();
        let source_finfo = orig_ds.get_fieldinfo(source_field)
            .ok_or(Error::from_kind(ErrorKind::DataConfigError("bad transform call".to_string())))?;
        if source_finfo.ty != FieldType::Text {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: vectorize hash transform requires string source values".to_string())));
        }

        let data_vec = orig_ds.get_text_field(source_field).unwrap();
        let hash_size = self.hash_size();
        let mut hash_vecs: Vec<Vec<f64>> = vec![vec![0.0; data_vec.len()]; hash_size as usize];
        let midpoint = 1u64.shl(63);

        for (i, s) in data_vec.iter().enumerate() {
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            let h = hasher.finish();
            hash_vecs[(h % hash_size) as usize][i] += if h >= midpoint { 1.0 } else { -1.0 };
        }

        let mut tf_data = DataStore::empty();
        for i in 0..hash_size as usize {
            tf_data.merge_float(&(target_name.clone() + &format!("_{}", i)[..]),
                hash_vecs[i].clone())?;
        }
        Ok(tf_data)
    }
}

fn mean(v: &Vec<f64>) -> f64 {
    v.iter().fold(0.0, |acc, &f| acc + f) / (v.len() as f64)
}
fn variance(v: &Vec<f64>, mu: f64, correction: f64) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    assert!(correction != v.len() as f64);

    let sum_sq = v.iter().fold(0.0, |acc, &f| { let x = f - mu; acc + x * x });
    sum_sq / (v.len() as f64 - correction)
}
fn stdev(v: &Vec<f64>, mu: f64, correction: f64) -> f64 {
    variance(v, mu, correction).sqrt()
}

impl TransformFields for NormalizeConfig {
    fn transform_fields(&self, orig_ds: &DataStore, source_fields: &Vec<String>,
            target_name: &String) -> Result<DataStore> {
        if !source_fields.len() == 1 {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: normalization expects only 1 source field".to_string())));
        }

        let source_field = source_fields.first().unwrap();
        let source_finfo = orig_ds.get_fieldinfo(source_field)
            .ok_or(Error::from_kind(ErrorKind::DataConfigError("bad transform call".to_string())))?;
        if source_finfo.ty != FieldType::Float {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: normalize transform requires floats".to_string())));
        }

        let data_vec = orig_ds.get_float_field(source_field).unwrap();
        let mean = mean(&data_vec);
        let stdev = stdev(&data_vec, mean, self.sample_stdev_correction());

        let mut tf_data = DataStore::empty();
        tf_data.merge_float(target_name, data_vec.iter().map(|&f| (f - mean) / stdev).collect())?;
        Ok(tf_data)
    }
}

impl TransformFields for ScaleConfig {
    fn transform_fields(&self, orig_ds: &DataStore, source_fields: &Vec<String>,
            target_name: &String) -> Result<DataStore> {
        if !source_fields.len() == 1 {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: scaling expects only 1 source field".to_string())));
        }

        let source_field = source_fields.first().unwrap();
        let source_finfo = orig_ds.get_fieldinfo(source_field)
            .ok_or(Error::from_kind(ErrorKind::DataConfigError("bad transform call".to_string())))?;
        if source_finfo.ty != FieldType::Float {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "transform: scale transform requires floats".to_string())));
        }

        let data_vec = orig_ds.get_float_field(source_field).unwrap();
        let data_max = data_vec.iter().fold(f64::NEG_INFINITY, |acc, &f| acc.max(f));
        let data_min = data_vec.iter().fold(f64::INFINITY, |acc, &f| acc.min(f));
        let range = data_max - data_min;

        let mut tf_data = DataStore::empty();
        if self.has_custom_minmax() {
            tf_data.merge_float(target_name, data_vec.iter().map(|&f| {
                    let alpha = (f - data_min) / range;
                    (1.0 - alpha) * self.min_value() + alpha * self.max_value()
            }).collect())?;
        } else {
            tf_data.merge_float(target_name,
                data_vec.iter().map(|&f| (f - data_min) / range).collect())?;
        }
        Ok(tf_data)
    }
}
