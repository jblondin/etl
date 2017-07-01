use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde_json;
use toml;

use dataframe::DataStore;
use dataframe::TransformFields;

use errors::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataConfig {
    pub source_files: Vec<SourceFile>,
    pub transforms: Option<Vec<Transform>>,
}

impl DataConfig {
    pub fn from_config(config_file_path: &Path) -> Result<DataConfig> {
        if !config_file_path.exists() {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                "config file does not exist".to_string())));
        }

        enum ConfigType {
            Toml,
            Json
        }
        let config_type = match config_file_path.extension() {
            Some(ext) => {
                match &ext.to_str().ok_or(Error::from_kind(ErrorKind::DataConfigError(
                        "invalid extension".to_string())))?.to_uppercase()[..] {
                    "JSON" => ConfigType::Json,
                    "TOML" => ConfigType::Toml,
                    _                  => {
                        return Err(Error::from_kind(ErrorKind::DataConfigError(
                            "invalid extension".to_string())));
                    }
                }
            }
            None => {
                return Err(Error::from_kind(ErrorKind::DataConfigError(
                    "invalid extension".to_string())));
            }
        };
        let mut f = File::open(config_file_path).chain_err(
            || Error::from_kind(ErrorKind::DataConfigError("unable to open file".to_string())))?;
        let mut s = String::new();
        f.read_to_string(&mut s).chain_err(|| Error::from_kind(ErrorKind::DataConfigError(
            "error reading from file".to_string())))?;
        let mut config: DataConfig = match config_type {
            ConfigType::Toml => toml::from_str(&s).chain_err(|| Error::from_kind(
                ErrorKind::DataConfigError("error parsing file as TOML".to_string())))?,
            ConfigType::Json => serde_json::from_str(&s).chain_err(|| Error::from_kind(
                ErrorKind::DataConfigError("error parsing file as JSON".to_string())))?
        };
        config.fix_paths(&config_file_path)?;
        config.validate()?;
        Ok(config)
    }

    fn fix_paths(&mut self, config_file_path: &Path) -> Result<()> {
        let config_file_dir = config_file_path.parent().ok_or(Error::from_kind(
            ErrorKind::DataConfigError(
                "unable to find parent directory of config file".to_string())))?;
        for source_file in &mut self.source_files {
            let curr_name = source_file.name.clone();
            source_file.name = config_file_dir.join(curr_name).to_str().ok_or(
                Error::from_kind(ErrorKind::DataConfigError(
                    "unable to convert pathname to str".to_string())))?.to_string();
        }
        Ok(())
    }
    fn validate(&self) -> Result<()> {
        for source_file in &self.source_files {
            // check if source_file exists
            if !source_file.path().exists() {
                return Err(Error::from_kind(ErrorKind::DataConfigError(
                    format!("source file does not exist: {}", source_file.name))))
            }

            // verify delimiter
            if let Some(ref delim) = source_file.delimiter {
                if delim.len() != 1 {
                    return Err(Error::from_kind(ErrorKind::DataConfigError(
                        format!("invalid delimiter specification: {}", delim))))
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceFile {
    pub name: String,
    pub delimiter: Option<String>,
    pub fields: Vec<Field>,
    pub filters: Option<Vec<Filter>>,
}

impl SourceFile {
    pub fn path(&self) -> &Path {
        Path::new(&self.name[..])
    }
    pub fn delimiter(&self) -> Result<u8> {
        Ok(match self.delimiter {
            Some(ref delim) => {
                delim.bytes().next().ok_or(Error::from(ErrorKind::DataConfigError(
                    "invalid delimiter specification".to_string())))?
            }
            None => { b',' }
        })
    }
    pub fn get_source_field(&self, s: &String) -> Option<&Field> {
        self.fields.iter().find(|&&ref field| field.source_name == *s)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub source_name: String,
    pub target_name: Option<String>,
    pub field_type: FieldType,
    pub add_to_frame: Option<bool>,
}

impl Field {
    pub fn target_name(&self) -> &String {
        self.target_name.as_ref().unwrap_or(&self.source_name)
    }

    pub fn add_to_frame(&self) -> bool {
        self.add_to_frame.unwrap_or(true)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    Unsigned,
    Signed,
    Text,
    Boolean,
    Float
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    pub source_field: String,
    pub filter: FilterMethod,
}
impl Filter {
    pub fn apply(&self, value_str: &String) -> Result<bool> {
        self.filter.apply(value_str)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum FilterMethod {
    Match(MatchConfig),
    MatchNot(MatchConfig),
    Inequality(InequalityConfig),
}
impl FilterMethod {
    pub fn apply(&self, value_str: &String) -> Result<bool> {
        match *self {
            FilterMethod::Match(ref config) => { config.does_match(value_str) }
            FilterMethod::MatchNot(ref config) => { config.does_match(value_str).map(|b| !b) }
            FilterMethod::Inequality(ref config) => { config.does_satisfy(value_str) }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchConfig {
    text: Option<String>,
    signed: Option<i64>,
    unsigned: Option<u64>,
    boolean: Option<bool>,
    float: Option<f64>,
}
impl MatchConfig {
    pub fn does_match(&self, value_str: &String) -> Result<bool> {
        Ok(if let Some(ref s) = self.text {
            s == value_str
        } else if let Some(i) = self.signed {
            i == value_str.parse::<i64>().chain_err(|| "signed integer parse error")?
        } else if let Some(u) = self.unsigned {
            u == value_str.parse::<u64>().chain_err(|| "unsigned integer parse error")?
        } else if let Some(b) = self.boolean {
            b == value_str.parse::<bool>().chain_err(|| "boolean parse error")?
        } else if let Some(f) = self.float {
            f == value_str.parse::<f64>().chain_err(|| "float parse error")?
        } else {
            return Err(ErrorKind::DataConfigError("missing match value".to_string()).into());
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InequalityConfig {
    inequality: InequalityMethod,

    signed: Option<i64>,
    unsigned: Option<u64>,
    float: Option<f64>,

}
impl InequalityConfig {
    pub fn does_satisfy(&self, value_str: &String) -> Result<bool> {
        Ok(if let Some(i) = self.signed {
            self.inequality.does_satisfy(
                value_str.parse::<i64>().chain_err(|| "signed integer parse error")?, i)
        } else if let Some(u) = self.unsigned {
            self.inequality.does_satisfy(
                value_str.parse::<u64>().chain_err(|| "unsigned integer parse error")?, u)
        } else if let Some(f) = self.float {
            self.inequality.does_satisfy(
                value_str.parse::<f64>().chain_err(|| "float parse error")?, f)
        } else {
            return Err(ErrorKind::DataConfigError("missing inequality value".to_string()).into());
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum InequalityMethod {
    Gt,
    Gte,
    Lt,
    Lte
}
impl InequalityMethod {
    pub fn does_satisfy<T: PartialOrd> (&self, value: T, target: T) -> bool {
        match *self {
            InequalityMethod::Gt => value > target,
            InequalityMethod::Gte => value >= target,
            InequalityMethod::Lt => value < target,
            InequalityMethod::Lte => value <= target,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transform {
    pub method: TransformMethod,
    pub source_fields: Vec<String>,
    pub target_name: String,
    pub add_to_frame: Option<bool>,
}

impl Transform {
    pub fn add_to_frame(&self) -> bool {
        self.add_to_frame.unwrap_or(true)
    }
    pub fn target_type(&self) -> FieldType {
        self.method.target_type()
    }
    pub fn source_exists(&self, ds: &DataStore) -> bool {
        match check_transform_source(&self.source_fields, ds) {
            Ok(_)  => true,
            Err(_) => false
        }
    }
    pub fn transform(&self, original: &DataStore) -> Result<DataStore> {
        check_transform_source(&self.source_fields, original)?;
        self.method.transform(original, &self.source_fields, &self.target_name)
    }
}

fn check_transform_source(source_fields: &Vec<String>, ds: &DataStore) -> Result<()> {
    // check of source field exists in data store
    for source_field in source_fields {
        if !ds.field_map.contains_key(source_field) {
            return Err(Error::from_kind(ErrorKind::DataConfigError(
                format!("transform refers to missing field '{}'", source_field))));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum TransformMethod {
    Convert(ConvertConfig),
    Map(MapConfig),
    Concatenate(ConcatenateConfig),
    VectorizeOneHot(VecOneHotConfig),
    VectorizeHash(VecHashConfig),
    Normalize(NormalizeConfig),
    Scale(ScaleConfig),
}

impl TransformMethod {
    pub fn target_type(&self) -> FieldType {
        match *self {
            TransformMethod::Convert(ref config)    => { config.target_type() }
            TransformMethod::Map(_)                 => { FieldType::Text }
            TransformMethod::Concatenate(_)         => { FieldType::Text }
            TransformMethod::VectorizeOneHot(_)     => { FieldType::Float }
            TransformMethod::VectorizeHash(_)       => { FieldType::Float }
            TransformMethod::Normalize(_)           => { FieldType::Float }
            TransformMethod::Scale(_)               => { FieldType::Float }
        }
    }
    pub fn transform(&self, orig_ds: &DataStore, sfs: &Vec<String>, tn: &String)
            -> Result<DataStore> {
        match *self {
            TransformMethod::Convert(ref config)         => {
                config.transform_fields(orig_ds, sfs, tn)
            }
            TransformMethod::Map(ref config)             => {
                config.transform_fields(orig_ds, sfs, tn)
            }
            TransformMethod::Concatenate(ref config)     => {
                config.transform_fields(orig_ds, sfs, tn)
            }
            TransformMethod::VectorizeOneHot(ref config) => {
                config.transform_fields(orig_ds, sfs, tn)
            }
            TransformMethod::VectorizeHash(ref config)   => {
                config.transform_fields(orig_ds, sfs, tn)
            }
            TransformMethod::Normalize(ref config)       => {
                config.transform_fields(orig_ds, sfs, tn)
            }
            TransformMethod::Scale(ref config)           => {
                config.transform_fields(orig_ds, sfs, tn)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConvertConfig {
    target_type: FieldType,
}

impl ConvertConfig {
    pub fn target_type(&self) -> FieldType {
        self.target_type
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MapConfig {
    pub default_value: String,
    pub map: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConcatenateConfig {
    pub separator: Option<String>,
}

impl ConcatenateConfig {
    pub fn separator(&self) -> String {
        self.separator.clone().unwrap_or(String::new())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryScaling {
    ZeroOne,
    NegOneOne,
}
impl BinaryScaling {
    pub fn off_value(&self) -> f64 {
        match *self {
            BinaryScaling::ZeroOne   =>  0.0,
            BinaryScaling::NegOneOne => -1.0,
        }
    }
    pub fn on_value(&self) -> f64 {
        1.0
    }
    pub fn values(&self) -> (f64, f64) {
        (self.off_value(), self.on_value())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VecOneHotConfig {
    pub binary_scaling: Option<BinaryScaling>,
}

impl VecOneHotConfig {
    pub fn binary_scaling(&self) -> BinaryScaling {
        self.binary_scaling.unwrap_or(BinaryScaling::ZeroOne)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VecHashConfig {
    pub hash_size: Option<u64>,
}

impl VecHashConfig {
    pub fn hash_size(&self) -> u64 {
        self.hash_size.unwrap_or(2u64.pow(18))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizeConfig {
    // correction for sample stdev calculation, subtracted from N
    // 0.0 for uncorrected, 1.0 for corrected, 1.5 for unbiased (normal distributions
    // only)
    pub sample_stdev_correction: Option<f64>,
}

impl NormalizeConfig {
    pub fn sample_stdev_correction(&self) -> f64 {
        self.sample_stdev_correction.unwrap_or(0.0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaleConfig {
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
}

impl ScaleConfig {
    pub fn min_value(&self) -> f64 {
        self.min_value.unwrap_or(0.0)
    }
    pub fn max_value(&self) -> f64 {
        self.max_value.unwrap_or(1.0)
    }
    pub fn has_custom_minmax(&self) -> bool {
        self.min_value.is_some() || self.max_value.is_some()
    }
}
