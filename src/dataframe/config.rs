//! Dataframe configuration structs and methods

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde_json;
use toml;

use dataframe::DataStore;
use dataframe::TransformFields;

use errors::*;

/// Specification of how a data frame is configued from data sources
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataConfig {
    /// Source files
    pub source_files: Vec<SourceFile>,
    /// (Optional) list of transforms on fields in the source files
    pub transforms: Option<Vec<Transform>>,
}

impl DataConfig {
    /// Generate a DataConfig from a JSON or TOML config file path
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

/// Source file details
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceFile {
    /// Source file name
    pub name: String,
    /// Delimiter used in source file
    pub delimiter: Option<String>,
    /// List of fields in source file
    pub fields: Vec<Field>,
    /// (Optional) filters used when importing this source file
    pub filters: Option<Vec<Filter>>,
}

impl SourceFile {
    /// Returns the path for this source file
    pub fn path(&self) -> &Path {
        Path::new(&self.name[..])
    }

    /// Returns the delimiter used in this source file
    pub fn delimiter(&self) -> Result<u8> {
        Ok(match self.delimiter {
            Some(ref delim) => {
                delim.bytes().next().ok_or(Error::from(ErrorKind::DataConfigError(
                    "invalid delimiter specification".to_string())))?
            }
            None => { b',' }
        })
    }

    /// Gets field details, given a specific field name
    pub fn get_source_field(&self, s: &String) -> Option<&Field> {
        self.fields.iter().find(|&&ref field| field.source_name == *s)
    }
}

/// Source field details
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    /// Name of field
    pub source_name: String,
    /// (Optional) transformed name of field
    pub target_name: Option<String>,
    /// Field type
    pub field_type: FieldType,
    /// Whether or not to add this field to the dataframe. Defaults to true
    pub add_to_frame: Option<bool>,
}

impl Field {
    /// Final name of this field, either the specific target name (if available), or the source name
    /// if not
    pub fn target_name(&self) -> &String {
        self.target_name.as_ref().unwrap_or(&self.source_name)
    }

    /// Whether or not this field is added to the dataframe
    pub fn add_to_frame(&self) -> bool {
        self.add_to_frame.unwrap_or(true)
    }
}

/// Specification of the type of field
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    /// Unsigned integer field
    Unsigned,
    /// Signed integer field
    Signed,
    /// Text (string) field
    Text,
    /// Boolean (yes/no) field
    Boolean,
    /// Floating-point field
    Float
}

/// Source file filter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    /// Name of source field
    pub source_field: String,
    /// Method of filter
    pub filter: FilterMethod,
}
impl Filter {
    /// Apply this filter to the value, returning whether or not to include the value in the
    /// resulting data frame
    pub fn apply(&self, value_str: &String) -> Result<bool> {
        self.filter.apply(value_str)
    }
}

/// Filter method
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum FilterMethod {
    /// Filter based on whether or not the value matches a particular value
    Match(MatchConfig),
    /// Filter based on whether or not the value doesn't match a particular value
    MatchNot(MatchConfig),
    /// Filter based on an inequality comparison (less than, greater than)
    Inequality(InequalityConfig),
}
impl FilterMethod {
    /// Apply the filter method to the value
    pub fn apply(&self, value_str: &String) -> Result<bool> {
        match *self {
            FilterMethod::Match(ref config) => { config.does_match(value_str) }
            FilterMethod::MatchNot(ref config) => { config.does_match(value_str).map(|b| !b) }
            FilterMethod::Inequality(ref config) => { config.does_satisfy(value_str) }
        }
    }
}

/// Configuration details for matching-based filters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchConfig {
    /// Text-based match target
    text: Option<String>,
    /// Signed integer match target
    signed: Option<i64>,
    /// Unsigned integer match target
    unsigned: Option<u64>,
    /// Boolean match target
    boolean: Option<bool>,
    /// Floating-point match target
    float: Option<f64>,
}
impl MatchConfig {
    /// Checks match against given value.
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

/// Configuration details for inequality-based filters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InequalityConfig {
    /// Which inequality to use
    inequality: InequalityMethod,

    /// Signed integer inequality target
    signed: Option<i64>,
    /// Unsigned integer inequality target
    unsigned: Option<u64>,
    /// Floating point inequality target
    float: Option<f64>,

}
impl InequalityConfig {
    /// Checks to see if value satisfies the inequality
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

/// Type of inequality
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum InequalityMethod {
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    Gte,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    Lte
}
impl InequalityMethod {
    /// Check to see if value satisfies the inequality
    pub fn does_satisfy<T: PartialOrd> (&self, value: T, target: T) -> bool {
        match *self {
            InequalityMethod::Gt => value > target,
            InequalityMethod::Gte => value >= target,
            InequalityMethod::Lt => value < target,
            InequalityMethod::Lte => value <= target,
        }
    }
}

/// Definition of a transform (conversion between one or more fields into a target field)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transform {
    /// Method of transformation
    pub method: TransformMethod,
    /// List of one of more source field names
    pub source_fields: Vec<String>,
    /// Field name for transformed field
    pub target_name: String,
    /// Whether or not to add this transformed field to the final frame (defaults to true)
    pub add_to_frame: Option<bool>,
}

impl Transform {
    /// Whether or not this transformed field will be added to the frame
    pub fn add_to_frame(&self) -> bool {
        self.add_to_frame.unwrap_or(true)
    }
    /// Target type for the transformed field
    pub fn target_type(&self) -> FieldType {
        self.method.target_type()
    }
    /// Check whether or not the source exists is the specified data store for this transform
    pub fn source_exists(&self, ds: &DataStore) -> bool {
        match check_transform_source(&self.source_fields, ds) {
            Ok(_)  => true,
            Err(_) => false
        }
    }
    /// Perform the transform using the specified data store
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

/// Transformation method
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum TransformMethod {
    /// Conversion transform (converting between types)
    Convert(ConvertConfig),
    /// Map transform (mapping from one value to another value of the same type)
    Map(MapConfig),
    /// Concatenation transform (for string field types)
    Concatenate(ConcatenateConfig),
    /// One-hot vectorization of a string categorical field
    VectorizeOneHot(VecOneHotConfig),
    /// Hash vectorization of a string categorical field
    VectorizeHash(VecHashConfig),
    /// Normalize a floating-point field
    Normalize(NormalizeConfig),
    /// Scaling for a floating-point field
    Scale(ScaleConfig),
}

impl TransformMethod {
    /// The field type that result from this transformation method
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
    /// Use this method to transform a data store's one or more source fields into a field with the
    /// target name
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

/// Configuration of a conversion transformation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConvertConfig {
    target_type: FieldType,
}

impl ConvertConfig {
    /// Target field type of this conversion
    pub fn target_type(&self) -> FieldType {
        self.target_type
    }
}

/// Configuration of a mapping transformation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MapConfig {
    /// Default target value (when source value doesn't exist in map)
    pub default_value: String,
    /// Map of source values to target values
    pub map: HashMap<String, String>,
}

/// Configuration of a concatenation transformation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConcatenateConfig {
    separator: Option<String>,
}

impl ConcatenateConfig {
    /// Separator used for concatenation
    pub fn separator(&self) -> String {
        self.separator.clone().unwrap_or(String::new())
    }
}

/// Method for binary scaling
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryScaling {
    /// Scale between zero and one
    ZeroOne,
    /// Scale between negative one and one
    NegOneOne,
}
impl BinaryScaling {
    /// Return the "off" value for this scaling: 0.0 for ZeroOne, -1.0 for NegOneONe
    pub fn off_value(&self) -> f64 {
        match *self {
            BinaryScaling::ZeroOne   =>  0.0,
            BinaryScaling::NegOneOne => -1.0,
        }
    }
    /// Return the "on" value for this scaling: 1.0
    pub fn on_value(&self) -> f64 {
        1.0
    }
    /// Tuple of Off and On values
    pub fn values(&self) -> (f64, f64) {
        (self.off_value(), self.on_value())
    }
}

/// Configuration of a one-hot vectorization transformation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VecOneHotConfig {
    binary_scaling: Option<BinaryScaling>,
}

impl VecOneHotConfig {
    /// Return the binary scaling used in this vectorization
    pub fn binary_scaling(&self) -> BinaryScaling {
        self.binary_scaling.unwrap_or(BinaryScaling::ZeroOne)
    }
}

/// Configuration of a hash vectorization transformation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VecHashConfig {
    hash_size: Option<u64>,
}

impl VecHashConfig {
    /// Return the hash size used in this vectorization
    pub fn hash_size(&self) -> u64 {
        self.hash_size.unwrap_or(2u64.pow(18))
    }
}

/// Configuration of a normalization transformation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizeConfig {
    // correction for sample stdev calculation, subtracted from N
    // 0.0 for uncorrected, 1.0 for corrected, 1.5 for unbiased (normal distributions
    // only)
    sample_stdev_correction: Option<f64>,
}

impl NormalizeConfig {
    /// Return the sample StDev correction for the sample StDev calculation (subtracted from N)
    /// 0.0 for uncorrected, 1.0 for corrected, 1.5 for unbiased (normal distributions
    /// only)
    pub fn sample_stdev_correction(&self) -> f64 {
        self.sample_stdev_correction.unwrap_or(0.0)
    }
}

/// Configuration for a scaling transformation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaleConfig {
    min_value: Option<f64>,
    max_value: Option<f64>,
}

impl ScaleConfig {
    /// Return the minimum scaling value (default to 0.0)
    pub fn min_value(&self) -> f64 {
        self.min_value.unwrap_or(0.0)
    }
    /// Return the maximum scaling value (default to 1.0)
    pub fn max_value(&self) -> f64 {
        self.max_value.unwrap_or(1.0)
    }
    /// Return whether or not this scaling configuration has a custom minimum or maximum
    pub fn has_custom_minmax(&self) -> bool {
        self.min_value.is_some() || self.max_value.is_some()
    }
}
