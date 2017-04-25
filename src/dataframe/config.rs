use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::Read;
use std::path::{Path};

use yaml_rust::{Yaml, YamlLoader};

use dataframe::{DataFrameError};
use dataframe::transform::{Transform, TransformType};

#[derive(Debug, Copy, Clone)]
pub enum FieldType {
    Unsigned,
    Signed,
    Str,
    Bool,
    Float
}
impl FieldType {
    fn from_str(s: &str) -> Result<FieldType, DataFrameError> {
        Ok(match &s.to_uppercase()[..] {
            "SIGNED"    => FieldType::Signed,
            "UNSIGNED"  => FieldType::Unsigned,
            "STRING"    => FieldType::Str,
            "BOOL"      => FieldType::Bool,
            "FLOAT"     => FieldType::Float,
            _           => {
                return Err(DataFrameError::new("invalid 'type' in 'fields' list"));
            }
        })
    }
    fn as_str(&self) -> String {
        match *self {
            FieldType::Unsigned     => "Unsigned",
            FieldType::Signed       => "Signed",
            FieldType::Str          => "Str",
            FieldType::Bool         => "Bool",
            FieldType::Float        => "Float",
        }.to_string()
    }
}
// struct FieldTypeIterator {
//     curr: FieldType,
//     end: bool,
// }
// impl FieldTypeIterator {
//     fn new() -> FieldTypeIterator {
//         FieldTypeIterator {
//             curr: FieldType::Unsigned,
//             end: false,
//         }
//     }
// }
// impl Iterator for FieldTypeIterator {
//     type Item = FieldType;

//     fn next(&mut self) -> Option<FieldType> {
//         if self.end == true {
//             None
//         } else {
//             match self.curr {
//                 FieldType::Unsigned => {
//                     self.curr = FieldType::Signed;
//                     Some(FieldType::Unsigned)
//                 },
//                 FieldType::Signed => {
//                     self.curr = FieldType::Str;
//                     Some(FieldType::Signed)
//                 },
//                 FieldType::Str => {
//                     self.curr = FieldType::Bool;
//                     Some(FieldType::Str)
//                 },
//                 FieldType::Bool => {
//                     self.curr = FieldType::Float;
//                     Some(FieldType::Bool)
//                 },
//                 FieldType::Float => {
//                     self.end = true;
//                     Some(FieldType::Float)
//                 },
//             }
//         }
//     }
// }
type FieldTypeMap = HashMap<String, FieldType>;

type TransformMap = HashMap<String, Vec<Transform>>;

#[derive(Debug)]
pub struct Config {
    source_types: FieldTypeMap,

    transforms: TransformMap,
}
impl Config {
    fn new(source_types: FieldTypeMap, transforms: TransformMap) -> Config {
        Config {
            source_types: source_types,
            transforms: transforms,
        }
    }
    fn from_yaml(yaml: Yaml) -> Result<Config, DataFrameError> {
        let source_types = try!(parse_source_types(&yaml["fields"]));
        let transforms = try!(parse_transforms(&yaml["transforms"], &source_types));
        Ok(Config::new(source_types, transforms))
    }

    pub fn using_field(&self, s: &String) -> bool {
        self.source_types.contains_key(s)
    }
    pub fn field_names(&self) -> Vec<String> {
        self.source_types.keys().cloned().collect()
    }
    pub fn get_source_type(&self, k: &String) -> FieldType {
        self.source_types[k]
    }
    pub fn get_transforms_for_field(&self, field_name: &String) -> Option<&Vec<Transform>> {
        self.transforms.get(field_name)
    }
}

pub fn load_configfile(config_file_path: &Path) -> Result<Config, DataFrameError> {
    let mut f = try!(File::open(config_file_path));
    let mut s = String::new();
    try!(f.read_to_string(&mut s));
    let mut yaml = try!(YamlLoader::load_from_str(&s));
    if yaml.len() != 1 {
        return Err(DataFrameError::new("config file must contain 1 (and only 1) configuration"));
    }
    Ok(try!(Config::from_yaml(yaml.pop().unwrap())))
}

fn parse_source_types(source_fields_yaml: &Yaml) -> Result<FieldTypeMap, DataFrameError> {
    let mut source_types = FieldTypeMap::new();
    match source_fields_yaml.as_vec() {
        Some(source_fields) => {
            for field in source_fields {
                let ref field_name_yaml = field["name"];
                let ref field_type_yaml = field["type"];
                if field_name_yaml.is_badvalue() || field_type_yaml.is_badvalue() {
                    return Err(DataFrameError::new(
                        "invalid name / type usage in 'fields' list"));
                }
                let field_name = try!(field_name_yaml.as_str().ok_or("invalid 'name'"))
                    .to_string();
                let field_type_str = try!(field_type_yaml.as_str().ok_or(
                    "invalid 'type' in 'fields' list"));
                let field_type = try!(FieldType::from_str(field_type_str));

                source_types.insert(field_name, field_type);
            }
        },
        None => {
            return Err(DataFrameError::new("'fields' list is missing / empty / not a list"));
        }
    }
    Ok(source_types)
}

fn parse_transforms(transforms_yaml: &Yaml, source_types: &FieldTypeMap)
        -> Result<TransformMap, DataFrameError> {
    let mut transforms = TransformMap::new();
    match *transforms_yaml {
        Yaml::Array(ref transforms_hash) => {
            for transform_yaml in transforms_hash {
                println!("{:#?}", transform_yaml);
                let (name, transform) = try!(parse_transform(&transform_yaml, source_types));
                if transforms.contains_key(&name) {
                    transforms.get_mut(&name).unwrap().push(transform);
                } else {
                    transforms.insert(name, vec!(transform));
                }
            }
            Ok(transforms)
        },
        _ => {
            Err(DataFrameError::new("'transforms' does not contains a list"))
        }
    }
}

fn parse_transform(transform_yaml: &Yaml, source_types: &FieldTypeMap)
        -> Result<(String, Transform), DataFrameError> {

    match *transform_yaml {
        Yaml::Hash(ref transform_hash) => {
            let keep_source = match transform_hash.get(&Yaml::from_str("keep_source")) {
                Some(v) => { try!(v.as_bool().ok_or("'keep_source' expected to be boolean")) },
                None    => { true },
            };
            let target_type = try!(FieldType::from_str(try!(
                try!(transform_hash.get(&Yaml::from_str("target_type"))
                    .ok_or("'target_type' missing in 'transforms' list element")
                ).as_str().ok_or("'target_type' expected to be string")
            )));
            let source_name = try!(
                try!(transform_hash.get(&Yaml::from_str("source_field_name"))
                    .ok_or("'source_field_name' missing in 'transforms' list element")
                ).as_str().ok_or("'source_field_name' expected to be string")
            ).to_string();
            if !source_types.contains_key(&source_name) {
                return Err(DataFrameError::new(
                    &format!("unknown 'source_field_name' ({}) in 'transforms' list element",
                        source_name)[..]));
            }
            let target_name = try!(
                try!(transform_hash.get(&Yaml::from_str("target_field_name"))
                    .ok_or("'target_field_name' missing in 'transforms' list element")
                ).as_str().ok_or("'target_field_name' expected to be string")
            ).to_string();
            //TODO: build additional transformations (other than maps)
            let trtype = try!(parse_transform_map(
                try!(transform_hash.get(&Yaml::from_str("transform_map"))
                    .ok_or("'transform_map' missing in 'transforms' list element")
                ), &source_types[&source_name], &target_type
            ));

            Ok((source_name, Transform::new(target_name, trtype, keep_source)))
        },
        _ => {
            Err(DataFrameError::new("'transforms' list element expects a hash"))
        }
    }
}

fn format_yaml(yaml: &Yaml) -> String {
    match *yaml {
        Yaml::Real(ref s)       => { format!("Float: {}", s) },
        Yaml::Integer(ref i)    => { format!("Integer: {}", i) },
        Yaml::String(ref s)     => { format!("String: {}", s) },
        Yaml::Boolean(ref b)    => { format!("Boolean: {}", b) },
        Yaml::Array(_)          => { "List".to_string() },
        Yaml::Hash(_)           => { "Hash".to_string() },
        Yaml::Alias(_)          => { "Alias".to_string() },
        Yaml::Null              => { "Null".to_string() },
        Yaml::BadValue          => { "BadValue".to_string() },
    }
}
fn enforce_expected_type<T>(ftype: &FieldType, opt_value: Option<T>, source: String)
        -> Result<T, DataFrameError> {
    Ok(try!(opt_value.ok_or(
        format!("{} does not match expected type ({})", source, ftype.as_str())
    )))
}

fn parse_transform_map(tmap_yaml: &Yaml, source_type: &FieldType, target_type: &FieldType)
        -> Result<TransformType, DataFrameError> {
    match *tmap_yaml {
        Yaml::Hash(ref tmap_hash) => {
            let default_value_yaml = try!(tmap_hash.get(&Yaml::from_str("default_value"))
                .ok_or("'default_value' missing in 'transform_map'"));
            let mappings_yaml = try!(
                try!(tmap_hash.get(&Yaml::from_str("mappings"))
                    .ok_or("'mappings' missing in 'transform_map'")
                ).as_vec().ok_or("'mappings' expected to be a list")
            );
            Ok(try!(parse_mappings(mappings_yaml, source_type, target_type, &default_value_yaml)))
        },
        _ => {
            Err(DataFrameError::new("'transform_map' expects a hash"))
        }
    }
}

macro_rules! convert_map_into_transform_type {
    ($mappings_yaml:expr, $trtype:expr, $default_value_yaml:expr, $src_type:ty, $dest_type:ty,
            $src_ftype:expr, $dest_ftype:expr, $src_extractor:expr, $dest_extractor:expr) => {{
        let err_str_source = format!("'default value' ({})", format_yaml($default_value_yaml));
        let default_value = try!(enforce_expected_type($dest_ftype,
            $dest_extractor($default_value_yaml), err_str_source));
        let map = try!(build_map::<$src_type, $dest_type>($mappings_yaml, $src_ftype,
            $dest_ftype, $src_extractor, $dest_extractor));
        $trtype(box move |u: &$src_type| { map.get(u).unwrap_or(&default_value).clone() })
    }}
}

fn parse_mappings(mappings_yaml: &Vec<Yaml>, source_type: &FieldType, dest_type: &FieldType,
        def_val: &Yaml) -> Result<TransformType, DataFrameError> {
    // get ready for giant match statement!

    let trtype = match *source_type {
        FieldType::Unsigned => {
            match *dest_type {
                FieldType::Unsigned => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::UnsignedToUnsigned, def_val, u64, u64, source_type, dest_type,
                    box extract_u64, box extract_u64),
                FieldType::Signed   => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::UnsignedToSigned, def_val, u64, i64, source_type, dest_type,
                    box extract_u64, box extract_i64),
                FieldType::Str      => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::UnsignedToStr, def_val, u64, String, source_type, dest_type,
                    box extract_u64, box extract_str),
                FieldType:: Bool    => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::UnsignedToBool, def_val, u64, bool, source_type, dest_type,
                    box extract_u64, box extract_bool),
                FieldType::Float    => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::UnsignedToFloat, def_val, u64, f64, source_type, dest_type,
                    box extract_u64, box extract_f64),
            }
        },
        FieldType::Signed => {
            match *dest_type {
                FieldType::Unsigned => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::SignedToUnsigned, def_val, i64, u64, source_type, dest_type,
                    box extract_i64, box extract_u64),
                FieldType::Signed   => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::SignedToSigned, def_val, i64, i64, source_type, dest_type,
                    box extract_i64, box extract_i64),
                FieldType::Str      => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::SignedToStr, def_val, i64, String, source_type, dest_type,
                    box extract_i64, box extract_str),
                FieldType:: Bool    => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::SignedToBool, def_val, i64, bool, source_type, dest_type,
                    box extract_i64, box extract_bool),
                FieldType::Float    => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::SignedToFloat, def_val, i64, f64, source_type, dest_type,
                    box extract_i64, box extract_f64),
            }
        },
        FieldType::Str => {
            match *dest_type {
                FieldType::Unsigned => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::StrToUnsigned, def_val, String, u64, source_type, dest_type,
                    box extract_str, box extract_u64),
                FieldType::Signed   => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::StrToSigned, def_val, String, i64, source_type, dest_type,
                    box extract_str, box extract_i64),
                FieldType::Str      => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::StrToStr, def_val, String, String, source_type, dest_type,
                    box extract_str, box extract_str),
                FieldType:: Bool    => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::StrToBool, def_val, String, bool, source_type, dest_type,
                    box extract_str, box extract_bool),
                FieldType::Float    => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::StrToFloat, def_val, String, f64, source_type, dest_type,
                    box extract_str, box extract_f64),
            }
        },
        FieldType::Bool => {
            match *dest_type {
                FieldType::Unsigned => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::BoolToUnsigned, def_val, bool, u64, source_type, dest_type,
                    box extract_bool, box extract_u64),
                FieldType::Signed   => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::BoolToSigned, def_val, bool, i64, source_type, dest_type,
                    box extract_bool, box extract_i64),
                FieldType::Str      => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::BoolToStr, def_val, bool, String, source_type, dest_type,
                    box extract_bool, box extract_str),
                FieldType:: Bool    => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::BoolToBool, def_val, bool, bool, source_type, dest_type,
                    box extract_bool, box extract_bool),
                FieldType::Float    => convert_map_into_transform_type!(mappings_yaml,
                    TransformType::BoolToFloat, def_val, bool, f64, source_type, dest_type,
                    box extract_bool, box extract_f64),
            }
        },
        FieldType::Float => {
            return Err(DataFrameError::new(
                "unable to use floating-point types as 'source_value' in mapping"));
        },
    };
    Ok(trtype)
}

fn extract_u64(yaml: &Yaml) -> Option<u64> {
    yaml.as_i64().and_then(|i| if i < 0 { None } else { Some(i as u64) })
}
fn extract_i64(yaml: &Yaml) -> Option<i64> { yaml.as_i64() }
fn extract_str(yaml: &Yaml) -> Option<String> { yaml.as_str().map(|s| s.to_string()) }
fn extract_bool(yaml: &Yaml) -> Option<bool> { yaml.as_bool() }
fn extract_f64(yaml: &Yaml) -> Option<f64> { yaml.as_f64() }

fn build_map<T, U>(mapping_yaml_vec: &Vec<Yaml>, source_type: &FieldType, target_type: &FieldType,
        src_extractor: Box<Fn(&Yaml) -> Option<T>>, target_extractor: Box<Fn(&Yaml) -> Option<U>>)
        -> Result<HashMap<T, U>, DataFrameError> where T: Eq + Hash {
    let mut map = HashMap::new();
    for mapping_yaml in mapping_yaml_vec {
        let single_map = try!(mapping_yaml.as_hash()
            .ok_or("'mappings' element expected to be hash"));

        let source_value_yaml = try!(single_map.get(&Yaml::from_str("source_value"))
            .ok_or("'mappings' list element missing 'source_value'"));
        let err_str_source = format!("'source_value' ({})", format_yaml(source_value_yaml));
        let source_value = try!(enforce_expected_type(source_type, src_extractor(source_value_yaml),
            err_str_source));

        let target_value_yaml = try!(single_map.get(&Yaml::from_str("target_value"))
            .ok_or("'mappings' list element missing 'target_value'"));
        let err_str_target = format!("'target_value' ({})", format_yaml(target_value_yaml));
        let target_value = try!(enforce_expected_type(target_type,
            target_extractor(target_value_yaml), err_str_target));

        map.insert(source_value, target_value);
    }
    Ok(map)
}
