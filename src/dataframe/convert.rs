//! Field conversion methods

use std::fmt;

use errors::*;

use num::traits::cast::ToPrimitive;

use dataframe::DataStore;
use dataframe::config::FieldType;

pub enum ConvertType {
    UnsignedToUnsigned,
    UnsignedToSigned,
    UnsignedToText,
    UnsignedToBoolean,
    UnsignedToFloat,

    SignedToUnsigned,
    SignedToSigned,
    SignedToText,
    SignedToBoolean,
    SignedToFloat,

    TextToUnsigned,
    TextToSigned,
    TextToText,
    TextToBoolean,
    TextToFloat,

    BooleanToUnsigned,
    BooleanToSigned,
    BooleanToText,
    BooleanToBoolean,
    BooleanToFloat,

    FloatToUnsigned,
    FloatToSigned,
    FloatToText,
    FloatToBoolean,
    FloatToFloat,
}

pub fn convert_field(
        source_field: &String, source_type: FieldType,
        target_field: &String, target_type: FieldType,
        orig_ds: &DataStore) -> Result<DataStore> {
    let mut conv_data = DataStore::empty();
    match gen_convert_type(source_type, target_type) {
        ConvertType::UnsignedToUnsigned => { conv_data.merge_unsigned(target_field,
            orig_ds.get_unsigned_field(source_field).unwrap().vec_convert())?; }
        ConvertType::UnsignedToSigned => { conv_data.merge_signed(target_field,
            orig_ds.get_unsigned_field(source_field).unwrap().vec_convert())?; }
        ConvertType::UnsignedToText => { conv_data.merge_text(target_field,
            orig_ds.get_unsigned_field(source_field).unwrap().vec_convert())?; }
        ConvertType::UnsignedToBoolean => { conv_data.merge_boolean(target_field,
            orig_ds.get_unsigned_field(source_field).unwrap().vec_convert())?; }
        ConvertType::UnsignedToFloat => { conv_data.merge_float(target_field,
            orig_ds.get_unsigned_field(source_field).unwrap().vec_convert())?; }

        ConvertType::SignedToUnsigned => { conv_data.merge_unsigned(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }
        ConvertType::SignedToSigned => { conv_data.merge_signed(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }
        ConvertType::SignedToText => { conv_data.merge_text(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }
        ConvertType::SignedToBoolean => { conv_data.merge_boolean(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }
        ConvertType::SignedToFloat => { conv_data.merge_float(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }

        ConvertType::TextToUnsigned => { conv_data.merge_unsigned(target_field,
            orig_ds.get_text_field(source_field).unwrap().vec_convert())?; }
        ConvertType::TextToSigned => { conv_data.merge_signed(target_field,
            orig_ds.get_text_field(source_field).unwrap().vec_convert())?; }
        ConvertType::TextToText => { conv_data.merge_text(target_field,
            orig_ds.get_text_field(source_field).unwrap().vec_convert())?; }
        ConvertType::TextToBoolean => { conv_data.merge_boolean(target_field,
            orig_ds.get_text_field(source_field).unwrap().vec_convert())?; }
        ConvertType::TextToFloat => { conv_data.merge_float(target_field,
            orig_ds.get_text_field(source_field).unwrap().vec_convert())?; }

        ConvertType::BooleanToUnsigned => { conv_data.merge_unsigned(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }
        ConvertType::BooleanToSigned => { conv_data.merge_signed(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }
        ConvertType::BooleanToText => { conv_data.merge_text(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }
        ConvertType::BooleanToBoolean => { conv_data.merge_boolean(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }
        ConvertType::BooleanToFloat => { conv_data.merge_float(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }

        ConvertType::FloatToUnsigned => { conv_data.merge_unsigned(target_field,
            orig_ds.get_float_field(source_field).unwrap().vec_convert())?; }
        ConvertType::FloatToSigned => { conv_data.merge_signed(target_field,
            orig_ds.get_float_field(source_field).unwrap().vec_convert())?; }
        ConvertType::FloatToText => { conv_data.merge_text(target_field,
            orig_ds.get_float_field(source_field).unwrap().vec_convert())?; }
        ConvertType::FloatToBoolean => { conv_data.merge_boolean(target_field,
            orig_ds.get_float_field(source_field).unwrap().vec_convert())?; }
        ConvertType::FloatToFloat => { conv_data.merge_float(target_field,
            orig_ds.get_float_field(source_field).unwrap().vec_convert())?; }
    }

    Ok(conv_data)
}

trait VecConvert<T> {
    fn vec_convert(&self) -> Vec<T>;
}

// Unsigned -> *
impl VecConvert<u64> for Vec<u64> {
    fn vec_convert(&self) -> Vec<u64> { self.clone() }
}
impl VecConvert<i64> for Vec<u64> {
    fn vec_convert(&self) -> Vec<i64> { self.iter().map(|u| u.to_i64().unwrap()).collect() }
}
impl VecConvert<String> for Vec<u64> {
    fn vec_convert(&self) -> Vec<String> { self.iter().map(|u| format!("{}", u)).collect() }
}
impl VecConvert<bool> for Vec<u64> {
    fn vec_convert(&self) -> Vec<bool> {
        self.iter().map(|&u| if u == 0 { false } else { true } ).collect()
    }
}
impl VecConvert<f64> for Vec<u64> {
    fn vec_convert(&self) -> Vec<f64> { self.iter().map(|u| u.to_f64().unwrap()).collect() }
}

// Signed -> *
impl VecConvert<u64> for Vec<i64> {
    fn vec_convert(&self) -> Vec<u64> { self.iter().map(|i| i.to_u64().unwrap()).collect() }
}
impl VecConvert<i64> for Vec<i64> {
    fn vec_convert(&self) -> Vec<i64> { self.clone() }
}
impl VecConvert<String> for Vec<i64> {
    fn vec_convert(&self) -> Vec<String> { self.iter().map(|i| format!("{}", i)).collect() }
}
impl VecConvert<bool> for Vec<i64> {
    fn vec_convert(&self) -> Vec<bool> {
        self.iter().map(|&i| if i == 0 { false } else { true } ).collect()
    }
}
impl VecConvert<f64> for Vec<i64> {
    fn vec_convert(&self) -> Vec<f64> { self.iter().map(|i| i.to_f64().unwrap()).collect() }
}

// String -> *
impl VecConvert<u64> for Vec<String> {
    fn vec_convert(&self) -> Vec<u64> {
        self.iter().map(|s| s.parse().unwrap()).collect()
    }
}
impl VecConvert<i64> for Vec<String> {
    fn vec_convert(&self) -> Vec<i64> {
        self.iter().map(|s| s.parse().unwrap()).collect()
    }
}
impl VecConvert<String> for Vec<String> {
    fn vec_convert(&self) -> Vec<String> { self.clone() }
}
impl VecConvert<bool> for Vec<String> {
    fn vec_convert(&self) -> Vec<bool> {
        self.iter().map(|s| s.parse().unwrap()).collect()
    }
}
impl VecConvert<f64> for Vec<String> {
    fn vec_convert(&self) -> Vec<f64> {
        self.iter().map(|s| s.parse().unwrap()).collect()
    }
}

// Bool -> *
impl VecConvert<u64> for Vec<bool> {
    fn vec_convert(&self) -> Vec<u64> {
        self.iter().map(|&b| if b { 1 } else { 0 }).collect()
    }
}
impl VecConvert<i64> for Vec<bool> {
    fn vec_convert(&self) -> Vec<i64> {
        self.iter().map(|&b| if b { 1 } else { 0 } ).collect()
    }
}
impl VecConvert<String> for Vec<bool> {
    fn vec_convert(&self) -> Vec<String> {
        self.iter().map(|&b| format!("{}", b) ).collect()
    }
}
impl VecConvert<bool> for Vec<bool> {
    fn vec_convert(&self) -> Vec<bool> { self.clone() }
}
impl VecConvert<f64> for Vec<bool> {
    fn vec_convert(&self) -> Vec<f64> {
        self.iter().map(|&b| if b { 1.0 } else { 0.0 } ).collect()
    }
}

// Float -> *
impl VecConvert<u64> for Vec<f64> {
    fn vec_convert(&self) -> Vec<u64> { self.iter().map(|f| f.to_u64().unwrap()).collect() }
}
impl VecConvert<i64> for Vec<f64> {
    fn vec_convert(&self) -> Vec<i64> { self.iter().map(|f| f.to_i64().unwrap()).collect() }
}
impl VecConvert<String> for Vec<f64> {
    fn vec_convert(&self) -> Vec<String> { self.iter().map(|f| format!("{}", f)).collect() }
}
impl VecConvert<bool> for Vec<f64> {
    fn vec_convert(&self) -> Vec<bool> {
        self.iter().map(|&f| if f == 0.0 { false } else { true } ).collect()
    }
}
impl VecConvert<f64> for Vec<f64> {
    fn vec_convert(&self) -> Vec<f64> { self.clone() }
}

fn gen_convert_type(source_type: FieldType, target_type: FieldType) -> ConvertType {
    // get ready for giant match statement!
    match source_type {
        FieldType::Unsigned => {
            match target_type {
                FieldType::Unsigned => ConvertType::UnsignedToUnsigned,
                FieldType::Signed   => ConvertType::UnsignedToSigned,
                FieldType::Text     => ConvertType::UnsignedToText,
                FieldType::Boolean  => ConvertType::UnsignedToBoolean,
                FieldType::Float    => ConvertType::UnsignedToFloat,
            }
        },
        FieldType::Signed => {
            match target_type {
                FieldType::Unsigned => ConvertType::SignedToUnsigned,
                FieldType::Signed   => ConvertType::SignedToSigned,
                FieldType::Text     => ConvertType::SignedToText,
                FieldType::Boolean  => ConvertType::SignedToBoolean,
                FieldType::Float    => ConvertType::SignedToFloat,
            }
        },
        FieldType::Text => {
            match target_type {
                FieldType::Unsigned => ConvertType::TextToUnsigned,
                FieldType::Signed   => ConvertType::TextToSigned,
                FieldType::Text     => ConvertType::TextToText,
                FieldType::Boolean  => ConvertType::TextToBoolean,
                FieldType::Float    => ConvertType::TextToFloat,
            }
        },
        FieldType::Boolean => {
            match target_type {
                FieldType::Unsigned => ConvertType::BooleanToUnsigned,
                FieldType::Signed   => ConvertType::BooleanToSigned,
                FieldType::Text     => ConvertType::BooleanToText,
                FieldType::Boolean  => ConvertType::BooleanToBoolean,
                FieldType::Float    => ConvertType::BooleanToFloat,
            }
        },
        FieldType::Float => {
            match target_type {
                FieldType::Unsigned => ConvertType::FloatToUnsigned,
                FieldType::Signed   => ConvertType::FloatToSigned,
                FieldType::Text     => ConvertType::FloatToText,
                FieldType::Boolean  => ConvertType::FloatToBoolean,
                FieldType::Float    => ConvertType::FloatToFloat,
            }
        },
    }
}

impl fmt::Debug for ConvertType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Convert{}", {
            match *self {
                ConvertType::UnsignedToUnsigned => "UnsignedToUnsigned",
                ConvertType::UnsignedToSigned   => "UnsignedToSigned",
                ConvertType::UnsignedToText     => "UnsignedToText",
                ConvertType::UnsignedToBoolean  => "UnsignedToBooleanean",
                ConvertType::UnsignedToFloat    => "UnsignedToFloat",

                ConvertType::SignedToUnsigned   => "SignedToUnsigned",
                ConvertType::SignedToSigned     => "SignedToSigned",
                ConvertType::SignedToText       => "SignedToText",
                ConvertType::SignedToBoolean    => "SignedToBoolean",
                ConvertType::SignedToFloat      => "SignedToFloat",

                ConvertType::TextToUnsigned     => "TextToUnsigned",
                ConvertType::TextToSigned       => "TextToSigned",
                ConvertType::TextToText         => "TextToText",
                ConvertType::TextToBoolean      => "TextToBoolean",
                ConvertType::TextToFloat        => "TextToFloat",

                ConvertType::BooleanToUnsigned  => "BooleanToUnsigned",
                ConvertType::BooleanToSigned    => "BooleanToSigned",
                ConvertType::BooleanToText      => "BooleanToText",
                ConvertType::BooleanToBoolean   => "BooleanToBoolean",
                ConvertType::BooleanToFloat     => "BooleanToFloat",

                ConvertType::FloatToUnsigned    => "FloatToUnsigned",
                ConvertType::FloatToSigned      => "FloatToSigned",
                ConvertType::FloatToText        => "FloatToText",
                ConvertType::FloatToBoolean     => "FloatToBoolean",
                ConvertType::FloatToFloat       => "FloatToFloat",
            }
        })
    }
}
