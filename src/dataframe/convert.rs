use std::fmt;

use errors::*;

use num::traits::cast::ToPrimitive;

use dataframe::DataStore;
use dataframe::config::FieldType;

pub enum ConvertType {
    UnsignedToUnsigned,
    UnsignedToSigned,
    UnsignedToStr,
    UnsignedToBool,
    UnsignedToFloat,

    SignedToUnsigned,
    SignedToSigned,
    SignedToStr,
    SignedToBool,
    SignedToFloat,

    StrToUnsigned,
    StrToSigned,
    StrToStr,
    StrToBool,
    StrToFloat,

    BoolToUnsigned,
    BoolToSigned,
    BoolToStr,
    BoolToBool,
    BoolToFloat,

    FloatToUnsigned,
    FloatToSigned,
    FloatToStr,
    FloatToBool,
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
        ConvertType::UnsignedToStr => { conv_data.merge_string(target_field,
            orig_ds.get_unsigned_field(source_field).unwrap().vec_convert())?; }
        ConvertType::UnsignedToBool => { conv_data.merge_boolean(target_field,
            orig_ds.get_unsigned_field(source_field).unwrap().vec_convert())?; }
        ConvertType::UnsignedToFloat => { conv_data.merge_float(target_field,
            orig_ds.get_unsigned_field(source_field).unwrap().vec_convert())?; }

        ConvertType::SignedToUnsigned => { conv_data.merge_unsigned(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }
        ConvertType::SignedToSigned => { conv_data.merge_signed(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }
        ConvertType::SignedToStr => { conv_data.merge_string(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }
        ConvertType::SignedToBool => { conv_data.merge_boolean(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }
        ConvertType::SignedToFloat => { conv_data.merge_float(target_field,
            orig_ds.get_signed_field(source_field).unwrap().vec_convert())?; }

        ConvertType::StrToUnsigned => { conv_data.merge_unsigned(target_field,
            orig_ds.get_string_field(source_field).unwrap().vec_convert())?; }
        ConvertType::StrToSigned => { conv_data.merge_signed(target_field,
            orig_ds.get_string_field(source_field).unwrap().vec_convert())?; }
        ConvertType::StrToStr => { conv_data.merge_string(target_field,
            orig_ds.get_string_field(source_field).unwrap().vec_convert())?; }
        ConvertType::StrToBool => { conv_data.merge_boolean(target_field,
            orig_ds.get_string_field(source_field).unwrap().vec_convert())?; }
        ConvertType::StrToFloat => { conv_data.merge_float(target_field,
            orig_ds.get_string_field(source_field).unwrap().vec_convert())?; }

        ConvertType::BoolToUnsigned => { conv_data.merge_unsigned(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }
        ConvertType::BoolToSigned => { conv_data.merge_signed(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }
        ConvertType::BoolToStr => { conv_data.merge_string(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }
        ConvertType::BoolToBool => { conv_data.merge_boolean(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }
        ConvertType::BoolToFloat => { conv_data.merge_float(target_field,
            orig_ds.get_boolean_field(source_field).unwrap().vec_convert())?; }

        ConvertType::FloatToUnsigned => { conv_data.merge_unsigned(target_field,
            orig_ds.get_float_field(source_field).unwrap().vec_convert())?; }
        ConvertType::FloatToSigned => { conv_data.merge_signed(target_field,
            orig_ds.get_float_field(source_field).unwrap().vec_convert())?; }
        ConvertType::FloatToStr => { conv_data.merge_string(target_field,
            orig_ds.get_float_field(source_field).unwrap().vec_convert())?; }
        ConvertType::FloatToBool => { conv_data.merge_boolean(target_field,
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
                FieldType::Str      => ConvertType::UnsignedToStr,
                FieldType:: Bool    => ConvertType::UnsignedToBool,
                FieldType::Float    => ConvertType::UnsignedToFloat,
            }
        },
        FieldType::Signed => {
            match target_type {
                FieldType::Unsigned => ConvertType::SignedToUnsigned,
                FieldType::Signed   => ConvertType::SignedToSigned,
                FieldType::Str      => ConvertType::SignedToStr,
                FieldType:: Bool    => ConvertType::SignedToBool,
                FieldType::Float    => ConvertType::SignedToFloat,
            }
        },
        FieldType::Str => {
            match target_type {
                FieldType::Unsigned => ConvertType::StrToUnsigned,
                FieldType::Signed   => ConvertType::StrToSigned,
                FieldType::Str      => ConvertType::StrToStr,
                FieldType:: Bool    => ConvertType::StrToBool,
                FieldType::Float    => ConvertType::StrToFloat,
            }
        },
        FieldType::Bool => {
            match target_type {
                FieldType::Unsigned => ConvertType::BoolToUnsigned,
                FieldType::Signed   => ConvertType::BoolToSigned,
                FieldType::Str      => ConvertType::BoolToStr,
                FieldType:: Bool    => ConvertType::BoolToBool,
                FieldType::Float    => ConvertType::BoolToFloat,
            }
        },
        FieldType::Float => {
            match target_type {
                FieldType::Unsigned => ConvertType::FloatToUnsigned,
                FieldType::Signed   => ConvertType::FloatToSigned,
                FieldType::Str      => ConvertType::FloatToStr,
                FieldType:: Bool    => ConvertType::FloatToBool,
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
                ConvertType::UnsignedToStr      => "UnsignedToStr",
                ConvertType::UnsignedToBool     => "UnsignedToBool",
                ConvertType::UnsignedToFloat    => "UnsignedToFloat",

                ConvertType::SignedToUnsigned   => "SignedToUnsigned",
                ConvertType::SignedToSigned     => "SignedToSigned",
                ConvertType::SignedToStr        => "SignedToStr",
                ConvertType::SignedToBool       => "SignedToBool",
                ConvertType::SignedToFloat      => "SignedToFloat",

                ConvertType::StrToUnsigned      => "StrToUnsigned",
                ConvertType::StrToSigned        => "StrToSigned",
                ConvertType::StrToStr           => "StrToStr",
                ConvertType::StrToBool          => "StrToBool",
                ConvertType::StrToFloat         => "StrToFloat",

                ConvertType::BoolToUnsigned     => "BoolToUnsigned",
                ConvertType::BoolToSigned       => "BoolToSigned",
                ConvertType::BoolToStr          => "BoolToStr",
                ConvertType::BoolToBool         => "BoolToBool",
                ConvertType::BoolToFloat        => "BoolToFloat",

                ConvertType::FloatToUnsigned    => "FloatToUnsigned",
                ConvertType::FloatToSigned      => "FloatToSigned",
                ConvertType::FloatToStr         => "FloatToStr",
                ConvertType::FloatToBool        => "FloatToBool",
                ConvertType::FloatToFloat       => "FloatToFloat",
            }
        })
    }
}
