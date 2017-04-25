use std::fmt;

#[derive(Debug)]
pub struct Transform {
    name: String,
    pub trtype: TransformType,
    keep_source: bool,
}
impl Transform {
    pub fn new(name: String, trtype: TransformType, keep_source: bool) -> Transform {
        Transform {
            name: name,
            trtype: trtype,
            keep_source: keep_source,
        }
    }
    pub fn dest_name(&self) -> &String {
        &self.name
    }
    pub fn keep_source(&self) -> bool {
        self.keep_source
    }
}

pub enum TransformType {
    UnsignedToUnsigned(Box<Fn(&u64) -> u64>),
    UnsignedToSigned(Box<Fn(&u64) -> i64>),
    UnsignedToStr(Box<Fn(&u64) -> String>),
    UnsignedToBool(Box<Fn(&u64) -> bool>),
    UnsignedToFloat(Box<Fn(&u64) -> f64>),

    SignedToUnsigned(Box<Fn(&i64) -> u64>),
    SignedToSigned(Box<Fn(&i64) -> i64>),
    SignedToStr(Box<Fn(&i64) -> String>),
    SignedToBool(Box<Fn(&i64) -> bool>),
    SignedToFloat(Box<Fn(&i64) -> f64>),

    StrToUnsigned(Box<Fn(&String) -> u64>),
    StrToSigned(Box<Fn(&String) -> i64>),
    StrToStr(Box<Fn(&String) -> String>),
    StrToBool(Box<Fn(&String) -> bool>),
    StrToFloat(Box<Fn(&String) -> f64>),

    BoolToUnsigned(Box<Fn(&bool) -> u64>),
    BoolToSigned(Box<Fn(&bool) -> i64>),
    BoolToStr(Box<Fn(&bool) -> String>),
    BoolToBool(Box<Fn(&bool) -> bool>),
    BoolToFloat(Box<Fn(&bool) -> f64>),

    FloatToUnsigned(Box<Fn(&f64) -> u64>),
    FloatToSigned(Box<Fn(&f64) -> i64>),
    FloatToStr(Box<Fn(&f64) -> String>),
    FloatToBool(Box<Fn(&f64) -> bool>),
    FloatToFloat(Box<Fn(&f64) -> f64>),
}
impl fmt::Debug for TransformType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Transform{}", {
            match *self {
                TransformType::UnsignedToUnsigned(_) => "UnsignedToUnsigned",
                TransformType::UnsignedToSigned(_) => "UnsignedToSigned",
                TransformType::UnsignedToStr(_) => "UnsignedToStr",
                TransformType::UnsignedToBool(_) => "UnsignedToBool",
                TransformType::UnsignedToFloat(_) => "UnsignedToFloat",

                TransformType::SignedToUnsigned(_) => "SignedToUnsigned",
                TransformType::SignedToSigned(_) => "SignedToSigned",
                TransformType::SignedToStr(_) => "SignedToStr",
                TransformType::SignedToBool(_) => "SignedToBool",
                TransformType::SignedToFloat(_) => "SignedToFloat",

                TransformType::StrToUnsigned(_) => "StrToUnsigned",
                TransformType::StrToSigned(_) => "StrToSigned",
                TransformType::StrToStr(_) => "StrToStr",
                TransformType::StrToBool(_) => "StrToBool",
                TransformType::StrToFloat(_) => "StrToFloat",

                TransformType::BoolToUnsigned(_) => "BoolToUnsigned",
                TransformType::BoolToSigned(_) => "BoolToSigned",
                TransformType::BoolToStr(_) => "BoolToStr",
                TransformType::BoolToBool(_) => "BoolToBool",
                TransformType::BoolToFloat(_) => "BoolToFloat",

                TransformType::FloatToUnsigned(_) => "FloatToUnsigned",
                TransformType::FloatToSigned(_) => "FloatToSigned",
                TransformType::FloatToStr(_) => "FloatToStr",
                TransformType::FloatToBool(_) => "FloatToBool",
                TransformType::FloatToFloat(_) => "FloatToFloat",
            }
        })
    }
}
