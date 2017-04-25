use std::error::Error;
use std;
use std::fmt::{Display, Formatter};

use csv;

use yaml_rust;

#[derive(Debug, PartialEq)]
pub struct DataFrameError {
    desc: String,
}
impl Display for DataFrameError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "data frame error: {}", self.desc)
    }
}
impl std::error::Error for DataFrameError {
    fn description(&self) -> &str { &self.desc[..] }
    fn cause(&self) -> Option<&std::error::Error> { None }
}
impl From<String> for DataFrameError {
    fn from(s: String) -> DataFrameError {
        DataFrameError { desc: s }
    }
}
impl<'a> From<&'a str> for DataFrameError {
    fn from(s: &'a str) -> DataFrameError {
        DataFrameError { desc: s.to_string() }
    }
}
macro_rules! from_error {
    ($err:ident, $serr:ty) => {
        impl From<$serr> for $err {
            fn from(e: $serr) -> $err {
                $err { desc: e.description().to_string() }
            }
        }
    }
}
from_error!(DataFrameError, csv::Error);
from_error!(DataFrameError, std::io::Error);
from_error!(DataFrameError, yaml_rust::ScanError);
from_error!(DataFrameError, std::str::ParseBoolError);
from_error!(DataFrameError, std::num::ParseFloatError);
from_error!(DataFrameError, std::num::ParseIntError);
from_error!(DataFrameError, std::string::ParseError);

impl DataFrameError {
    pub fn new(s: &str) -> DataFrameError {
        DataFrameError { desc: s.to_string() }
    }
}
