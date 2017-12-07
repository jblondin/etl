//! Extract-Transform-Load (ETL) library for Rust.

#![warn(missing_docs)]

extern crate num;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate csv;
extern crate encoding;
extern crate toml;
#[macro_use] extern crate error_chain;

extern crate matrix;

mod errors;

pub mod dataframe;
pub use dataframe::{DataConfig, DataFrame};
