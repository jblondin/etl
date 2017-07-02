# ETL

[![Build Status](https://travis-ci.org/jblondin/etl.svg?branch=master)](https://travis-ci.org/jblondin/etl)

This package is general-purpose Extract-Transform-Load (ETL) library for Rust, built to load arbitrary plain text files into data frame objects.

Features:
* Delimiter speification (comma, tab, etc.)
* Data types:
  * Signed / unsigned integers
  * Floating point numbers
  * Text fields
  * Boolean values
* Transformations:
  * Concatenation (of text fields)
  * Mapping (from one text field to another)
  * Conversion between types
  * Scaling of values (for numeric values, e.g. between -1 and 1)
  * Normalization of values
  * Vectorization ([one-hot](https://en.wikipedia.org/wiki/One-hot) or [feature hashing](https://en.wikipedia.org/wiki/Feature_hashing))
* Filtering

Configuration is handled through a TOML file. For example:
```toml
## data_config.toml

[[source_files]]
name = "source1.csv"
delimiter = ","
fields = [ { source_name = "a_text_field", field_type = "Text", add_to_frame = false },
           { source_name = "another_text_field", field_type = "Text", add_to_frame = false } ]

[[source_files]]
name = "sourc2.tsv"
delimiter = "\t"
fields = [ { source_name = "an_integer", field_type = "Signed" },
           { source_name = "another_integer", field_type = "Signed" },
           { source_name = "a_category", field_type = "Text" },
           { source_name = "an_unused_float", field_type = "Float", add_to_frame = false } ]

[[transforms]]
method = { action = "Concatenate",  separator = " & " }
source_fields = [ "a_text_field", "another_text_field" ]
target_name = "a_new_text_field"

[[transforms]]
source_fields = [ "a_category" ]
target_name = "category_mapped_to_integers"

[transforms.method]
action = "Map"
default_value = "-1"
map = { "first_category" = "0", "second_category" = "1" }
```
To load a configuration file:
```rust
let data_path = PathBuf::from(file!()).parent().unwrap().join("data_config.toml");

let (config, df) = DataFrame::load(data_path.as_path()).unwrap();

let mut fieldnames = df.fieldnames();
fieldnames.sort();
assert_eq!(fieldnames, ["a_category", "a_new_text_field", "an_integer", "another_integer"
    "category_mapped_to_integers"]);
```

Once loaded, files can be transformed into a [matrix](https://github.com/jblondin/matrix) for further processing.
```rust
let (config, df) = DataFrame::load(data_path.as_path()).unwrap();
let (fieldnames, mat) = df.as_matrix().unwrap();
```
