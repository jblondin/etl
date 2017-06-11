extern crate etl;
extern crate toml;
extern crate serde_json;

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use etl::dataframe::DataConfig;

#[test]
fn test_json_toml() {
    let data_config_json = {
        let mut config_file = File::open(PathBuf::from(file!()).parent().unwrap()
            .join("data/json_toml_test.json")).expect("Unable to open!");
        let mut buffer = String::new();
        config_file.read_to_string(&mut buffer).expect("Unable to read to string");

        let data_config: DataConfig = serde_json::from_str(&buffer[..]).unwrap();

        println!("{:?}", data_config);
        data_config
    };
    let data_config_toml = {
        let mut config_file = File::open(PathBuf::from(file!()).parent().unwrap()
            .join("data/json_toml_test.toml")).expect("Unable to open!");
        let mut buffer = String::new();
        config_file.read_to_string(&mut buffer).expect("Unable to read to string");

        let data_config: DataConfig = toml::from_str(&buffer[..]).unwrap();

        println!("{:?}", data_config);
        data_config
    };

    assert!(data_config_json == data_config_toml);
}
