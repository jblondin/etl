#![feature(box_syntax)]

extern crate csv;
extern crate yaml_rust;

pub mod dataframe;

#[cfg(test)]
mod tests {
    use std::path::{PathBuf};
    use super::dataframe::DataFrame;

    macro_rules! test_data_path {
        () => {{
            PathBuf::from(file!()) // current file
                .parent().unwrap() // "src" directory
                .parent().unwrap() // etl crate root directory;
                .join("test_files")
        }}
    }

    #[test]
    fn basic_test() {
        let data_dir_pathbuf = test_data_path!();
        let data_file_path = data_dir_pathbuf.join("ncvoter_sample.txt");
        let config_file_path = data_dir_pathbuf.join("ncvoter_config.yaml");

        let (config, df) = DataFrame::load(&config_file_path, &data_file_path).unwrap();
        println!("{:#?}", config);
        println!("{:#?}", df);
        assert_eq!(df.nrows(), 100);
    }
}
