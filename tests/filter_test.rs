extern crate etl;

use std::path::PathBuf;
use etl::dataframe::DataFrame;

#[test]
fn test_filter() {
    let data_path = PathBuf::from(file!()).parent().unwrap().join("data/filter_test.toml");

    let (config, df) = DataFrame::load(data_path.as_path()).unwrap();

    println!("{:?}", config);
    println!("{:?}", df);

    let mut fieldnames = df.fieldnames();
    fieldnames.sort();
    assert_eq!(fieldnames, ["c", "f"]);

    assert_eq!(df.nrows(), 2);

    let field_c = df.get_signed_field("c");
    assert!(field_c.is_some());
    assert_eq!(field_c.unwrap(), &[3, 6]);

    let field_f = df.get_float_field("f");
    assert!(field_f.is_some());
    assert_eq!(field_f.unwrap(), &[7.0, 10.0]);
}
