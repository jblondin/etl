extern crate etl;

use std::path::PathBuf;

use etl::dataframe::DataFrame;

#[test]
fn basic_test() {
    let config_path = PathBuf::from(file!()).parent().unwrap().join("data/people.toml");
    let (config, df) = DataFrame::load(config_path.as_path()).unwrap();

    println!("{:#?}", config);
    assert_eq!(df.nrows(), 99);
    let fns = df.fieldnames();
    assert_eq!(fns.len(), 10);
    println!("{:#?}", fns);
}

#[test]
fn matrix_test() {

    let config_path = PathBuf::from(file!()).parent().unwrap().join("data/matrix_test.toml");
    let (config, df) = DataFrame::load(&config_path.as_path()).unwrap();

    println!("{:#?}", config);
    assert_eq!(df.nrows(), 100);

    let (fieldnames, mat) = df.as_matrix().unwrap();
    println!("{:#?}", fieldnames);
    println!("{:#?}", mat);
    assert_eq!(fieldnames.len(), 2);
    assert_eq!(mat.nrows(), 100);
    assert_eq!(mat.ncols(), 2);
}

#[test]
fn sub_test() {
    let config_path = PathBuf::from(file!()).parent().unwrap().join("data/people.toml");
    let (_, df) = DataFrame::load(config_path.as_path()).unwrap();

    let subdf = df.sub(vec!["age", "income", "credit_rating"]).expect("sub() failed");
    assert_eq!(subdf.fieldnames().len(), 3);
    assert_eq!(subdf.nrows(), 99);
    println!("{:#?}", subdf);
}
