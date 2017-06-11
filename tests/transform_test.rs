extern crate etl;
#[macro_use] extern crate unittest;

use std::path::PathBuf;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Shl;

use etl::dataframe::DataFrame;

fn hash_details(value: &str) -> (String, f64) {
    let mut hasher = DefaultHasher::new();
    let hash_size: u64 = 4;
    let midpoint = 1u64.shl(63);
    value.to_string().hash(&mut hasher);
    let h = hasher.finish();
    let hash_feature = format!("vec_hash_e_{}", h % hash_size);
    let hash_sign= if h >= midpoint { 1.0 } else { -1.0 };
    (hash_feature, hash_sign)
}

#[test]
fn test_transform() {
    let data_path = PathBuf::from(file!()).parent().unwrap().join("data/transform_test.toml");

    let (config, df) = DataFrame::load(data_path.as_path()).unwrap();

    println!("{:?}", config);
    println!("{:?}", df);

    let mut fieldnames = df.fieldnames();
    fieldnames.sort();
    assert_eq!(fieldnames, ["c", "cat_ab", "d", "e", "map_convert_e", "map_e",
        "norm_f_sample", "norm_f_uncorr", "scaled_f_custom", "scaled_f_default",
        "vec_hash_e_0", "vec_hash_e_1", "vec_hash_e_2", "vec_hash_e_3",
        "vec_onehot_e_F", "vec_onehot_e_M","vec_onehot_e_e2",
        "vec_onehot_e_n11_F", "vec_onehot_e_n11_M", "vec_onehot_e_n11_e2"]);

    let field_c = df.get_signed_field("c");
    assert!(field_c.is_some());
    assert_eq!(field_c.unwrap(), &[1, 2, 3, 4, 5, 6, 7, 8, 9]);

    let field_d = df.get_signed_field("d");
    assert!(field_d.is_some());
    assert_eq!(field_d.unwrap(), &[9, 8, 7, 6, 5, 4, 3, 2, 1]);

    let field_e = df.get_string_field("e");
    assert!(field_e.is_some());
    assert_eq!(field_e.unwrap(), &["M", "e2", "F", "M", "M", "F", "F", "F", "F"]);

    let field_cat_ab = df.get_string_field("cat_ab");
    assert!(field_cat_ab.is_some());
    assert_eq!(field_cat_ab.unwrap(), &["a1!!b1", "a2!!b2", "a3!!b3", "a4!!b4", "a5!!b5", "a6!!b6",
        "a7!!b7", "a8!!b8", "a9!!b9"]);

    let field_map_e = df.get_string_field("map_e");
    assert!(field_map_e.is_some());
    assert_eq!(field_map_e.unwrap(), &["0", "-1", "1", "0", "0", "1", "1", "1", "1"]);

    let field_map_convert_e = df.get_signed_field("map_convert_e");
    assert!(field_map_convert_e.is_some());
    assert_eq!(field_map_convert_e.unwrap(), &[0, -1, 1, 0, 0, 1, 1, 1, 1]);

    let field_scaled_f_custom = df.get_float_field("scaled_f_custom");
    assert!(field_scaled_f_custom.is_some());
    assert_fpvec_eq!(field_scaled_f_custom.unwrap(),
        [-1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 1.0], 1e-12);

    let field_scaled_f_default = df.get_float_field("scaled_f_default");
    assert!(field_scaled_f_default.is_some());
    assert_fpvec_eq!(field_scaled_f_default.unwrap(),
        [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 1.0], 1e-12);

    let field_norm_f_uncorr = df.get_float_field("norm_f_uncorr");
    assert!(field_norm_f_uncorr.is_some());
    assert_fpvec_eq!(field_norm_f_uncorr.unwrap(), [-1.4201266762, -1.0837808845, -0.7474350928,
        -0.411089301, -0.0747435093, 0.2616022825, 0.5979480742, 0.9342938659, 1.9433312412], 1e-9);

    let field_norm_f_sample = df.get_float_field("norm_f_sample");
    assert!(field_norm_f_sample.is_some());
    assert_fpvec_eq!(field_norm_f_sample.unwrap(), [-1.3389082705, -1.021798417, -0.7046885634,
        -0.3875787099, -0.0704688563, 0.2466409972, 0.5637508508, 0.8808607043, 1.8321902649],
        1e-9);

    let field_vec_onehot_e_e2 = df.get_float_field("vec_onehot_e_e2");
    assert!(field_vec_onehot_e_e2.is_some());
    assert_fpvec_eq!(field_vec_onehot_e_e2.unwrap(), [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);

    let field_vec_onehot_e_f = df.get_float_field("vec_onehot_e_F");
    assert!(field_vec_onehot_e_f.is_some());
    assert_fpvec_eq!(field_vec_onehot_e_f.unwrap(), [0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0]);

    let field_vec_onehot_e_m = df.get_float_field("vec_onehot_e_M");
    assert!(field_vec_onehot_e_m.is_some());
    assert_fpvec_eq!(field_vec_onehot_e_m.unwrap(), [1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0]);

    let field_vec_onehot_e_n11_e2 = df.get_float_field("vec_onehot_e_n11_e2");
    assert!(field_vec_onehot_e_n11_e2.is_some());
    assert_fpvec_eq!(field_vec_onehot_e_n11_e2.unwrap(),
        [-1.0, 1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0]);

    let field_vec_onehot_e_n11_f = df.get_float_field("vec_onehot_e_n11_F");
    assert!(field_vec_onehot_e_n11_f.is_some());
    assert_fpvec_eq!(field_vec_onehot_e_n11_f.unwrap(),
        [-1.0, -1.0, 1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0]);

    let field_vec_onehot_e_n11_m = df.get_float_field("vec_onehot_e_n11_M");
    assert!(field_vec_onehot_e_n11_m.is_some());
    assert_fpvec_eq!(field_vec_onehot_e_n11_m.unwrap(),
        [1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0]);

    let (feature_hash_e2, feature_sign_e2) = hash_details("e2");
    let (feature_hash_m, feature_sign_m) = hash_details("M");
    let (feature_hash_f, feature_sign_f) = hash_details("F");
    let expected_hash = |s| {
        [
            if feature_hash_m == s { feature_sign_m } else { 0.0 },
            if feature_hash_e2 == s { feature_sign_e2 } else { 0.0 },
            if feature_hash_f == s { feature_sign_f } else { 0.0 },
            if feature_hash_m == s { feature_sign_m } else { 0.0 },
            if feature_hash_m == s { feature_sign_m } else { 0.0 },
            if feature_hash_f == s { feature_sign_f } else { 0.0 },
            if feature_hash_f == s { feature_sign_f } else { 0.0 },
            if feature_hash_f == s { feature_sign_f } else { 0.0 },
            if feature_hash_f == s { feature_sign_f } else { 0.0 },
        ]
    };

    let field_vec_hash_e_0 = df.get_float_field("vec_hash_e_0");
    assert!(field_vec_hash_e_0.is_some());
    assert_fpvec_eq!(field_vec_hash_e_0.unwrap(), expected_hash("vec_hash_e_0"));

    let field_vec_hash_e_1 = df.get_float_field("vec_hash_e_1");
    assert!(field_vec_hash_e_1.is_some());
    assert_fpvec_eq!(field_vec_hash_e_1.unwrap(), expected_hash("vec_hash_e_1"));

    let field_vec_hash_e_2 = df.get_float_field("vec_hash_e_2");
    assert!(field_vec_hash_e_2.is_some());
    assert_fpvec_eq!(field_vec_hash_e_2.unwrap(), expected_hash("vec_hash_e_2"));

    let field_vec_hash_e_3 = df.get_float_field("vec_hash_e_3");
    assert!(field_vec_hash_e_3.is_some());
    assert_fpvec_eq!(field_vec_hash_e_3.unwrap(), expected_hash("vec_hash_e_3"));
}
