// error_chain causes unused_doc_comment warnings
#![allow(unused_doc_comment)]

error_chain! {
    errors {
        DataFrameError(s: String) {
            description("DataFrame error")
            display("DataFrame error: {}", s)
        }
        DataConfigError(s: String) {
            description("DataConfig error")
            display("DataConfig error: {}", s)
        }
    }
}
