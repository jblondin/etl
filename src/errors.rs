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
