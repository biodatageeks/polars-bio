pub(crate) fn default_cols_to_string(s: &[&str; 3]) -> Vec<String> {
    s.iter().map(|x| x.to_string()).collect()
}
