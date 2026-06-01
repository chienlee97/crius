#![allow(dead_code)]

pub(crate) fn normalize_cell(value: &str) -> String {
    let value = value.replace(['\n', '\r'], " ");
    if value.is_empty() {
        "-".to_string()
    } else {
        value
    }
}
