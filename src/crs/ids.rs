#![allow(dead_code)]

pub(crate) fn short_id(id: &str) -> &str {
    id.get(..12).unwrap_or(id)
}
