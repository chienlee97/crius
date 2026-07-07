#![allow(dead_code)]

pub(crate) fn short_id(id: &str) -> &str {
    id.get(..12).unwrap_or(id)
}

pub(crate) fn short_image_id(id: &str) -> String {
    let trimmed = id.strip_prefix("sha256:").unwrap_or(id);
    short_id(trimmed).to_string()
}

pub(crate) fn truncate_field(value: &str, no_trunc: bool) -> String {
    const MAX_FIELD_WIDTH: usize = 96;

    if no_trunc || value.chars().count() <= MAX_FIELD_WIDTH {
        return value.to_string();
    }

    let mut truncated = value.chars().take(MAX_FIELD_WIDTH - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

pub(crate) fn truncate_error(value: &str, no_trunc: bool) -> String {
    const MAX_ERROR_WIDTH: usize = 160;

    if no_trunc || value.chars().count() <= MAX_ERROR_WIDTH {
        return value.to_string();
    }

    let mut truncated = value.chars().take(MAX_ERROR_WIDTH - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shortens_ids() {
        assert_eq!(short_id("123456789abczzz"), "123456789abc");
        assert_eq!(short_id("short"), "short");
        assert_eq!(short_image_id("sha256:abcdef1234567890"), "abcdef123456");
    }

    #[test]
    fn truncates_fields() {
        let value = "a".repeat(120);

        assert_eq!(truncate_field(&value, false).chars().count(), 96);
        assert!(truncate_field(&value, false).ends_with("..."));
        assert_eq!(truncate_field(&value, true), value);
    }

    #[test]
    fn truncates_errors() {
        let value = "a".repeat(200);

        assert_eq!(truncate_error(&value, false).chars().count(), 160);
        assert!(truncate_error(&value, false).ends_with("..."));
        assert_eq!(truncate_error(&value, true), value);
    }
}
