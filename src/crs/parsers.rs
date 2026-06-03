use std::time::Duration;

pub(crate) const DEFAULT_ENDPOINT: &str = "unix:///run/crius/crius.sock";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Endpoint {
    Unix(String),
    Tcp(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KeyValuePair {
    pub(crate) key: String,
    pub(crate) value: String,
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unix(path) => write!(f, "unix://{path}"),
            Self::Tcp(uri) => f.write_str(uri),
        }
    }
}

pub(crate) fn parse_endpoint(value: &str) -> Result<Endpoint, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("invalid endpoint \"\": expected unix path, unix://, http://, or https://".into());
    }

    if let Some(path) = value.strip_prefix("unix://") {
        if path.is_empty() {
            return Err(format!(
                "invalid endpoint \"{value}\": unix endpoint requires a socket path"
            ));
        }
        return Ok(Endpoint::Unix(path.to_string()));
    }

    if value.starts_with('/') {
        return Ok(Endpoint::Unix(value.to_string()));
    }

    if value.starts_with("http://") || value.starts_with("https://") {
        return Ok(Endpoint::Tcp(value.to_string()));
    }

    Err(format!(
        "invalid endpoint \"{value}\": expected unix path, unix://, http://, or https://"
    ))
}

pub(crate) fn parse_duration(value: &str) -> Result<Duration, String> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, unit) = trimmed.split_at(split_at);

    if digits.is_empty() || unit.is_empty() {
        return Err(format!(
            "invalid duration \"{value}\": expected an integer followed by ms, s, m, or h"
        ));
    }

    let amount = digits
        .parse::<u64>()
        .map_err(|_| format!("invalid duration \"{value}\": value is out of range"))?;

    match unit {
        "ms" => Ok(Duration::from_millis(amount)),
        "s" => Ok(Duration::from_secs(amount)),
        "m" => amount
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("invalid duration \"{value}\": value is out of range")),
        "h" => amount
            .checked_mul(60 * 60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("invalid duration \"{value}\": value is out of range")),
        _ => Err(format!(
            "invalid duration \"{value}\": expected an integer followed by ms, s, m, or h"
        )),
    }
}

#[allow(dead_code)]
pub(crate) fn parse_byte_size(value: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!(
            "invalid byte size \"{value}\": expected an integer optionally followed by B, KiB, MiB, GiB, or TiB"
        ));
    }

    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, unit) = trimmed.split_at(split_at);

    if digits.is_empty() {
        return Err(format!(
            "invalid byte size \"{value}\": expected an integer optionally followed by B, KiB, MiB, GiB, or TiB"
        ));
    }

    let amount = digits
        .parse::<u64>()
        .map_err(|_| format!("invalid byte size \"{value}\": value is out of range"))?;

    match unit {
        "" | "B" => Ok(amount),
        "KiB" => amount
            .checked_mul(1024)
            .ok_or_else(|| format!("invalid byte size \"{value}\": value is out of range")),
        "MiB" => amount
            .checked_mul(1024 * 1024)
            .ok_or_else(|| format!("invalid byte size \"{value}\": value is out of range")),
        "GiB" => amount
            .checked_mul(1024 * 1024 * 1024)
            .ok_or_else(|| format!("invalid byte size \"{value}\": value is out of range")),
        "TiB" => amount
            .checked_mul(1024 * 1024 * 1024 * 1024)
            .ok_or_else(|| format!("invalid byte size \"{value}\": value is out of range")),
        _ => Err(format!(
            "invalid byte size \"{value}\": expected an integer optionally followed by B, KiB, MiB, GiB, or TiB"
        )),
    }
}

#[allow(dead_code)]
pub(crate) fn parse_key_value(flag: &str, value: &str) -> Result<KeyValuePair, String> {
    let Some((key, parsed_value)) = value.split_once('=') else {
        return Err(format!(
            "invalid {flag} value \"{value}\": expected KEY=VALUE"
        ));
    };

    if key.is_empty() {
        return Err(format!(
            "invalid {flag} value \"{value}\": key must not be empty"
        ));
    }

    Ok(KeyValuePair {
        key: key.to_string(),
        value: parsed_value.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_endpoints() {
        assert_eq!(
            parse_endpoint("unix:///run/crius/crius.sock").unwrap(),
            Endpoint::Unix("/run/crius/crius.sock".into())
        );
        assert_eq!(
            parse_endpoint("/run/crius/crius.sock").unwrap(),
            Endpoint::Unix("/run/crius/crius.sock".into())
        );
        assert_eq!(
            parse_endpoint("http://127.0.0.1:8080").unwrap(),
            Endpoint::Tcp("http://127.0.0.1:8080".into())
        );
        assert_eq!(
            parse_endpoint("https://127.0.0.1:8443").unwrap(),
            Endpoint::Tcp("https://127.0.0.1:8443".into())
        );
    }

    #[test]
    fn rejects_invalid_endpoints() {
        for endpoint in ["", "tcp://127.0.0.1:8080", "unix://"] {
            let error = parse_endpoint(endpoint).expect_err("endpoint should be rejected");
            assert!(error.contains("invalid endpoint"));
            assert!(error.contains(endpoint));
        }
    }

    #[test]
    fn parses_durations_with_supported_units() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3_600));
    }

    #[test]
    fn rejects_invalid_durations() {
        for input in ["", "10", "1.5s", "5d", "ms"] {
            let error = parse_duration(input).expect_err("duration should be rejected");
            assert!(error.contains(&format!("invalid duration \"{input}\"")));
        }

        let overflow = format!("{}h", u64::MAX);
        let error = parse_duration(&overflow).expect_err("duration overflow should be rejected");
        assert!(error.contains("value is out of range"));
    }

    #[test]
    fn parses_byte_sizes() {
        assert_eq!(parse_byte_size("0").unwrap(), 0);
        assert_eq!(parse_byte_size("512").unwrap(), 512);
        assert_eq!(parse_byte_size("512B").unwrap(), 512);
        assert_eq!(parse_byte_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_byte_size("64MiB").unwrap(), 67_108_864);
        assert_eq!(parse_byte_size("1GiB").unwrap(), 1_073_741_824);
        assert_eq!(parse_byte_size("1TiB").unwrap(), 1_099_511_627_776);
    }

    #[test]
    fn rejects_invalid_byte_sizes() {
        for input in ["", "KiB", "abc", "64Mi", "64 MB", "-1"] {
            let error = parse_byte_size(input).expect_err(&format!("{input} should be rejected"));
            assert!(error.contains(&format!("invalid byte size \"{input}\"")), "{error}");
        }

        let overflow = format!("{}TiB", u64::MAX);
        let error = parse_byte_size(&overflow).expect_err("byte size overflow should be rejected");
        assert!(error.contains("value is out of range"));
    }

    #[test]
    fn parses_key_value_pairs() {
        assert_eq!(
            parse_key_value("--env", "Name=").unwrap(),
            KeyValuePair {
                key: "Name".into(),
                value: "".into()
            }
        );
        assert_eq!(
            parse_key_value("--label", "App=Demo").unwrap(),
            KeyValuePair {
                key: "App".into(),
                value: "Demo".into()
            }
        );
    }

    #[test]
    fn rejects_invalid_key_value_pairs() {
        for input in ["", "missing-equals", "=x"] {
            let error = parse_key_value("--env", input).expect_err("key/value should be rejected");
            assert!(error.contains("--env"), "{error}");
            assert!(error.contains(input), "{error}");
        }
    }
}
