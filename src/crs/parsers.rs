use std::time::Duration;

pub(crate) const DEFAULT_ENDPOINT: &str = "unix:///run/crius/crius.sock";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Endpoint {
    Unix(String),
    Tcp(String),
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

}
