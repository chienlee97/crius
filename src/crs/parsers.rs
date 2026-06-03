use std::{fs, path::Path, time::Duration};

use base64::Engine;

use crate::proto::runtime::v1::AuthConfig;

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

#[allow(dead_code)]
pub(crate) fn parse_env_file(path: impl AsRef<Path>) -> Result<Vec<KeyValuePair>, String> {
    let path = path.as_ref();
    let source = path.display().to_string();
    let content = fs::read_to_string(path)
        .map_err(|error| format!("failed to read env file \"{source}\": {error}"))?;

    content
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                None
            } else {
                Some((index + 1, line))
            }
        })
        .map(|(line_number, line)| {
            parse_key_value("env file", line).map_err(|error| {
                format!("invalid env file \"{source}\" line {line_number}: {error}")
            })
        })
        .collect()
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthConfigJson {
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    auth: String,
    #[serde(default, alias = "server")]
    server_address: String,
    #[serde(default)]
    identity_token: String,
    #[serde(default)]
    registry_token: String,
}

#[derive(serde::Deserialize)]
struct DockerAuthFile {
    auths: std::collections::BTreeMap<String, DockerAuthEntry>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DockerAuthEntry {
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    auth: String,
    #[serde(default)]
    identity_token: String,
    #[serde(default)]
    registry_token: String,
}

impl From<AuthConfigJson> for AuthConfig {
    fn from(value: AuthConfigJson) -> Self {
        Self {
            username: value.username,
            password: value.password,
            auth: value.auth,
            server_address: value.server_address,
            identity_token: value.identity_token,
            registry_token: value.registry_token,
        }
    }
}

#[allow(dead_code)]
pub(crate) fn parse_auth_json(source: &str, value: &str) -> Result<AuthConfig, String> {
    if let Ok(config) = serde_json::from_str::<AuthConfigJson>(value) {
        return Ok(config.into());
    }

    let docker = serde_json::from_str::<DockerAuthFile>(value)
        .map_err(|error| format!("invalid auth JSON from {source}: {error}"))?;

    let Some((server, entry)) = docker.auths.into_iter().next() else {
        return Err(format!("invalid auth JSON from {source}: auths must not be empty"));
    };

    let (username, password) = if !entry.auth.is_empty()
        && (entry.username.is_empty() || entry.password.is_empty())
    {
        decode_docker_auth(source, &entry.auth)?
    } else {
        (entry.username, entry.password)
    };

    Ok(AuthConfig {
        username,
        password,
        auth: entry.auth,
        server_address: server,
        identity_token: entry.identity_token,
        registry_token: entry.registry_token,
    })
}

fn decode_docker_auth(source: &str, auth: &str) -> Result<(String, String), String> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(auth)
        .map_err(|error| format!("invalid auth JSON from {source}: invalid docker auth: {error}"))?;
    let decoded = String::from_utf8(decoded)
        .map_err(|error| format!("invalid auth JSON from {source}: invalid docker auth: {error}"))?;
    let Some((username, password)) = decoded.split_once(':') else {
        return Err(format!(
            "invalid auth JSON from {source}: docker auth must decode to username:password"
        ));
    };

    Ok((username.to_string(), password.to_string()))
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

    #[test]
    fn parses_env_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("env.list");
        std::fs::write(&path, "# comment\n\nFOO=bar\nEMPTY=\nName=Value\n").unwrap();

        let entries = parse_env_file(&path).unwrap();

        assert_eq!(
            entries,
            vec![
                KeyValuePair {
                    key: "FOO".into(),
                    value: "bar".into(),
                },
                KeyValuePair {
                    key: "EMPTY".into(),
                    value: "".into(),
                },
                KeyValuePair {
                    key: "Name".into(),
                    value: "Value".into(),
                },
            ]
        );
    }

    #[test]
    fn rejects_invalid_env_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("env.list");
        std::fs::write(&path, "=secret\n").unwrap();

        let error = parse_env_file(&path).expect_err("invalid env file should be rejected");
        assert!(error.contains("env file"), "{error}");
        assert!(error.contains("=secret"), "{error}");

        let missing = dir.path().join("missing.env");
        let error = parse_env_file(&missing).expect_err("missing env file should be rejected");
        assert!(error.contains("failed to read env file"), "{error}");
    }

    #[test]
    fn parses_cri_auth_json() {
        let auth = parse_auth_json(
            "inline",
            r#"{"username":"u","password":"p","serverAddress":"registry.example","identityToken":"id","registryToken":"rt"}"#,
        )
        .unwrap();

        assert_eq!(auth.username, "u");
        assert_eq!(auth.password, "p");
        assert_eq!(auth.server_address, "registry.example");
        assert_eq!(auth.identity_token, "id");
        assert_eq!(auth.registry_token, "rt");
    }

    #[test]
    fn parses_docker_auth_json() {
        let auth = parse_auth_json(
            "config.json",
            r#"{"auths":{"registry.example":{"auth":"dXNlcjpzZWNyZXQ="}}}"#,
        )
        .unwrap();

        assert_eq!(auth.server_address, "registry.example");
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "secret");
        assert_eq!(auth.auth, "dXNlcjpzZWNyZXQ=");
    }

    #[test]
    fn rejects_invalid_auth_json_without_leaking_secret() {
        let error = parse_auth_json("inline", r#"{"auths":{"r":{"auth":"not a secret token"}}}"#)
            .expect_err("invalid auth JSON should be rejected");

        assert!(error.contains("inline"), "{error}");
        assert!(!error.contains("not a secret token"), "{error}");
    }
}
