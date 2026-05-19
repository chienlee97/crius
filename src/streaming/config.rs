use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig as RustlsServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize, Serializer};

const DEFAULT_STREAMING_ADDRESS: &str = "127.0.0.1";
const DEFAULT_STREAMING_PORT: u16 = 0;
const DEFAULT_STREAMING_REQUEST_TTL: Duration = Duration::from_secs(30);
const DEFAULT_PORT_FORWARD_STREAM_CREATION_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_PORT_FORWARD_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);
const DEFAULT_TLS_MIN_VERSION: &str = "VersionTLS12";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StreamingConfig {
    pub address: String,
    pub port: u16,
    pub enable_tls: bool,
    pub tls_cert_file: String,
    pub tls_key_file: String,
    pub tls_ca_file: String,
    pub tls_min_version: String,
    pub tls_cipher_suites: Vec<String>,
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub request_token_ttl: Duration,
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub port_forward_stream_creation_timeout: Duration,
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub port_forward_idle_timeout: Duration,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            address: DEFAULT_STREAMING_ADDRESS.to_string(),
            port: DEFAULT_STREAMING_PORT,
            enable_tls: false,
            tls_cert_file: String::new(),
            tls_key_file: String::new(),
            tls_ca_file: String::new(),
            tls_min_version: DEFAULT_TLS_MIN_VERSION.to_string(),
            tls_cipher_suites: Vec::new(),
            request_token_ttl: DEFAULT_STREAMING_REQUEST_TTL,
            port_forward_stream_creation_timeout: DEFAULT_PORT_FORWARD_STREAM_CREATION_TIMEOUT,
            port_forward_idle_timeout: DEFAULT_PORT_FORWARD_STREAM_IDLE_TIMEOUT,
        }
    }
}

impl StreamingConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.address.trim().is_empty() {
            anyhow::bail!("api.streaming.address must not be empty");
        }
        if self.enable_tls {
            if self.tls_cert_file.trim().is_empty() {
                anyhow::bail!("api.streaming.tls_cert_file must not be empty when TLS is enabled");
            }
            if self.tls_key_file.trim().is_empty() {
                anyhow::bail!("api.streaming.tls_key_file must not be empty when TLS is enabled");
            }
            if !Path::new(self.tls_cert_file.trim()).is_absolute() {
                anyhow::bail!("api.streaming.tls_cert_file must be an absolute path");
            }
            if !Path::new(self.tls_key_file.trim()).is_absolute() {
                anyhow::bail!("api.streaming.tls_key_file must be an absolute path");
            }
        }
        if !self.tls_ca_file.trim().is_empty() && !Path::new(self.tls_ca_file.trim()).is_absolute()
        {
            anyhow::bail!("api.streaming.tls_ca_file must be an absolute path");
        }
        if !matches!(self.tls_min_version.trim(), "VersionTLS12" | "VersionTLS13") {
            anyhow::bail!("api.streaming.tls_min_version must be VersionTLS12 or VersionTLS13");
        }
        resolve_tls_cipher_suites(&self.tls_cipher_suites)?;
        if self.request_token_ttl.is_zero() {
            anyhow::bail!("api.streaming.request_token_ttl must be greater than zero");
        }
        if self.port_forward_stream_creation_timeout.is_zero() {
            anyhow::bail!(
                "api.streaming.port_forward_stream_creation_timeout must be greater than zero"
            );
        }
        Ok(())
    }

    pub(super) fn bind_target(&self) -> (&str, u16) {
        (self.address.as_str(), self.port)
    }

    pub(super) fn base_url(&self, local_addr: std::net::SocketAddr) -> String {
        format!(
            "{}://{}:{}",
            if self.enable_tls { "https" } else { "http" },
            format_http_host(&self.address),
            local_addr.port()
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DurationValue {
    String(String),
    Seconds(u64),
}

pub(crate) fn deserialize_duration<'de, D>(
    deserializer: D,
) -> std::result::Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value = DurationValue::deserialize(deserializer)?;
    parse_duration_value(value).map_err(serde::de::Error::custom)
}

pub(crate) fn serialize_duration<S>(
    duration: &Duration,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format_duration(*duration))
}

fn parse_duration_value(value: DurationValue) -> anyhow::Result<Duration> {
    match value {
        DurationValue::Seconds(seconds) => Ok(Duration::from_secs(seconds)),
        DurationValue::String(raw) => parse_duration(&raw),
    }
}

pub(crate) fn parse_duration(raw: &str) -> anyhow::Result<Duration> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("duration must not be empty");
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Ok(Duration::from_secs(seconds));
    }

    let (value, unit) = trimmed
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit())
        .map(|(idx, _)| trimmed.split_at(idx))
        .ok_or_else(|| anyhow::anyhow!("duration is missing a unit"))?;
    let amount = value
        .parse::<u64>()
        .map_err(|err| anyhow::anyhow!("invalid duration value {trimmed}: {err}"))?;

    match unit {
        "ms" => Ok(Duration::from_millis(amount)),
        "s" => Ok(Duration::from_secs(amount)),
        "m" => Ok(Duration::from_secs(amount.saturating_mul(60))),
        "h" => Ok(Duration::from_secs(amount.saturating_mul(60 * 60))),
        _ => anyhow::bail!("unsupported duration unit {unit}; use ms, s, m or h"),
    }
}

pub(crate) fn format_duration(duration: Duration) -> String {
    if duration.is_zero() {
        return "0s".to_string();
    }
    let millis = duration.as_millis();
    if millis % (60 * 60 * 1000) as u128 == 0 {
        format!("{}h", millis / (60 * 60 * 1000) as u128)
    } else if millis % (60 * 1000) as u128 == 0 {
        format!("{}m", millis / (60 * 1000) as u128)
    } else if millis % 1000 == 0 {
        format!("{}s", millis / 1000)
    } else {
        format!("{}ms", millis)
    }
}

fn format_http_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') && !host.ends_with(']') {
        format!("[{}]", host)
    } else {
        host.to_string()
    }
}

fn resolve_tls_cipher_suites(
    configured: &[String],
) -> anyhow::Result<Vec<rustls::SupportedCipherSuite>> {
    use rustls::crypto::ring::cipher_suite;

    configured
        .iter()
        .map(|entry| entry.trim())
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let suite = match entry {
                "TLS13_AES_256_GCM_SHA384" => cipher_suite::TLS13_AES_256_GCM_SHA384,
                "TLS13_AES_128_GCM_SHA256" => cipher_suite::TLS13_AES_128_GCM_SHA256,
                "TLS13_CHACHA20_POLY1305_SHA256" => cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
                "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" => {
                    cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
                }
                "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => {
                    cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
                }
                "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256" => {
                    cipher_suite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
                }
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" => {
                    cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
                }
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => {
                    cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
                }
                "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256" => {
                    cipher_suite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
                }
                other => {
                    anyhow::bail!(
                        "unsupported streaming TLS cipher suite {}; expected a rustls ring cipher suite name",
                        other
                    );
                }
            };
            Ok(suite)
        })
        .collect()
}

pub(super) fn load_streaming_tls_config(
    config: &StreamingConfig,
) -> anyhow::Result<RustlsServerConfig> {
    let certs = load_certificates(Path::new(&config.tls_cert_file))?;
    let key = load_private_key(Path::new(&config.tls_key_file))?;
    let versions = tls_versions_from_config(config)?;
    let mut provider = rustls::crypto::ring::default_provider();
    let cipher_suites = resolve_tls_cipher_suites(&config.tls_cipher_suites)?;
    if !cipher_suites.is_empty() {
        provider.cipher_suites = cipher_suites;
    }
    let provider = Arc::new(provider);
    let builder = RustlsServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&versions)
        .map_err(|err| anyhow::anyhow!("failed to configure streaming TLS versions: {}", err))?;
    let builder = if config.tls_ca_file.trim().is_empty() {
        builder.with_no_client_auth()
    } else {
        let mut roots = RootCertStore::empty();
        for cert in load_certificates(Path::new(&config.tls_ca_file))? {
            roots
                .add(cert)
                .map_err(|err| anyhow::anyhow!("failed to add client CA certificate: {}", err))?;
        }
        let verifier =
            WebPkiClientVerifier::builder_with_provider(roots.into(), provider).build()?;
        builder.with_client_cert_verifier(verifier)
    };

    Ok(builder.with_single_cert(certs, key)?)
}

fn tls_versions_from_config(
    config: &StreamingConfig,
) -> anyhow::Result<Vec<&'static rustls::SupportedProtocolVersion>> {
    match config.tls_min_version.trim() {
        "VersionTLS12" => Ok(vec![&rustls::version::TLS13, &rustls::version::TLS12]),
        "VersionTLS13" => Ok(vec![&rustls::version::TLS13]),
        other => Err(anyhow::anyhow!(
            "unsupported TLS minimum version {}; expected VersionTLS12 or VersionTLS13",
            other
        )),
    }
}

fn load_certificates(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).map_err(|err| {
        anyhow::anyhow!(
            "failed to open certificate file {}: {}",
            path.display(),
            err
        )
    })?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> =
        certs(&mut reader).collect::<std::result::Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        anyhow::bail!(
            "certificate file {} did not contain any certificates",
            path.display()
        );
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(|err| {
        anyhow::anyhow!(
            "failed to open private key file {}: {}",
            path.display(),
            err
        )
    })?;
    let mut reader = BufReader::new(file);
    if let Some(key) = pkcs8_private_keys(&mut reader)
        .next()
        .transpose()?
        .map(PrivateKeyDer::Pkcs8)
    {
        return Ok(key);
    }

    let file = File::open(path).map_err(|err| {
        anyhow::anyhow!(
            "failed to reopen private key file {}: {}",
            path.display(),
            err
        )
    })?;
    let mut reader = BufReader::new(file);
    if let Some(key) = rsa_private_keys(&mut reader)
        .next()
        .transpose()?
        .map(PrivateKeyDer::Pkcs1)
    {
        return Ok(key);
    }

    anyhow::bail!(
        "private key file {} did not contain a PKCS#8 or RSA private key",
        path.display()
    )
}
