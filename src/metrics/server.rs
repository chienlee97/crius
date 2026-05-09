use std::collections::HashSet;
use std::convert::Infallible;
use std::fs::File;
use std::io::BufReader;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use hyper::body::Body;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Method, Request, Response, Server, StatusCode};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig as RustlsServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use tokio::net::{TcpListener as TokioTcpListener, UnixListener};
use tokio_rustls::TlsAcceptor;

use crate::config::MetricsConfig;
use crate::image::ImageMetricsProvider;
use crate::server::RuntimeMetricsProvider;

#[derive(Debug, Clone, Default)]
pub struct MetricsServer {
    pub tcp_bind: Option<SocketAddr>,
    pub socket_path: Option<PathBuf>,
}

#[derive(Clone)]
struct MetricsHandler {
    config: MetricsConfig,
    runtime_provider: RuntimeMetricsProvider,
    image_provider: ImageMetricsProvider,
}

impl MetricsServer {
    pub async fn start(
        config: MetricsConfig,
        runtime_provider: RuntimeMetricsProvider,
        image_provider: ImageMetricsProvider,
    ) -> anyhow::Result<Self> {
        let handler = MetricsHandler {
            config: config.clone(),
            runtime_provider,
            image_provider,
        };

        let mut server = MetricsServer::default();
        if !config.enable {
            return Ok(server);
        }

        let tcp_bind = if config.socket_path.trim().is_empty() || !config.host.trim().is_empty() {
            Some(start_tcp_listener(config.clone(), handler.clone()).await?)
        } else {
            None
        };
        if let Some(addr) = tcp_bind {
            server.tcp_bind = Some(addr);
        }

        if !config.socket_path.trim().is_empty() {
            let socket_path = PathBuf::from(config.socket_path.trim());
            start_uds_listener(socket_path.clone(), handler).await?;
            server.socket_path = Some(socket_path);
        }

        Ok(server)
    }
}

async fn start_tcp_listener(
    config: MetricsConfig,
    handler: MetricsHandler,
) -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind((config.host.as_str(), config.port))?;
    listener.set_nonblocking(true)?;
    let local_addr = listener.local_addr()?;

    if config.enable_tls {
        let tls_config = Arc::new(load_metrics_tls_config(&config)?);
        let listener = TokioTcpListener::from_std(listener)?;
        let acceptor = TlsAcceptor::from(tls_config);
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        log::error!("Metrics TLS listener accept failed: {}", err);
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let handler = handler.clone();
                tokio::spawn(async move {
                    let tls_stream = match acceptor.accept(stream).await {
                        Ok(stream) => stream,
                        Err(err) => {
                            log::error!("Metrics TLS handshake failed: {}", err);
                            return;
                        }
                    };
                    let service = service_fn(move |req| {
                        let handler = handler.clone();
                        async move { Ok::<_, Infallible>(handler.handle(req).await) }
                    });
                    if let Err(err) = hyper::server::conn::Http::new()
                        .serve_connection(tls_stream, service)
                        .await
                    {
                        log::error!("Metrics TLS connection exited: {}", err);
                    }
                });
            }
        });
    } else {
        let make_service = make_service_fn(move |_| {
            let handler = handler.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let handler = handler.clone();
                    async move { Ok::<_, Infallible>(handler.handle(req).await) }
                }))
            }
        });
        let server = Server::from_tcp(listener)?.serve(make_service);
        tokio::spawn(async move {
            if let Err(err) = server.await {
                log::error!("Metrics server exited: {}", err);
            }
        });
    }

    Ok(local_addr)
}

async fn start_uds_listener(socket_path: PathBuf, handler: MetricsHandler) -> anyhow::Result<()> {
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path)?;
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    log::error!("Metrics unix listener accept failed: {}", err);
                    continue;
                }
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let handler = handler.clone();
                    async move { Ok::<_, Infallible>(handler.handle(req).await) }
                });
                if let Err(err) = hyper::server::conn::Http::new()
                    .serve_connection(stream, service)
                    .await
                {
                    log::error!("Metrics unix connection exited: {}", err);
                }
            });
        }
    });
    Ok(())
}

impl MetricsHandler {
    async fn handle(&self, req: Request<Body>) -> Response<Body> {
        if req.method() != Method::GET || req.uri().path() != "/metrics" {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("not found"))
                .unwrap_or_else(|_| Response::new(Body::from("not found")));
        }

        match self.render_metrics().await {
            Ok(body) => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain; version=0.0.4")
                .body(Body::from(body))
                .unwrap_or_else(|_| Response::new(Body::from("internal error"))),
            Err(err) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(err.to_string()))
                .unwrap_or_else(|_| Response::new(Body::from("internal error"))),
        }
    }

    async fn render_metrics(&self) -> anyhow::Result<String> {
        let runtime = self.runtime_provider.snapshot().await;
        let images = self.image_provider.snapshot().await;
        let enabled_collectors: HashSet<&str> = self
            .config
            .collectors
            .iter()
            .map(|collector| collector.trim())
            .filter(|collector| !collector.is_empty())
            .collect();

        let mut output = String::new();
        if enabled_collectors.contains("runtime") {
            output.push_str("# HELP crius_build_info Build information for crius.\n");
            output.push_str("# TYPE crius_build_info gauge\n");
            output.push_str(&format!(
                "crius_build_info{{version=\"{}\"}} 1\n",
                env!("CARGO_PKG_VERSION")
            ));
            output.push_str("# HELP crius_runtime_ready Runtime ready condition.\n");
            output.push_str("# TYPE crius_runtime_ready gauge\n");
            output.push_str(&format!(
                "crius_runtime_ready {}\n",
                bool_to_metric(runtime.runtime_ready)
            ));
            output.push_str("# HELP crius_network_ready Network ready condition.\n");
            output.push_str("# TYPE crius_network_ready gauge\n");
            output.push_str(&format!(
                "crius_network_ready {}\n",
                bool_to_metric(runtime.network_ready)
            ));
        }
        if enabled_collectors.contains("resources") {
            output.push_str("# HELP crius_containers Number of tracked containers.\n");
            output.push_str("# TYPE crius_containers gauge\n");
            output.push_str(&format!("crius_containers {}\n", runtime.container_count));
            output.push_str("# HELP crius_pod_sandboxes Number of tracked pod sandboxes.\n");
            output.push_str("# TYPE crius_pod_sandboxes gauge\n");
            output.push_str(&format!(
                "crius_pod_sandboxes {}\n",
                runtime.pod_sandbox_count
            ));
            output.push_str("# HELP crius_event_subscribers Number of GetEvents subscribers.\n");
            output.push_str("# TYPE crius_event_subscribers gauge\n");
            output.push_str(&format!(
                "crius_event_subscribers {}\n",
                runtime.event_subscriber_count
            ));
            output.push_str("# HELP crius_stats_cache_entries Number of stats cache entries.\n");
            output.push_str("# TYPE crius_stats_cache_entries gauge\n");
            output.push_str(&format!(
                "crius_stats_cache_entries{{kind=\"container\"}} {}\n",
                runtime.container_stats_cache_entries
            ));
            output.push_str(&format!(
                "crius_stats_cache_entries{{kind=\"pod\"}} {}\n",
                runtime.pod_stats_cache_entries
            ));
            output.push_str(&format!(
                "crius_stats_cache_entries{{kind=\"pod_metrics\"}} {}\n",
                runtime.pod_metrics_cache_entries
            ));
        }
        if enabled_collectors.contains("images") {
            output.push_str("# HELP crius_images Number of tracked images.\n");
            output.push_str("# TYPE crius_images gauge\n");
            output.push_str(&format!("crius_images {}\n", images.image_count));
            output.push_str("# HELP crius_image_size_bytes Total logical image size.\n");
            output.push_str("# TYPE crius_image_size_bytes gauge\n");
            output.push_str(&format!(
                "crius_image_size_bytes {}\n",
                images.total_image_size_bytes
            ));
            output.push_str("# HELP crius_image_fs_usage_bytes Image filesystem bytes used.\n");
            output.push_str("# TYPE crius_image_fs_usage_bytes gauge\n");
            output.push_str(&format!(
                "crius_image_fs_usage_bytes {}\n",
                images.image_fs_bytes_used
            ));
            output.push_str("# HELP crius_image_fs_inodes_used Image filesystem inodes used.\n");
            output.push_str("# TYPE crius_image_fs_inodes_used gauge\n");
            output.push_str(&format!(
                "crius_image_fs_inodes_used {}\n",
                images.image_fs_inodes_used
            ));
        }

        Ok(output)
    }
}

fn bool_to_metric(value: bool) -> u8 {
    if value {
        1
    } else {
        0
    }
}

fn load_metrics_tls_config(config: &MetricsConfig) -> anyhow::Result<RustlsServerConfig> {
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
        .map_err(|err| anyhow::anyhow!("failed to configure metrics TLS versions: {}", err))?;
    let builder = if config.tls_ca_file.trim().is_empty() {
        builder.with_no_client_auth()
    } else {
        let mut roots = RootCertStore::empty();
        for cert in load_certificates(Path::new(&config.tls_ca_file))? {
            roots
                .add(cert)
                .map_err(|err| anyhow::anyhow!("failed to add metrics client CA: {}", err))?;
        }
        let verifier =
            WebPkiClientVerifier::builder_with_provider(roots.into(), provider).build()?;
        builder.with_client_cert_verifier(verifier)
    };

    Ok(builder.with_single_cert(certs, key)?)
}

fn tls_versions_from_config(
    config: &MetricsConfig,
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
                        "unsupported metrics TLS cipher suite {}; expected a rustls ring cipher suite name",
                        other
                    );
                }
            };
            Ok(suite)
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::bool_to_metric;

    #[test]
    fn bool_metric_is_stable() {
        assert_eq!(bool_to_metric(false), 0);
        assert_eq!(bool_to_metric(true), 1);
    }
}
