use std::{future::Future, path::Path, time::Duration};

use hyper::Uri;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint as TonicEndpoint};
use tower::service_fn;

use crate::crs::{context::CliContext, error::CliError, parsers::Endpoint};
use crate::proto::diagnostics::v1::diagnostics_service_client::DiagnosticsServiceClient;
use crate::proto::local::v1::local_service_client::LocalServiceClient;
use crate::proto::runtime::v1::{
    image_service_client::ImageServiceClient, runtime_service_client::RuntimeServiceClient,
};

#[derive(Clone, Debug)]
pub(crate) struct CrsClient {
    endpoint: Endpoint,
    endpoint_display: String,
    connect_timeout: Duration,
    rpc_timeout: Duration,
    runtime: Option<RuntimeServiceClient<Channel>>,
    image: Option<ImageServiceClient<Channel>>,
    diagnostics: Option<DiagnosticsServiceClient<Channel>>,
    local: Option<LocalServiceClient<Channel>>,
}

impl CrsClient {
    pub(crate) fn new(ctx: &CliContext) -> Self {
        let endpoint = ctx.endpoint().clone();
        let endpoint_display = ctx.endpoint_display();

        Self {
            endpoint,
            endpoint_display,
            connect_timeout: ctx.connect_timeout(),
            rpc_timeout: ctx.rpc_timeout(),
            runtime: None,
            image: None,
            diagnostics: None,
            local: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn connect(ctx: &CliContext) -> Result<Self, CliError> {
        let mut client = Self::new(ctx);
        let channel = client.connect_channel().await?;
        client.runtime = Some(RuntimeServiceClient::new(channel.clone()));
        client.image = Some(ImageServiceClient::new(channel.clone()));
        client.diagnostics = Some(DiagnosticsServiceClient::new(channel.clone()));
        client.local = Some(LocalServiceClient::new(channel));
        Ok(client)
    }

    #[allow(dead_code)]
    async fn connect_channel(&self) -> Result<Channel, CliError> {
        let connect = async {
            match self.endpoint_kind() {
                Endpoint::Unix(path) => connect_unix_channel(path).await,
                Endpoint::Tcp(uri) => connect_tcp_channel(uri).await,
            }
        };

        tokio::time::timeout(self.connect_timeout, connect)
            .await
            .map_err(|_| CliError::timeout("connection timed out", self.endpoint()))?
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint_display
    }

    #[allow(dead_code)]
    pub(crate) fn endpoint_kind(&self) -> &Endpoint {
        &self.endpoint
    }

    #[allow(dead_code)]
    pub(crate) fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    #[allow(dead_code)]
    pub(crate) fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }

    #[allow(dead_code, clippy::result_large_err)]
    pub(crate) fn runtime(&self) -> Result<RuntimeServiceClient<Channel>, CliError> {
        self.runtime.clone().ok_or_else(|| {
            CliError::daemon_unavailable(
                self.endpoint(),
                "runtime service client is not connected; call CrsClient::connect first",
            )
        })
    }

    #[allow(dead_code, clippy::result_large_err)]
    pub(crate) fn image(&self) -> Result<ImageServiceClient<Channel>, CliError> {
        self.image.clone().ok_or_else(|| {
            CliError::daemon_unavailable(
                self.endpoint(),
                "image service client is not connected; call CrsClient::connect first",
            )
        })
    }

    #[allow(dead_code, clippy::result_large_err)]
    pub(crate) fn diagnostics(&self) -> Result<DiagnosticsServiceClient<Channel>, CliError> {
        self.diagnostics
            .clone()
            .ok_or_else(|| self.diagnostics_unavailable())
    }

    #[allow(dead_code, clippy::result_large_err)]
    pub(crate) fn local(&self) -> Result<LocalServiceClient<Channel>, CliError> {
        self.local.clone().ok_or_else(|| {
            CliError::daemon_unavailable(
                self.endpoint(),
                "local service client is not connected; this crius daemon may not support native local containers",
            )
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn with_rpc_timeout<F, T>(&self, future: F) -> Result<T, CliError>
    where
        F: Future<Output = Result<T, CliError>>,
    {
        tokio::time::timeout(self.rpc_timeout, future)
            .await
            .map_err(|_| CliError::timeout("RPC timed out", self.endpoint()))?
    }

    #[allow(dead_code)]
    pub(crate) fn diagnostics_unavailable(&self) -> CliError {
        CliError::diagnostics_unavailable(self.endpoint())
    }
}

async fn connect_unix_channel(path: &str) -> Result<Channel, CliError> {
    if !Path::new(path).exists() {
        return Err(CliError::daemon_unavailable(
            format!("unix://{path}"),
            format!("socket {path} does not exist"),
        ));
    }

    let path = path.to_string();
    TonicEndpoint::try_from("http://[::]:50051")
        .map_err(|source| CliError::internal(format!("failed to build unix channel: {source}")))?
        .connect_with_connector(service_fn(move |_uri: Uri| {
            let path = path.clone();
            async move { UnixStream::connect(path).await }
        }))
        .await
        .map_err(|source| {
            CliError::daemon_unavailable("unix socket", format!("failed to connect: {source}"))
        })
}

async fn connect_tcp_channel(uri: &str) -> Result<Channel, CliError> {
    TonicEndpoint::from_shared(uri.to_string())
        .map_err(|source| CliError::internal(format!("invalid TCP endpoint {uri}: {source}")))?
        .connect()
        .await
        .map_err(|source| CliError::daemon_unavailable(uri, format!("failed to connect: {source}")))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::crs::args::Args;

    use super::*;

    #[test]
    fn builds_client_from_context() {
        let args = Args::try_parse_from([
            "crs",
            "--address",
            "http://127.0.0.1:1234",
            "--connect-timeout",
            "1s",
            "--timeout",
            "2s",
            "version",
        ])
        .expect("args should parse");
        let ctx = CliContext::from_args(&args).expect("context should build");
        let client = CrsClient::new(&ctx);

        assert_eq!(client.endpoint(), "http://127.0.0.1:1234");
        assert_eq!(
            client.endpoint_kind(),
            &Endpoint::Tcp("http://127.0.0.1:1234".into())
        );
        assert_eq!(client.connect_timeout(), Duration::from_secs(1));
        assert_eq!(client.rpc_timeout(), Duration::from_secs(2));
    }

    #[tokio::test]
    async fn maps_rpc_timeout_to_cli_error() {
        let args = Args::try_parse_from(["crs", "--timeout", "1ms", "version"])
            .expect("args should parse");
        let ctx = CliContext::from_args(&args).expect("context should build");
        let client = CrsClient::new(&ctx);

        let error = client
            .with_rpc_timeout(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            })
            .await
            .expect_err("future should time out");

        assert_eq!(error.exit_status().code(), 124);
    }

    #[test]
    fn creates_diagnostics_unavailable_error() {
        let args = Args::try_parse_from(["crs", "version"]).expect("args should parse");
        let ctx = CliContext::from_args(&args).expect("context should build");
        let client = CrsClient::new(&ctx);

        let error = client.diagnostics_unavailable();

        assert!(error
            .to_string()
            .contains("diagnostics service is not available"));
    }

    #[test]
    fn getters_report_unconnected_clients() {
        let args = Args::try_parse_from(["crs", "version"]).expect("args should parse");
        let ctx = CliContext::from_args(&args).expect("context should build");
        let client = CrsClient::new(&ctx);

        assert_eq!(client.runtime().unwrap_err().exit_status().code(), 125);
        assert_eq!(client.image().unwrap_err().exit_status().code(), 125);
        assert_eq!(client.diagnostics().unwrap_err().exit_status().code(), 1);
    }

    #[tokio::test]
    async fn missing_unix_socket_is_daemon_unavailable() {
        let socket_path = tempfile::tempdir()
            .expect("tempdir should be created")
            .path()
            .join("missing.sock");
        let args = Args::try_parse_from([
            "crs",
            "--address",
            socket_path.to_str().expect("path should be utf8"),
            "version",
        ])
        .expect("args should parse");
        let ctx = CliContext::from_args(&args).expect("context should build");

        let error = CrsClient::connect(&ctx)
            .await
            .expect_err("missing socket should fail");

        assert_eq!(error.exit_status().code(), 125);
        assert!(error.to_string().contains("does not exist"));
    }
}
