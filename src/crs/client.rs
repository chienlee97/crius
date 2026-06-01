use std::{future::Future, time::Duration};

use crate::crs::{context::CliContext, error::CliError, parsers::Endpoint};

#[derive(Clone, Debug)]
pub(crate) struct CrsClient {
    endpoint: Endpoint,
    endpoint_display: String,
    connect_timeout: Duration,
    rpc_timeout: Duration,
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
        }
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

        assert!(error.to_string().contains("diagnostics service is not available"));
    }
}
