use std::time::Duration;

use crate::crs::{
    args::{Args, OutputArg},
    parsers::{parse_endpoint, Endpoint},
};

#[derive(Clone, Debug)]
pub(crate) struct CliContext {
    endpoint: Endpoint,
    connect_timeout: Duration,
    rpc_timeout: Duration,
    output: OutputArg,
    quiet: bool,
    no_trunc: bool,
    debug: bool,
}

impl CliContext {
    pub(crate) fn from_args(args: &Args) -> Result<Self, String> {
        let endpoint = parse_endpoint(&args.address)?;

        Ok(Self {
            endpoint,
            connect_timeout: args.connect_timeout,
            rpc_timeout: args.timeout,
            output: args.output,
            quiet: args.quiet,
            no_trunc: args.no_trunc,
            debug: args.debug,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub(crate) fn endpoint_display(&self) -> String {
        self.endpoint.to_string()
    }

    pub(crate) fn output(&self) -> OutputArg {
        self.output
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
    pub(crate) fn quiet(&self) -> bool {
        self.quiet
    }

    #[allow(dead_code)]
    pub(crate) fn no_trunc(&self) -> bool {
        self.no_trunc
    }

    #[allow(dead_code)]
    pub(crate) fn debug(&self) -> bool {
        self.debug
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn builds_context_from_args() {
        let args = Args::try_parse_from([
            "crs",
            "--address",
            "/tmp/crius.sock",
            "--connect-timeout",
            "500ms",
            "--timeout",
            "5s",
            "--debug",
            "--quiet",
            "--no-trunc",
            "version",
        ])
        .expect("args should parse");

        let ctx = CliContext::from_args(&args).expect("context should build");

        assert_eq!(ctx.endpoint(), &Endpoint::Unix("/tmp/crius.sock".into()));
        assert_eq!(ctx.connect_timeout(), Duration::from_millis(500));
        assert_eq!(ctx.rpc_timeout(), Duration::from_secs(5));
        assert!(ctx.debug());
        assert!(ctx.quiet());
        assert!(ctx.no_trunc());
    }
}
