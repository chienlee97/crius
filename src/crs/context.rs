use crate::crs::args::{Args, OutputArg};
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct CliContext {
    endpoint: String,
    connect_timeout: Duration,
    rpc_timeout: Duration,
    output: OutputArg,
    quiet: bool,
    no_trunc: bool,
    debug: bool,
}

impl CliContext {
    pub(crate) fn from_args(args: &Args) -> Self {
        Self {
            endpoint: args.address.clone(),
            connect_timeout: args.connect_timeout.clone(),
            rpc_timeout: args.timeout.clone(),
            output: args.output,
            quiet: args.quiet,
            no_trunc: args.no_trunc,
            debug: args.debug,
        }
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
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
