use crate::crs::args::OutputArg;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExitStatus {
    Success,
    General,
    Usage,
    Timeout,
}

impl ExitStatus {
    pub fn code(self) -> i32 {
        match self {
            Self::Success => 0,
            Self::General => 1,
            Self::Usage => 2,
            Self::Timeout => 124,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CommandResult {
    exit_code: i32,
}

impl CommandResult {
    pub fn success() -> Self {
        Self {
            exit_code: ExitStatus::Success.code(),
        }
    }

    pub fn failure(status: ExitStatus) -> Self {
        Self {
            exit_code: status.code(),
        }
    }

    pub fn from_code(exit_code: i32) -> Self {
        Self { exit_code }
    }

    pub fn code(self) -> i32 {
        self.exit_code
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("{command} is not implemented yet")]
    NotImplemented { command: String },
    #[error("{message} for endpoint {endpoint}")]
    Timeout { message: String, endpoint: String },
    #[error("diagnostics service is not available from this crius daemon at {endpoint}")]
    DiagnosticsUnavailable { endpoint: String },
}

impl CliError {
    pub(crate) fn not_implemented(command: impl Into<String>) -> Self {
        Self::NotImplemented {
            command: command.into(),
        }
    }

    pub(crate) fn timeout(message: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
            endpoint: endpoint.into(),
        }
    }

    pub(crate) fn diagnostics_unavailable(endpoint: impl Into<String>) -> Self {
        Self::DiagnosticsUnavailable {
            endpoint: endpoint.into(),
        }
    }

    pub(crate) fn exit_status(&self) -> ExitStatus {
        match self {
            Self::NotImplemented { .. } => ExitStatus::General,
            Self::Timeout { .. } => ExitStatus::Timeout,
            Self::DiagnosticsUnavailable { .. } => ExitStatus::General,
        }
    }

    pub(crate) fn render(&self, output: OutputArg) {
        match output {
            OutputArg::Json => eprintln!(r#"{{"error":{{"message":"{}"}}}}"#, self),
            OutputArg::Table | OutputArg::Text => eprintln!("error: {self}"),
        }
    }
}
