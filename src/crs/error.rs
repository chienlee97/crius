use crate::crs::args::OutputArg;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExitStatus {
    Success,
    General,
    Usage,
}

impl ExitStatus {
    pub fn code(self) -> i32 {
        match self {
            Self::Success => 0,
            Self::General => 1,
            Self::Usage => 2,
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
}

impl CliError {
    pub(crate) fn not_implemented(command: impl Into<String>) -> Self {
        Self::NotImplemented {
            command: command.into(),
        }
    }

    pub(crate) fn exit_status(&self) -> ExitStatus {
        match self {
            Self::NotImplemented { .. } => ExitStatus::General,
        }
    }

    pub(crate) fn render(&self, output: OutputArg) {
        match output {
            OutputArg::Json => eprintln!(r#"{{"error":{{"message":"{}"}}}}"#, self),
            OutputArg::Table | OutputArg::Text => eprintln!("error: {self}"),
        }
    }
}
