use crate::crs::args::OutputArg;
use serde_json::json;
use tonic::Code;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExitStatus {
    Success,
    General,
    Usage,
    NotFound,
    AlreadyExists,
    FailedPrecondition,
    PermissionDenied,
    Timeout,
    Unavailable,
    Interrupted,
}

impl ExitStatus {
    pub fn code(self) -> i32 {
        match self {
            Self::Success => 0,
            Self::General => 1,
            Self::Usage => 2,
            Self::NotFound => 4,
            Self::AlreadyExists => 5,
            Self::FailedPrecondition => 6,
            Self::PermissionDenied => 13,
            Self::Timeout => 124,
            Self::Unavailable => 125,
            Self::Interrupted => 130,
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

    pub fn container_exit(code: i32) -> Self {
        Self { exit_code: code }
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
    #[error("daemon is unavailable at {endpoint}: {message}")]
    DaemonUnavailable { endpoint: String, message: String },
    #[error("daemon returned {code:?}: {message}")]
    Grpc { code: Code, message: String },
    #[error("operation interrupted")]
    Interrupted,
    #[error("{message}")]
    Internal { message: String },
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

    pub(crate) fn daemon_unavailable(
        endpoint: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::DaemonUnavailable {
            endpoint: endpoint.into(),
            message: message.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn from_tonic_status(status: tonic::Status) -> Self {
        Self::Grpc {
            code: status.code(),
            message: status.message().to_string(),
        }
    }

    pub(crate) fn exit_status(&self) -> ExitStatus {
        match self {
            Self::NotImplemented { .. } => ExitStatus::General,
            Self::Timeout { .. } => ExitStatus::Timeout,
            Self::DiagnosticsUnavailable { .. } => ExitStatus::General,
            Self::DaemonUnavailable { .. } => ExitStatus::Unavailable,
            Self::Grpc { code, .. } => match code {
                Code::NotFound => ExitStatus::NotFound,
                Code::AlreadyExists => ExitStatus::AlreadyExists,
                Code::FailedPrecondition => ExitStatus::FailedPrecondition,
                Code::PermissionDenied => ExitStatus::PermissionDenied,
                Code::DeadlineExceeded => ExitStatus::Timeout,
                Code::Unavailable => ExitStatus::Unavailable,
                _ => ExitStatus::General,
            },
            Self::Interrupted => ExitStatus::Interrupted,
            Self::Internal { .. } => ExitStatus::General,
        }
    }

    pub(crate) fn render(&self, output: OutputArg) {
        eprintln!("{}", self.render_to_string(output));
    }

    pub(crate) fn render_to_string(&self, output: OutputArg) -> String {
        let message = redact_sensitive(&self.to_string());
        match output {
            OutputArg::Json => json!({
                "error": {
                    "message": message,
                    "exitCode": self.exit_status().code(),
                    "grpcCode": self.grpc_code_name(),
                }
            })
            .to_string(),
            OutputArg::Table | OutputArg::Text => format!("error: {message}"),
        }
    }

    fn grpc_code_name(&self) -> Option<String> {
        match self {
            Self::Grpc { code, .. } => Some(format!("{code:?}")),
            _ => None,
        }
    }
}

pub(crate) fn redact_sensitive(input: &str) -> String {
    input
        .split_whitespace()
        .map(redact_sensitive_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn redact_sensitive_token(token: &str) -> String {
    let Some((key, _value)) = token.split_once('=') else {
        return token.to_string();
    };
    let normalized = key
        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
        .to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "password" | "token" | "identity-token" | "identity_token" | "registry-token"
            | "registry_token" | "auth-json" | "auth_json"
    ) {
        format!("{key}=<redacted>")
    } else {
        token.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_exit_status_codes() {
        assert_eq!(ExitStatus::Success.code(), 0);
        assert_eq!(ExitStatus::General.code(), 1);
        assert_eq!(ExitStatus::Usage.code(), 2);
        assert_eq!(ExitStatus::NotFound.code(), 4);
        assert_eq!(ExitStatus::AlreadyExists.code(), 5);
        assert_eq!(ExitStatus::FailedPrecondition.code(), 6);
        assert_eq!(ExitStatus::PermissionDenied.code(), 13);
        assert_eq!(ExitStatus::Timeout.code(), 124);
        assert_eq!(ExitStatus::Unavailable.code(), 125);
        assert_eq!(ExitStatus::Interrupted.code(), 130);
    }

    #[test]
    fn maps_tonic_status_codes() {
        let cases = [
            (Code::NotFound, 4),
            (Code::AlreadyExists, 5),
            (Code::FailedPrecondition, 6),
            (Code::PermissionDenied, 13),
            (Code::DeadlineExceeded, 124),
            (Code::Unavailable, 125),
            (Code::Internal, 1),
        ];

        for (code, expected) in cases {
            let error = CliError::from_tonic_status(tonic::Status::new(code, "daemon message"));
            assert_eq!(error.exit_status().code(), expected);
        }
    }

    #[test]
    fn preserves_container_exit_code() {
        assert_eq!(CommandResult::container_exit(7).code(), 7);
    }

    #[test]
    fn renders_json_errors_to_stderr_shape() {
        let error =
            CliError::from_tonic_status(tonic::Status::new(Code::NotFound, "container missing"));

        let rendered = error.render_to_string(OutputArg::Json);
        let value: serde_json::Value = serde_json::from_str(&rendered).expect("json error");

        assert_eq!(value["error"]["exitCode"], 4);
        assert_eq!(value["error"]["grpcCode"], "NotFound");
        assert_eq!(value["error"]["message"], "daemon returned NotFound: container missing");
    }

    #[test]
    fn redacts_sensitive_fields() {
        let input = "password=secret token=abc identity_token=xyz registry-token=def normal=value";

        let redacted = redact_sensitive(input);

        assert_eq!(
            redacted,
            "password=<redacted> token=<redacted> identity_token=<redacted> registry-token=<redacted> normal=value"
        );
    }
}
