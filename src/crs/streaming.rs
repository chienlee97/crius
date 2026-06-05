use crate::crs::{
    args::{StreamOptions, StreamProtocolArg},
    error::CliError,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum StreamProtocol {
    Websocket,
    Spdy,
}

impl From<StreamProtocolArg> for StreamProtocol {
    fn from(value: StreamProtocolArg) -> Self {
        match value {
            StreamProtocolArg::Websocket => Self::Websocket,
            StreamProtocolArg::Spdy => Self::Spdy,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecStreamOptions {
    pub container_id: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub resize: Option<String>,
    pub protocol: StreamProtocol,
}

impl ExecStreamOptions {
    pub(crate) fn from_args(
        container_id: String,
        command: Vec<String>,
        stream: StreamOptions,
    ) -> Result<Self, CliError> {
        if container_id.trim().is_empty() {
            return Err(CliError::invalid_input("container id must not be empty")
                .with_command("crs container exec"));
        }
        if command.is_empty() {
            return Err(CliError::invalid_input("exec command must not be empty")
                .with_command("crs container exec"));
        }

        Ok(Self {
            container_id,
            command,
            tty: stream.tty,
            stdin: stream.stdin,
            stdout: stream.stdout,
            stderr: stream.stderr && !stream.tty,
            resize: stream.resize,
            protocol: stream.protocol.into(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AttachStreamOptions {
    pub container_id: String,
    pub tty: bool,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub resize: Option<String>,
    pub protocol: StreamProtocol,
}

impl AttachStreamOptions {
    pub(crate) fn from_args(container_id: String, stream: StreamOptions) -> Result<Self, CliError> {
        if container_id.trim().is_empty() {
            return Err(CliError::invalid_input("container id must not be empty")
                .with_command("crs container attach"));
        }
        if !stream.stdin && !stream.stdout && !stream.stderr {
            return Err(CliError::invalid_input(
                "attach requires at least one of stdin, stdout, or stderr",
            )
            .with_command("crs container attach"));
        }

        Ok(Self {
            container_id,
            tty: stream.tty,
            stdin: stream.stdin,
            stdout: stream.stdout,
            stderr: stream.stderr && !stream.tty,
            resize: stream.resize,
            protocol: stream.protocol.into(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortForwardOptions {
    pub pod_id: String,
    pub forwards: Vec<PortForwardSpec>,
    pub protocol: StreamProtocol,
}

impl PortForwardOptions {
    pub(crate) fn from_args(pod_id: String, forward: Vec<String>) -> Result<Self, CliError> {
        if pod_id.trim().is_empty() {
            return Err(CliError::invalid_input("pod id must not be empty")
                .with_command("crs pod port-forward"));
        }

        let forwards = forward
            .iter()
            .map(|value| parse_port_forward(value))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|message| {
                CliError::invalid_input(message).with_command("crs pod port-forward")
            })?;

        if forwards.is_empty() {
            return Err(CliError::invalid_input(
                "pod port-forward requires at least one --forward LOCAL:REMOTE",
            )
            .with_command("crs pod port-forward"));
        }

        Ok(Self {
            pod_id,
            forwards,
            protocol: StreamProtocol::Websocket,
        })
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortForwardSpec {
    pub local: u16,
    pub remote: u16,
}

fn parse_port_forward(value: &str) -> Result<PortForwardSpec, String> {
    let (local, remote) = value
        .split_once(':')
        .ok_or_else(|| format!("invalid port forward \"{value}\": expected LOCAL:REMOTE"))?;

    Ok(PortForwardSpec {
        local: parse_forward_port(value, local)?,
        remote: parse_forward_port(value, remote)?,
    })
}

fn parse_forward_port(source: &str, value: &str) -> Result<u16, String> {
    let port = value
        .parse::<u16>()
        .map_err(|_| format!("invalid port forward \"{source}\": port must be 1-65535"))?;
    if port == 0 {
        return Err(format!(
            "invalid port forward \"{source}\": port must be 1-65535"
        ));
    }
    Ok(port)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_options() -> StreamOptions {
        StreamOptions {
            stdin: true,
            tty: false,
            stdout: true,
            stderr: true,
            resize: Some("80x24".to_string()),
            protocol: StreamProtocolArg::Websocket,
        }
    }

    #[test]
    fn exec_options_disable_stderr_for_tty() {
        let options = ExecStreamOptions::from_args(
            "ctr".to_string(),
            vec!["sh".to_string()],
            StreamOptions {
                tty: true,
                ..stream_options()
            },
        )
        .expect("exec stream options should build");

        assert!(options.tty);
        assert!(options.stdin);
        assert!(options.stdout);
        assert!(!options.stderr);
        assert_eq!(options.protocol, StreamProtocol::Websocket);
    }

    #[test]
    fn exec_options_reject_empty_command() {
        let error = ExecStreamOptions::from_args("ctr".to_string(), Vec::new(), stream_options())
            .expect_err("empty command should fail");

        assert_eq!(error.exit_status().code(), 2);
        assert!(error.to_string().contains("exec command must not be empty"));
    }

    #[test]
    fn attach_options_require_at_least_one_stream() {
        let error = AttachStreamOptions::from_args(
            "ctr".to_string(),
            StreamOptions {
                stdin: false,
                stdout: false,
                stderr: false,
                ..stream_options()
            },
        )
        .expect_err("disabled attach streams should fail");

        assert_eq!(error.exit_status().code(), 2);
    }

    #[test]
    fn port_forward_options_parse_local_and_remote_ports() {
        let options = PortForwardOptions::from_args(
            "pod".to_string(),
            vec!["8080:80".to_string(), "8443:443".to_string()],
        )
        .expect("port forward options should build");

        assert_eq!(
            options.forwards,
            vec![
                PortForwardSpec {
                    local: 8080,
                    remote: 80
                },
                PortForwardSpec {
                    local: 8443,
                    remote: 443
                }
            ]
        );
        assert_eq!(options.protocol, StreamProtocol::Websocket);
    }

    #[test]
    fn port_forward_options_reject_invalid_ports() {
        let error = PortForwardOptions::from_args("pod".to_string(), vec!["8080:0".to_string()])
            .expect_err("zero port should fail");

        assert_eq!(error.exit_status().code(), 2);
        assert!(error.to_string().contains("port must be 1-65535"));
    }
}
