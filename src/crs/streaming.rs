use nix::sys::termios::{self, SetArg, Termios};
use tokio::sync::mpsc;

use crate::crs::{
    args::{StreamOptions, StreamProtocolArg},
    error::{CliError, CommandResult},
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

pub(crate) async fn exec(options: ExecStreamOptions) -> Result<CommandResult, CliError> {
    run_with_raw_mode(&SystemRawModeBackend, options.tty, || {
        let resize = ResizeEvents::start(options.tty);
        let _initial_size = resize.initial();
        let _resize_events = resize.into_receiver();
        Err(CliError::not_implemented("crs streaming exec"))
    })
}

pub(crate) async fn attach(options: AttachStreamOptions) -> Result<CommandResult, CliError> {
    run_with_raw_mode(&SystemRawModeBackend, options.tty, || {
        let resize = ResizeEvents::start(options.tty);
        let _initial_size = resize.initial();
        let _resize_events = resize.into_receiver();
        Err(CliError::not_implemented("crs streaming attach"))
    })
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct TerminalSize {
    pub width: u16,
    pub height: u16,
}

struct ResizeEvents {
    initial: Option<TerminalSize>,
    receiver: mpsc::Receiver<TerminalSize>,
}

impl ResizeEvents {
    fn start(enabled: bool) -> Self {
        if !enabled {
            return Self::empty();
        }

        let fd = nix::libc::STDIN_FILENO;
        if !nix::unistd::isatty(fd).unwrap_or(false) {
            return Self::empty();
        }

        let initial = terminal_size(fd);
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let Ok(mut signal) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())
            else {
                return;
            };

            while signal.recv().await.is_some() {
                let Some(size) = terminal_size(fd) else {
                    continue;
                };
                if sender.send(size).await.is_err() {
                    break;
                }
            }
        });

        Self { initial, receiver }
    }

    fn empty() -> Self {
        let (_sender, receiver) = mpsc::channel(1);
        Self {
            initial: None,
            receiver,
        }
    }

    fn initial(&self) -> Option<TerminalSize> {
        self.initial
    }

    fn into_receiver(self) -> mpsc::Receiver<TerminalSize> {
        self.receiver
    }
}

fn terminal_size(fd: i32) -> Option<TerminalSize> {
    let mut winsize = nix::libc::winsize {
        ws_row: 0,
        ws_col: 0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let rc = unsafe { nix::libc::ioctl(fd, nix::libc::TIOCGWINSZ, &mut winsize) };
    if rc != 0 || winsize.ws_col == 0 || winsize.ws_row == 0 {
        return None;
    }

    Some(TerminalSize {
        width: winsize.ws_col,
        height: winsize.ws_row,
    })
}

trait RawModeBackend {
    type Guard;

    fn enter_raw_mode(&self, enabled: bool) -> Result<Option<Self::Guard>, CliError>;
}

fn run_with_raw_mode<B, F, T>(backend: &B, enabled: bool, body: F) -> Result<T, CliError>
where
    B: RawModeBackend,
    F: FnOnce() -> Result<T, CliError>,
{
    let _guard = backend.enter_raw_mode(enabled)?;
    body()
}

struct SystemRawModeBackend;

impl RawModeBackend for SystemRawModeBackend {
    type Guard = TtyRawModeGuard;

    fn enter_raw_mode(&self, enabled: bool) -> Result<Option<Self::Guard>, CliError> {
        TtyRawModeGuard::enter(enabled)
    }
}

struct TtyRawModeGuard {
    fd: i32,
    original: Termios,
}

impl TtyRawModeGuard {
    fn enter(enabled: bool) -> Result<Option<Self>, CliError> {
        if !enabled {
            return Ok(None);
        }

        let fd = nix::libc::STDIN_FILENO;
        if !nix::unistd::isatty(fd).unwrap_or(false) {
            return Ok(None);
        }

        let original = termios::tcgetattr(fd).map_err(|source| {
            CliError::internal(format!("failed to read terminal mode: {source}"))
        })?;
        let mut raw = original.clone();
        termios::cfmakeraw(&mut raw);
        termios::tcsetattr(fd, SetArg::TCSANOW, &raw).map_err(|source| {
            CliError::internal(format!("failed to enter terminal raw mode: {source}"))
        })?;

        Ok(Some(Self { fd, original }))
    }
}

impl Drop for TtyRawModeGuard {
    fn drop(&mut self) {
        let _ = termios::tcsetattr(self.fd, SetArg::TCSANOW, &self.original);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

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

    #[test]
    fn raw_mode_guard_restores_after_error() {
        let restored = Arc::new(AtomicUsize::new(0));
        let backend = MockRawModeBackend {
            restored: Arc::clone(&restored),
        };

        let error = run_with_raw_mode(&backend, true, || -> Result<(), CliError> {
            Err(CliError::internal("streaming connection failed"))
        })
        .expect_err("body error should be returned");

        assert!(error.to_string().contains("streaming connection failed"));
        assert_eq!(restored.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn raw_mode_guard_skips_restore_when_disabled() {
        let restored = Arc::new(AtomicUsize::new(0));
        let backend = MockRawModeBackend {
            restored: Arc::clone(&restored),
        };

        run_with_raw_mode(&backend, false, || Ok(())).expect("body should succeed");

        assert_eq!(restored.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn resize_events_skip_cleanly_when_disabled() {
        let events = ResizeEvents::start(false);

        assert_eq!(events.initial(), None);
    }

    #[test]
    fn terminal_size_returns_none_for_invalid_fd() {
        assert_eq!(terminal_size(-1), None);
    }

    struct MockRawModeBackend {
        restored: Arc<AtomicUsize>,
    }

    impl RawModeBackend for MockRawModeBackend {
        type Guard = MockRawModeGuard;

        fn enter_raw_mode(&self, enabled: bool) -> Result<Option<Self::Guard>, CliError> {
            Ok(enabled.then(|| MockRawModeGuard {
                restored: Arc::clone(&self.restored),
            }))
        }
    }

    struct MockRawModeGuard {
        restored: Arc<AtomicUsize>,
    }

    impl Drop for MockRawModeGuard {
        fn drop(&mut self) {
            self.restored.fetch_add(1, Ordering::SeqCst);
        }
    }
}
