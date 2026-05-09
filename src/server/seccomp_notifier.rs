use std::collections::HashMap;
use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::{UnixListener as StdUnixListener, UnixStream as StdUnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use nix::libc;
use serde::{Deserialize, Serialize};

use crate::runtime::SeccompNotifierMode;

const SECCOMP_USER_NOTIF_STOP_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(super) struct SeccompNotificationEvent {
    pub(super) container_id: String,
    pub(super) syscall: String,
    pub(super) stop_mode: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct SeccompNotifierSnapshot {
    pub(super) stop_mode: bool,
    pub(super) socket_path: String,
    pub(super) syscalls: HashMap<String, u64>,
}

#[derive(Debug)]
pub(super) struct SeccompNotifier {
    socket_path: PathBuf,
    stop_mode: bool,
    shutdown: Arc<AtomicBool>,
    syscalls: Arc<StdMutex<HashMap<String, u64>>>,
    accept_thread: Option<JoinHandle<()>>,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct SeccompData {
    nr: i32,
    arch: u32,
    instruction_pointer: u64,
    args: [u64; 6],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct SeccompNotif {
    id: u64,
    pid: u32,
    flags: u32,
    data: SeccompData,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct SeccompNotifResp {
    id: u64,
    val: i64,
    error: i32,
    flags: u32,
}

impl SeccompNotifier {
    pub(super) fn bind(
        socket_path: PathBuf,
        container_id: String,
        mode: SeccompNotifierMode,
        tx: tokio::sync::mpsc::UnboundedSender<SeccompNotificationEvent>,
    ) -> io::Result<Self> {
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&socket_path);
        let listener = StdUnixListener::bind(&socket_path)?;
        listener.set_nonblocking(true)?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let syscalls = Arc::new(StdMutex::new(HashMap::new()));
        let shutdown_for_thread = shutdown.clone();
        let syscalls_for_thread = syscalls.clone();
        let socket_path_for_thread = socket_path.clone();
        let stop_mode = mode == SeccompNotifierMode::Stop;
        let accept_thread = thread::spawn(move || {
            while !shutdown_for_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let tx = tx.clone();
                        let container_id = container_id.clone();
                        let syscalls = syscalls_for_thread.clone();
                        thread::spawn(move || {
                            if let Err(err) = handle_seccomp_stream(
                                stream,
                                &container_id,
                                stop_mode,
                                &tx,
                                &syscalls,
                            ) {
                                log::warn!(
                                    "seccomp notifier stream for {} failed: {}",
                                    container_id,
                                    err
                                );
                            }
                        });
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(err) => {
                        if !shutdown_for_thread.load(Ordering::Relaxed) {
                            log::warn!(
                                "seccomp notifier accept loop for {} failed on {}: {}",
                                container_id,
                                socket_path_for_thread.display(),
                                err
                            );
                        }
                        break;
                    }
                }
            }
        });

        Ok(Self {
            socket_path,
            stop_mode,
            shutdown,
            syscalls,
            accept_thread: Some(accept_thread),
        })
    }

    pub(super) fn snapshot(&self) -> SeccompNotifierSnapshot {
        SeccompNotifierSnapshot {
            stop_mode: self.stop_mode,
            socket_path: self.socket_path.display().to_string(),
            syscalls: self.syscalls.lock().map(|v| v.clone()).unwrap_or_default(),
        }
    }

    pub(super) fn close(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = StdUnixStream::connect(&self.socket_path);
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

impl Drop for SeccompNotifier {
    fn drop(&mut self) {
        self.close();
    }
}

fn handle_seccomp_stream(
    stream: StdUnixStream,
    container_id: &str,
    stop_mode: bool,
    tx: &tokio::sync::mpsc::UnboundedSender<SeccompNotificationEvent>,
    syscall_counts: &Arc<StdMutex<HashMap<String, u64>>>,
) -> io::Result<()> {
    let seccomp_fd = receive_fd(&stream)?;
    let raw_fd = seccomp_fd.as_raw_fd();

    loop {
        let mut notif: SeccompNotif = unsafe { zeroed() };
        let recv_result = unsafe {
            libc::ioctl(
                raw_fd,
                nix::request_code_readwrite!(b'!', 0, size_of::<SeccompNotif>()) as libc::c_ulong,
                &mut notif,
            )
        };
        if recv_result < 0 {
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::ENOENT | libc::EBADF) => break,
                _ => return Err(err),
            }
        }

        let syscall = format!("syscall#{}", notif.data.nr);
        if let Ok(mut counts) = syscall_counts.lock() {
            *counts.entry(syscall.clone()).or_insert(0) += 1;
        }
        let _ = tx.send(SeccompNotificationEvent {
            container_id: container_id.to_string(),
            syscall,
            stop_mode,
        });

        let mut resp = SeccompNotifResp {
            id: notif.id,
            val: 0,
            error: libc::ENOSYS,
            flags: 0,
        };
        let send_result = unsafe {
            libc::ioctl(
                raw_fd,
                nix::request_code_readwrite!(b'!', 1, size_of::<SeccompNotifResp>())
                    as libc::c_ulong,
                &mut resp,
            )
        };
        if send_result < 0 {
            return Err(io::Error::last_os_error());
        }
    }

    Ok(())
}

fn receive_fd(stream: &StdUnixStream) -> io::Result<OwnedFd> {
    let mut data = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: data.as_mut_ptr().cast(),
        iov_len: data.len(),
    };
    let mut control = [0u8; 64];
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = control.len();

    let received = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) };
    if received < 0 {
        return Err(io::Error::last_os_error());
    }

    let header = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if header.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "seccomp notifier message did not include a file descriptor",
        ));
    }
    let header = unsafe { &*header };
    if header.cmsg_level != libc::SOL_SOCKET || header.cmsg_type != libc::SCM_RIGHTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "seccomp notifier message used an unexpected control type",
        ));
    }

    let fd_ptr = unsafe { libc::CMSG_DATA(header as *const _ as *mut _) as *const RawFd };
    let fd = unsafe { *fd_ptr };
    if fd < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "seccomp notifier message carried an invalid fd",
        ));
    }

    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

pub(super) fn seccomp_stop_delay() -> Duration {
    SECCOMP_USER_NOTIF_STOP_DELAY
}
