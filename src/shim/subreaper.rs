//! 子进程收割（Subreaper）功能
//!
//! 当容器进程的父进程（runc）退出后，容器进程会被重新父进程化为shim。
//! 通过设置PR_SET_CHILD_SUBREAPER，shim可以监控所有后代进程的状态。

use anyhow::Result;
use log::{debug, error, info, warn};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;

/// 子进程收割器
pub struct SubReaper;

impl SubReaper {
    /// 启用子进程收割
    ///
    /// 当调用进程及其子进程退出时，其孙进程会被重新父进程化为调用进程
    pub fn enable() -> Result<()> {
        const PR_SET_CHILD_SUBREAPER: i32 = 36;
        let result = unsafe { libc::prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };

        if result != 0 {
            return Err(anyhow::anyhow!(
                "Failed to set child subreaper: {}",
                std::io::Error::last_os_error()
            ));
        }

        info!("Subreaper enabled for current process");
        Ok(())
    }

    /// 禁用子进程收割
    pub fn disable() -> Result<()> {
        const PR_SET_CHILD_SUBREAPER: i32 = 36;
        let result = unsafe { libc::prctl(PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0) };

        if result != 0 {
            return Err(anyhow::anyhow!(
                "Failed to disable child subreaper: {}",
                std::io::Error::last_os_error()
            ));
        }

        info!("Subreaper disabled");
        Ok(())
    }

    /// 等待任何子进程状态变化
    ///
    /// 返回：(pid, exit_code, 是否被信号终止)
    pub fn wait_any_child() -> Result<Option<(Pid, i32, bool)>> {
        match waitpid(None, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(pid, code)) => {
                debug!("Child {} exited with code {}", pid, code);
                Ok(Some((pid, code, false)))
            }
            Ok(WaitStatus::Signaled(pid, signal, _)) => {
                debug!("Child {} killed by signal {:?}", pid, signal);
                let exit_code = 128 + signal as i32;
                Ok(Some((pid, exit_code, true)))
            }
            Ok(_) => {
                // 仍在运行或其他状态
                Ok(None)
            }
            Err(nix::errno::Errno::ECHILD) => {
                // 没有子进程
                Ok(None)
            }
            Err(e) => {
                error!("Error waiting for child: {}", e);
                Err(anyhow::anyhow!("Wait failed: {}", e))
            }
        }
    }

    /// 等待特定PID的进程
    ///
    /// 如果设置了nonblock，不会阻塞
    pub fn wait_pid(pid: Pid, nonblock: bool) -> Result<Option<(i32, bool)>> {
        let flags = if nonblock {
            Some(WaitPidFlag::WNOHANG)
        } else {
            None
        };

        match waitpid(Some(pid), flags) {
            Ok(WaitStatus::Exited(_, code)) => {
                debug!("Process {} exited with code {}", pid, code);
                Ok(Some((code, false)))
            }
            Ok(WaitStatus::Signaled(_, signal, _)) => {
                debug!("Process {} killed by signal {:?}", pid, signal);
                let exit_code = 128 + signal as i32;
                Ok(Some((exit_code, true)))
            }
            Ok(_) if nonblock => {
                // 仍在运行
                Ok(None)
            }
            Ok(_) => {
                // 不应该到达这里（阻塞模式下应该有明确结果）
                warn!("Unexpected wait status for process {}", pid);
                Ok(None)
            }
            Err(nix::errno::Errno::ECHILD) => {
                // 进程不存在或已经结束
                debug!("No child process with PID {} found", pid);
                Ok(None)
            }
            Err(e) => {
                error!("Error waiting for process {}: {}", pid, e);
                Err(anyhow::anyhow!("Wait failed: {}", e))
            }
        }
    }

    /// 收割所有已退出的子进程（非阻塞）
    ///
    /// 返回收割的子进程数量
    pub fn reap_all_children() -> Result<usize> {
        let mut count = 0;

        while let Some((pid, code, signaled)) = Self::wait_any_child()? {
            if signaled {
                info!("Reaped child {} killed by signal, exit code {}", pid, code);
            } else {
                info!("Reaped child {} exited with code {}", pid, code);
            }
            count += 1;
        }

        if count > 0 {
            debug!("Reaped {} zombie children", count);
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn get_child_subreaper() -> anyhow::Result<bool> {
        const PR_GET_CHILD_SUBREAPER: i32 = 37;
        let mut value = 0i32;
        let result = unsafe { libc::prctl(PR_GET_CHILD_SUBREAPER, &mut value, 0, 0, 0) };
        if result != 0 {
            return Err(anyhow::anyhow!(
                "Failed to get subreaper status: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(value != 0)
    }

    #[test]
    fn test_subreaper_enable_disable() {
        // 注意：这个测试需要root权限
        // 在非root环境下可能会失败
        if unsafe { nix::libc::getuid() } != 0 {
            eprintln!("Skipping subreaper test: requires root");
            return;
        }

        // 启用subreaper
        SubReaper::enable().expect("Failed to enable subreaper");

        // 检查是否设置成功
        let is_subreaper = get_child_subreaper().expect("Failed to get subreaper status");
        assert!(is_subreaper, "Subreaper should be enabled");

        // 禁用subreaper
        SubReaper::disable().expect("Failed to disable subreaper");

        let is_subreaper = get_child_subreaper().expect("Failed to get subreaper status");
        assert!(!is_subreaper, "Subreaper should be disabled");
    }

    #[test]
    fn test_reap_no_children() {
        // 确保没有僵尸进程等待
        let count = SubReaper::reap_all_children().expect("Failed to reap children");
        // 可能没有子进程，也可能有系统留下的僵尸
        // 所以不断言具体值，只确保不panic
        println!("Reaped {} children", count);
    }

    #[test]
    fn test_wait_pid_returns_child_exit_code() {
        let child = Command::new("sh")
            .arg("-c")
            .arg("exit 7")
            .spawn()
            .expect("failed to spawn child process");
        let pid = Pid::from_raw(child.id() as i32);

        let waited = SubReaper::wait_pid(pid, false).expect("failed to wait for child");

        assert_eq!(waited, Some((7, false)));
    }
}
