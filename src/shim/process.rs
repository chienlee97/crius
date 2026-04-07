//! 进程管理 - 容器进程的创建和管理
//!
//! 提供容器进程的启动、监控和清理功能

use anyhow::{Context, Result};
use log::{debug, error, info};
use std::path::Path;
use std::process::{Command, Stdio};

/// 容器进程管理器
pub struct ProcessManager {
    /// Runtime路径
    runtime: std::path::PathBuf,
    /// 容器ID
    container_id: String,
}

impl ProcessManager {
    /// 创建新的进程管理器
    pub fn new(runtime: std::path::PathBuf, container_id: String) -> Self {
        Self {
            runtime,
            container_id,
        }
    }

    /// 创建容器
    pub fn create(&self, bundle: &Path) -> Result<()> {
        info!(
            "Creating container {} with bundle {:?}",
            self.container_id, bundle
        );

        let output = Command::new(&self.runtime)
            .args(&[
                "create",
                "--bundle",
                bundle.to_str().unwrap(),
                &self.container_id,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to execute runc create")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("runc create failed: {}", stderr);
            return Err(anyhow::anyhow!("Failed to create container: {}", stderr));
        }

        debug!("Container {} created successfully", self.container_id);
        Ok(())
    }

    /// 启动容器
    pub fn start(&self) -> Result<()> {
        info!("Starting container {}", self.container_id);

        let output = Command::new(&self.runtime)
            .args(&["start", &self.container_id])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to execute runc start")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // 某些情况下容器可能已经启动，所以只记录警告
            error!("runc start warning: {}", stderr);
        }

        debug!("Container {} started", self.container_id);
        Ok(())
    }

    /// 停止容器
    pub fn stop(&self, timeout: u32) -> Result<()> {
        info!(
            "Stopping container {} with timeout {}",
            self.container_id, timeout
        );

        // 首先尝试发送SIGTERM
        let output = Command::new(&self.runtime)
            .args(&["kill", &self.container_id, "TERM"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to execute runc kill TERM")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("SIGTERM warning: {}", stderr);
        }

        // 等待容器停止
        let start = std::time::Instant::now();
        while start.elapsed().as_secs() < timeout as u64 {
            match self.state() {
                Ok(state) => {
                    if state.status == "stopped" {
                        info!("Container {} stopped gracefully", self.container_id);
                        return Ok(());
                    }
                }
                Err(_) => {
                    // 可能容器已经不存在了
                    return Ok(());
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // 超时后强制停止
        info!(
            "Container {} did not stop gracefully, force killing",
            self.container_id
        );
        let output = Command::new(&self.runtime)
            .args(&["kill", &self.container_id, "KILL"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to execute runc kill KILL")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("SIGKILL warning: {}", stderr);
        }

        Ok(())
    }

    /// 删除容器
    pub fn delete(&self) -> Result<()> {
        info!("Deleting container {}", self.container_id);

        let output = Command::new(&self.runtime)
            .args(&["delete", &self.container_id])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to execute runc delete")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("runc delete warning: {}", stderr);
            // 即使失败也继续，因为容器可能已经被删除了
        }

        debug!("Container {} deleted", self.container_id);
        Ok(())
    }

    /// 在容器中执行命令 (exec)
    pub fn exec(
        &self,
        command: &[String],
        tty: bool,
        stdin: bool,
        stdout: bool,
        stderr: bool,
    ) -> Result<std::process::Child> {
        info!(
            "Executing command in container {}: {:?}",
            self.container_id, command
        );

        let mut cmd = Command::new(&self.runtime);
        cmd.arg("exec");

        if tty {
            cmd.arg("-t");
        }
        if stdin {
            cmd.arg("-i");
        }

        // 添加容器ID
        cmd.arg(&self.container_id);

        // 添加命令
        for arg in command {
            cmd.arg(arg);
        }

        // 设置stdio
        if stdin {
            cmd.stdin(Stdio::piped());
        } else {
            cmd.stdin(Stdio::null());
        }

        if stdout {
            cmd.stdout(Stdio::piped());
        } else {
            cmd.stdout(Stdio::null());
        }

        if stderr {
            cmd.stderr(Stdio::piped());
        } else {
            cmd.stderr(Stdio::null());
        }

        let child = cmd.spawn().context("Failed to spawn runc exec")?;

        debug!("Exec process spawned with PID: {:?}", child.id());
        Ok(child)
    }

    /// 获取容器状态
    pub fn state(&self) -> Result<ContainerState> {
        let output = Command::new(&self.runtime)
            .args(&["state", &self.container_id])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to execute runc state")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("Failed to get container state: {}", stderr));
        }

        let state: ContainerState =
            serde_json::from_slice(&output.stdout).context("Failed to parse container state")?;

        Ok(state)
    }

    /// 获取容器PID
    pub fn pid(&self) -> Result<i32> {
        let state = self.state()?;
        Ok(state.pid)
    }
}

/// 容器状态
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ContainerState {
    pub oci_version: String,
    pub id: String,
    pub status: String,
    pub pid: i32,
    pub bundle: String,
    pub rootfs: String,
    pub created: String,
    pub owner: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_manager_creation() {
        let pm = ProcessManager::new(
            std::path::PathBuf::from("/usr/bin/runc"),
            "test-container".to_string(),
        );
        assert_eq!(pm.container_id, "test-container");
    }
}
