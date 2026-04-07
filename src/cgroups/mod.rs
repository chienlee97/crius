//! Cgroups资源限制模块
//!
//! 提供容器资源限制管理功能，支持cgroups v1和v2

use anyhow::{Context, Result};
use log::{debug, info};
use std::path::PathBuf;

/// 资源限制配置
#[derive(Debug, Clone, Default)]
pub struct ResourceLimits {
    /// CPU限制
    pub cpu: Option<CpuLimit>,
    /// 内存限制
    pub memory: Option<MemoryLimit>,
    /// 块设备IO限制
    pub blkio: Option<BlkioLimit>,
    /// 网络IO限制
    pub network: Option<NetworkLimit>,
    /// PID限制
    pub pids: Option<PidsLimit>,
}

/// CPU限制
#[derive(Debug, Clone)]
pub struct CpuLimit {
    /// CPU份额（相对权重）
    pub shares: Option<u64>,
    /// CPU配额（微秒）
    pub quota: Option<i64>,
    /// CPU周期（微秒）
    pub period: Option<u64>,
    /// 实时运行时间（微秒）
    pub realtime_runtime: Option<i64>,
    /// 实时周期（微秒）
    pub realtime_period: Option<u64>,
    /// CPU集（如"0-3,5"）
    pub cpus: Option<String>,
    /// MEM集
    pub mems: Option<String>,
}

/// 内存限制
#[derive(Debug, Clone)]
pub struct MemoryLimit {
    /// 内存限制（字节）
    pub limit: Option<i64>,
    /// 内存预留（软限制）
    pub reservation: Option<i64>,
    /// 内存+交换限制
    pub swap: Option<i64>,
    /// 内核内存限制
    pub kernel: Option<i64>,
    /// 内核TCP内存限制
    pub kernel_tcp: Option<i64>,
    /// 内存页回收阈值
    pub swappiness: Option<u64>,
    /// 禁用OOM killer
    pub disable_oom_killer: Option<bool>,
    /// 使用层级内存
    pub use_hierarchy: Option<bool>,
}

/// 块设备IO限制
#[derive(Debug, Clone)]
pub struct BlkioLimit {
    /// 权重
    pub weight: Option<u16>,
    /// 设备权重
    pub leaf_weight: Option<u16>,
    /// 设备特定限制
    pub device_weights: Vec<DeviceWeight>,
    /// 设备读取bps限制
    pub device_read_bps: Vec<DeviceThrottle>,
    /// 设备写入bps限制
    pub device_write_bps: Vec<DeviceThrottle>,
    /// 设备读取iops限制
    pub device_read_iops: Vec<DeviceThrottle>,
    /// 设备写入iops限制
    pub device_write_iops: Vec<DeviceThrottle>,
}

/// 设备权重
#[derive(Debug, Clone)]
pub struct DeviceWeight {
    pub major: i64,
    pub minor: i64,
    pub weight: Option<u16>,
    pub leaf_weight: Option<u16>,
}

/// 设备限速
#[derive(Debug, Clone)]
pub struct DeviceThrottle {
    pub major: i64,
    pub minor: i64,
    pub rate: u64,
}

/// 网络限制
#[derive(Debug, Clone)]
pub struct NetworkLimit {
    /// 网络类ID
    pub class_id: Option<u32>,
    /// 优先级
    pub priority: Option<u32>,
}

/// PID限制
#[derive(Debug, Clone)]
pub struct PidsLimit {
    /// 最大PID数量
    pub max: Option<i64>,
}

/// Cgroups管理器
pub struct CgroupManager {
    /// cgroups挂载点
    mount_point: PathBuf,
    /// cgroups版本
    version: CgroupVersion,
    /// 容器ID
    container_id: String,
}

/// Cgroups版本
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CgroupVersion {
    V1,
    V2,
}

impl CgroupManager {
    /// 创建新的cgroups管理器
    pub fn new(container_id: String) -> Result<Self> {
        let (mount_point, version) = Self::detect_cgroup_version()?;

        info!("Detected cgroups {:?} at {:?}", version, mount_point);

        Ok(Self {
            mount_point,
            version,
            container_id,
        })
    }

    /// 检测cgroups版本
    fn detect_cgroup_version() -> Result<(PathBuf, CgroupVersion)> {
        // 检查cgroup v2
        let v2_mount = PathBuf::from("/sys/fs/cgroup");
        if v2_mount.join("cgroup.controllers").exists() {
            return Ok((v2_mount, CgroupVersion::V2));
        }

        // 检查cgroup v1
        let v1_mount = PathBuf::from("/sys/fs/cgroup");
        if v1_mount.join("cpu").exists() {
            return Ok((v1_mount, CgroupVersion::V1));
        }

        Err(anyhow::anyhow!("No cgroups mount found"))
    }

    /// 创建容器cgroups
    pub fn create_cgroup(&self) -> Result<()> {
        match self.version {
            CgroupVersion::V1 => self.create_cgroup_v1(),
            CgroupVersion::V2 => self.create_cgroup_v2(),
        }
    }

    /// 删除容器cgroups
    pub fn remove_cgroup(&self) -> Result<()> {
        match self.version {
            CgroupVersion::V1 => self.remove_cgroup_v1(),
            CgroupVersion::V2 => self.remove_cgroup_v2(),
        }
    }

    /// 设置资源限制
    pub fn set_resources(&self, limits: &ResourceLimits) -> Result<()> {
        match self.version {
            CgroupVersion::V1 => self.set_resources_v1(limits),
            CgroupVersion::V2 => self.set_resources_v2(limits),
        }
    }

    /// 创建cgroup v1
    fn create_cgroup_v1(&self) -> Result<()> {
        let subsystems = [
            "cpu", "cpuacct", "cpuset", "memory", "blkio", "pids", "devices",
        ];

        for subsystem in &subsystems {
            let path = self
                .mount_point
                .join(subsystem)
                .join("crius")
                .join(&self.container_id);
            std::fs::create_dir_all(&path)
                .with_context(|| format!("Failed to create cgroup v1 directory: {:?}", path))?;
            debug!("Created cgroup v1 directory: {:?}", path);
        }

        Ok(())
    }

    /// 创建cgroup v2
    fn create_cgroup_v2(&self) -> Result<()> {
        let path = self.mount_point.join("crius").join(&self.container_id);
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Failed to create cgroup v2 directory: {:?}", path))?;

        // 启用所有控制器
        let cgroup_subtree = self.mount_point.join("cgroup.subtree_control");
        if cgroup_subtree.exists() {
            let controllers = ["cpu", "memory", "io", "pids"];
            for controller in &controllers {
                let _ = std::fs::write(&cgroup_subtree, format!("+{}", controller));
            }
        }

        debug!("Created cgroup v2 directory: {:?}", path);
        Ok(())
    }

    /// 删除cgroup v1
    fn remove_cgroup_v1(&self) -> Result<()> {
        let subsystems = [
            "cpu", "cpuacct", "cpuset", "memory", "blkio", "pids", "devices",
        ];

        for subsystem in &subsystems {
            let path = self
                .mount_point
                .join(subsystem)
                .join("crius")
                .join(&self.container_id);
            if path.exists() {
                let _ = std::fs::remove_dir(&path);
            }
        }

        info!("Removed cgroup v1 for container {}", self.container_id);
        Ok(())
    }

    /// 删除cgroup v2
    fn remove_cgroup_v2(&self) -> Result<()> {
        let path = self.mount_point.join("crius").join(&self.container_id);
        if path.exists() {
            // 先杀死所有进程
            let procs_file = path.join("cgroup.procs");
            if procs_file.exists() {
                if let Ok(content) = std::fs::read_to_string(&procs_file) {
                    for pid in content.lines() {
                        if let Ok(pid) = pid.parse::<i32>() {
                            let _ = nix::sys::signal::kill(
                                nix::unistd::Pid::from_raw(pid),
                                nix::sys::signal::Signal::SIGKILL,
                            );
                        }
                    }
                }
            }

            std::fs::remove_dir(&path)
                .with_context(|| format!("Failed to remove cgroup v2 directory: {:?}", path))?;
        }

        info!("Removed cgroup v2 for container {}", self.container_id);
        Ok(())
    }

    /// 设置资源限制v1
    fn set_resources_v1(&self, limits: &ResourceLimits) -> Result<()> {
        // 设置CPU限制
        if let Some(cpu) = &limits.cpu {
            let cpu_path = self
                .mount_point
                .join("cpu")
                .join("crius")
                .join(&self.container_id);

            if let Some(shares) = cpu.shares {
                self.write_file(&cpu_path.join("cpu.shares"), shares.to_string())?;
            }
            if let Some(quota) = cpu.quota {
                self.write_file(&cpu_path.join("cpu.cfs_quota_us"), quota.to_string())?;
            }
            if let Some(period) = cpu.period {
                self.write_file(&cpu_path.join("cpu.cfs_period_us"), period.to_string())?;
            }
            if let Some(cpus) = &cpu.cpus {
                self.write_file(&cpu_path.join("cpuset.cpus"), cpus.clone())?;
            }
        }

        // 设置内存限制
        if let Some(memory) = &limits.memory {
            let mem_path = self
                .mount_point
                .join("memory")
                .join("crius")
                .join(&self.container_id);

            if let Some(limit) = memory.limit {
                self.write_file(&mem_path.join("memory.limit_in_bytes"), limit.to_string())?;
            }
            if let Some(swap) = memory.swap {
                self.write_file(
                    &mem_path.join("memory.memsw.limit_in_bytes"),
                    swap.to_string(),
                )?;
            }
            if let Some(reservation) = memory.reservation {
                self.write_file(
                    &mem_path.join("memory.soft_limit_in_bytes"),
                    reservation.to_string(),
                )?;
            }
            if let Some(swappiness) = memory.swappiness {
                self.write_file(&mem_path.join("memory.swappiness"), swappiness.to_string())?;
            }
            if let Some(true) = memory.disable_oom_killer {
                self.write_file(&mem_path.join("memory.oom_control"), "1")?;
            }
        }

        // 设置PID限制
        if let Some(pids) = &limits.pids {
            let pids_path = self
                .mount_point
                .join("pids")
                .join("crius")
                .join(&self.container_id);

            if let Some(max) = pids.max {
                self.write_file(&pids_path.join("pids.max"), max.to_string())?;
            }
        }

        info!("Set resource limits for container {}", self.container_id);
        Ok(())
    }

    /// 设置资源限制v2
    fn set_resources_v2(&self, limits: &ResourceLimits) -> Result<()> {
        let cgroup_path = self.mount_point.join("crius").join(&self.container_id);

        // 设置CPU限制
        if let Some(cpu) = &limits.cpu {
            let mut cpu_max = String::new();

            if let Some(quota) = cpu.quota {
                cpu_max.push_str(&quota.to_string());
            } else {
                cpu_max.push_str("max");
            }

            cpu_max.push(' ');

            if let Some(period) = cpu.period {
                cpu_max.push_str(&period.to_string());
            } else {
                cpu_max.push_str("100000");
            }

            self.write_file(&cgroup_path.join("cpu.max"), cpu_max)?;

            if let Some(shares) = cpu.shares {
                // v2使用cpu.weight，范围1-10000
                let weight = ((shares as f64 / 1024.0) * 100.0) as u64;
                self.write_file(&cgroup_path.join("cpu.weight"), weight.to_string())?;
            }

            if let Some(cpus) = &cpu.cpus {
                self.write_file(&cgroup_path.join("cpuset.cpus"), cpus.clone())?;
            }
        }

        // 设置内存限制
        if let Some(memory) = &limits.memory {
            if let Some(limit) = memory.limit {
                self.write_file(&cgroup_path.join("memory.max"), limit.to_string())?;
            }
            if let Some(swap) = memory.swap {
                self.write_file(&cgroup_path.join("memory.swap.max"), swap.to_string())?;
            }
            if let Some(reservation) = memory.reservation {
                self.write_file(&cgroup_path.join("memory.high"), reservation.to_string())?;
            }
        }

        // 设置PID限制
        if let Some(pids) = &limits.pids {
            if let Some(max) = pids.max {
                self.write_file(&cgroup_path.join("pids.max"), max.to_string())?;
            }
        }

        info!(
            "Set resource limits (v2) for container {}",
            self.container_id
        );
        Ok(())
    }

    /// 将进程添加到cgroup
    pub fn add_process(&self, pid: i32) -> Result<()> {
        match self.version {
            CgroupVersion::V1 => {
                let subsystems = ["cpu", "memory", "pids"];
                for subsystem in &subsystems {
                    let procs_file = self
                        .mount_point
                        .join(subsystem)
                        .join("crius")
                        .join(&self.container_id)
                        .join("cgroup.procs");

                    self.write_file(&procs_file, pid.to_string())?;
                }
            }
            CgroupVersion::V2 => {
                let procs_file = self
                    .mount_point
                    .join("crius")
                    .join(&self.container_id)
                    .join("cgroup.procs");

                self.write_file(&procs_file, pid.to_string())?;
            }
        }

        debug!(
            "Added process {} to cgroup for container {}",
            pid, self.container_id
        );
        Ok(())
    }

    /// 写入cgroup文件
    fn write_file(&self, path: &PathBuf, content: impl AsRef<[u8]>) -> Result<()> {
        std::fs::write(path, content)
            .with_context(|| format!("Failed to write to cgroup file: {:?}", path))?;
        Ok(())
    }

    /// 获取cgroups版本
    pub fn version(&self) -> CgroupVersion {
        self.version
    }
}

/// 转换为OCI Linux资源
pub fn to_oci_resources(limits: &ResourceLimits) -> crate::oci::spec::LinuxResources {
    crate::oci::spec::LinuxResources {
        network: None,
        unified: None,
        cpu: limits.cpu.as_ref().map(|cpu| crate::oci::spec::LinuxCpu {
            shares: cpu.shares,
            quota: cpu.quota,
            period: cpu.period,
            realtime_runtime: cpu.realtime_runtime,
            realtime_period: cpu.realtime_period,
            cpus: cpu.cpus.clone(),
            mems: cpu.mems.clone(),
        }),
        memory: limits
            .memory
            .as_ref()
            .map(|memory| crate::oci::spec::LinuxMemory {
                limit: memory.limit,
                reservation: memory.reservation,
                swap: memory.swap,
                kernel: memory.kernel,
                kernel_tcp: memory.kernel_tcp,
                swappiness: memory.swappiness,
                disable_oom_killer: memory.disable_oom_killer,
                use_hierarchy: memory.use_hierarchy,
            }),
        pids: limits
            .pids
            .as_ref()
            .map(|pids| crate::oci::spec::LinuxPids {
                limit: pids.max.unwrap_or(-1),
            }),
        block_io: limits
            .blkio
            .as_ref()
            .map(|blkio| crate::oci::spec::LinuxBlockIo {
                weight: blkio.weight,
                leaf_weight: blkio.leaf_weight,
                weight_device: if blkio.device_weights.is_empty() {
                    None
                } else {
                    Some(
                        blkio
                            .device_weights
                            .iter()
                            .map(|d| crate::oci::spec::LinuxWeightDevice {
                                major: d.major,
                                minor: d.minor,
                                weight: d.weight,
                                leaf_weight: d.leaf_weight,
                            })
                            .collect(),
                    )
                },
                throttle_read_bps_device: if blkio.device_read_bps.is_empty() {
                    None
                } else {
                    Some(
                        blkio
                            .device_read_bps
                            .iter()
                            .map(|d| crate::oci::spec::LinuxThrottleDevice {
                                major: d.major,
                                minor: d.minor,
                                rate: d.rate,
                            })
                            .collect(),
                    )
                },
                throttle_write_bps_device: if blkio.device_write_bps.is_empty() {
                    None
                } else {
                    Some(
                        blkio
                            .device_write_bps
                            .iter()
                            .map(|d| crate::oci::spec::LinuxThrottleDevice {
                                major: d.major,
                                minor: d.minor,
                                rate: d.rate,
                            })
                            .collect(),
                    )
                },
                throttle_read_iops_device: if blkio.device_read_iops.is_empty() {
                    None
                } else {
                    Some(
                        blkio
                            .device_read_iops
                            .iter()
                            .map(|d| crate::oci::spec::LinuxThrottleDevice {
                                major: d.major,
                                minor: d.minor,
                                rate: d.rate,
                            })
                            .collect(),
                    )
                },
                throttle_write_iops_device: if blkio.device_write_iops.is_empty() {
                    None
                } else {
                    Some(
                        blkio
                            .device_write_iops
                            .iter()
                            .map(|d| crate::oci::spec::LinuxThrottleDevice {
                                major: d.major,
                                minor: d.minor,
                                rate: d.rate,
                            })
                            .collect(),
                    )
                },
            }),
        hugepage_limits: None,
        devices: None,
        intel_rdt: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_limits_default() {
        let limits = ResourceLimits::default();
        assert!(limits.cpu.is_none());
        assert!(limits.memory.is_none());
    }

    #[test]
    fn test_detect_cgroup_version() {
        // 这个测试需要root权限
        if nix::unistd::getuid().is_root() {
            let result = CgroupManager::detect_cgroup_version();
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_to_oci_resources() {
        let limits = ResourceLimits {
            cpu: Some(CpuLimit {
                shares: Some(1024),
                quota: Some(100000),
                period: Some(100000),
                realtime_runtime: None,
                realtime_period: None,
                cpus: Some("0-3".to_string()),
                mems: None,
            }),
            memory: Some(MemoryLimit {
                limit: Some(1073741824), // 1GB
                reservation: None,
                swap: None,
                kernel: None,
                kernel_tcp: None,
                swappiness: None,
                disable_oom_killer: None,
                use_hierarchy: None,
            }),
            pids: Some(PidsLimit { max: Some(100) }),
            blkio: None,
            network: None,
        };

        let resources = to_oci_resources(&limits);
        assert!(resources.cpu.is_some());
        assert!(resources.memory.is_some());
        assert!(resources.pids.is_some());
    }
}
