//! 性能监控指标模块
//!
//! 提供容器性能指标采集：
//! - CPU使用率统计
//! - 内存使用量统计
//! - 块IO统计
//! - 网络IO统计
//! - 进程数统计

use anyhow::{Context, Result};
use log::debug;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// 容器性能统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerStats {
    /// 容器ID
    pub container_id: String,
    /// CPU统计
    pub cpu: Option<CpuStats>,
    /// 内存统计
    pub memory: Option<MemoryStats>,
    /// 块IO统计
    pub blkio: Option<BlkioStats>,
    /// 网络统计
    pub network: Option<NetworkStats>,
    /// 进程统计
    pub pids: Option<PidsStats>,
    /// 采样时间
    pub timestamp: u64,
}

/// CPU统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CpuStats {
    /// CPU使用时间（纳秒）
    pub usage_total: u64,
    /// 用户态CPU时间（纳秒）
    pub usage_user: u64,
    /// 内核态CPU时间（纳秒）
    pub usage_kernel: u64,
    /// CPU限额（微秒/周期）
    pub throttle_periods: u64,
    /// CPU节流次数
    pub throttled_count: u64,
    /// CPU节流时间（纳秒）
    pub throttled_time: u64,
    /// 系统总CPU时间（用于计算使用率）
    pub system_usage: u64,
}

/// 内存统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryStats {
    /// 当前内存使用量（字节）
    pub usage: u64,
    /// 最大内存使用量（字节）
    pub max_usage: u64,
    /// 限制（字节）
    pub limit: u64,
    /// 缓存（字节）
    pub cache: u64,
    /// RSS（字节）
    pub rss: u64,
    /// Swap使用量（字节）
    pub swap: u64,
    /// Page faults
    pub pgfault: u64,
    /// Major page faults
    pub pgmajfault: u64,
    /// 内核内存（字节）
    pub kernel_usage: u64,
    /// TCP缓冲区内存（字节）
    pub kernel_tcp_usage: u64,
}

/// 块IO统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlkioStats {
    /// 设备IO统计
    pub io_service_bytes: Vec<BlkioDeviceStat>,
    /// 设备IO操作数
    pub io_serviced: Vec<BlkioDeviceStat>,
}

/// 块IO设备统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlkioDeviceStat {
    /// 主设备号
    pub major: u64,
    /// 次设备号
    pub minor: u64,
    /// 操作类型（read/write/sync/async）
    pub op: String,
    /// 数值
    pub value: u64,
}

/// 网络统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkStats {
    /// 接口名称
    pub name: String,
    /// 接收字节数
    pub rx_bytes: u64,
    /// 接收包数
    pub rx_packets: u64,
    /// 接收错误数
    pub rx_errors: u64,
    /// 接收丢包数
    pub rx_dropped: u64,
    /// 发送字节数
    pub tx_bytes: u64,
    /// 发送包数
    pub tx_packets: u64,
    /// 发送错误数
    pub tx_errors: u64,
    /// 发送丢包数
    pub tx_dropped: u64,
}

/// PIDs统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PidsStats {
    /// 当前进程数
    pub current: u64,
    /// 限制
    pub limit: u64,
}

/// 指标采集器
pub struct MetricsCollector {
    /// cgroup v2是否可用
    cgroup_v2: bool,
    /// cgroup基础路径
    cgroup_base: PathBuf,
}

impl MetricsCollector {
    /// 创建新的指标采集器
    pub fn new() -> Result<Self> {
        // 检测cgroup版本
        let cgroup_v2 = Self::is_cgroup_v2();
        let cgroup_base = PathBuf::from("/sys/fs/cgroup");

        debug!("MetricsCollector initialized: cgroup_v2={}", cgroup_v2);

        Ok(Self {
            cgroup_v2,
            cgroup_base,
        })
    }

    /// 检测是否为cgroup v2
    fn is_cgroup_v2() -> bool {
        // 检查cgroup v2的挂载
        if let Ok(content) = fs::read_to_string("/proc/mounts") {
            content
                .lines()
                .any(|line| line.contains("cgroup2") || line.contains("cgroups2"))
        } else {
            false
        }
    }

    /// 采集容器指标
    pub fn collect_container_stats(
        &self,
        container_id: &str,
        cgroup_path: &Path,
    ) -> Result<ContainerStats> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mut stats = ContainerStats {
            container_id: container_id.to_string(),
            timestamp,
            ..Default::default()
        };

        // 尝试找到容器的cgroup路径
        // 采集CPU统计 - 优先使用调用方提供的 cgroup 路径提示
        let cpu_path = self.find_cgroup_path(cgroup_path, container_id, "cpu,cpuacct");
        if let Some(ref path) = cpu_path {
            if let Ok(cpu) = self.collect_cpu_stats(path) {
                stats.cpu = Some(cpu);
            }
        }

        // 采集内存统计
        let memory_path = self.find_cgroup_path(cgroup_path, container_id, "memory");
        if let Some(ref path) = memory_path {
            if let Ok(memory) = self.collect_memory_stats(path) {
                stats.memory = Some(memory);
            }
        }

        // 采集块IO统计
        let blkio_path = self.find_cgroup_path(cgroup_path, container_id, "blkio");
        if let Some(ref path) = blkio_path {
            if let Ok(blkio) = self.collect_blkio_stats(path) {
                stats.blkio = Some(blkio);
            }
        }

        // 采集PIDs统计
        let pids_path = self.find_cgroup_path(cgroup_path, container_id, "pids");
        if let Some(ref path) = pids_path {
            if let Ok(pids) = self.collect_pids_stats(path) {
                stats.pids = Some(pids);
            }
        }

        debug!(
            "Collected stats for container {}: cpu={:?}, memory={:?}",
            container_id,
            stats.cpu.is_some(),
            stats.memory.is_some()
        );

        Ok(stats)
    }

    /// 查找容器的cgroup路径
    fn find_cgroup_path(
        &self,
        hint: &Path,
        container_id: &str,
        subsystem: &str,
    ) -> Option<PathBuf> {
        let mut search_roots = Vec::new();

        if !hint.as_os_str().is_empty() && hint != Path::new("/sys/fs/cgroup") {
            if hint.exists() {
                search_roots.push(hint.to_path_buf());
            }

            if self.cgroup_v2 {
                if hint.is_absolute() {
                    if let Ok(stripped) = hint.strip_prefix(&self.cgroup_base) {
                        search_roots.push(self.cgroup_base.join(stripped));
                    }
                } else {
                    search_roots.push(self.cgroup_base.join(hint));
                }
            } else {
                let subsystem_root = self.cgroup_base.join(subsystem);
                if hint.is_absolute() {
                    if let Ok(stripped) = hint.strip_prefix(&self.cgroup_base) {
                        search_roots.push(subsystem_root.join(stripped));
                    }
                } else {
                    search_roots.push(subsystem_root.join(hint));
                }
            }
        }

        if self.cgroup_v2 {
            search_roots.push(self.cgroup_base.clone());
        } else {
            search_roots.push(self.cgroup_base.join(subsystem));
        }

        for root in search_roots {
            if !root.exists() {
                continue;
            }

            if root.file_name().and_then(|name| name.to_str()) == Some(container_id) {
                return Some(root);
            }

            let direct_path = root.join(container_id);
            if direct_path.exists() {
                return Some(direct_path);
            }

            if let Some(found) = Self::find_in_directory(&root, container_id) {
                return Some(found);
            }
        }

        None
    }

    /// 递归在目录中查找容器cgroup
    fn find_in_directory(dir: &Path, container_id: &str) -> Option<PathBuf> {
        // 检查当前目录是否匹配
        let container_path = dir.join(container_id);
        if container_path.exists() {
            return Some(container_path);
        }

        // 递归查找子目录
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    // 检查当前子目录是否匹配
                    if let Some(file_name) = path.file_name() {
                        if file_name == container_id {
                            return Some(path);
                        }
                    }
                    // 递归查找
                    if let Some(found) = Self::find_in_directory(&path, container_id) {
                        return Some(found);
                    }
                }
            }
        }

        None
    }

    /// 采集CPU统计
    fn collect_cpu_stats(&self, cgroup_path: &Path) -> Result<CpuStats> {
        if self.cgroup_v2 {
            self.collect_cpu_stats_v2(cgroup_path)
        } else {
            self.collect_cpu_stats_v1(cgroup_path)
        }
    }

    /// 采集cgroup v2 CPU统计
    fn collect_cpu_stats_v2(&self, cgroup_path: &Path) -> Result<CpuStats> {
        let cpu_stat_path = cgroup_path.join("cpu.stat");
        let content = fs::read_to_string(&cpu_stat_path).context("Failed to read cpu.stat")?;

        let mut stats = CpuStats::default();

        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 2 {
                continue;
            }

            match parts[0] {
                "usage_usec" => stats.usage_total = parts[1].parse::<u64>()? * 1000,
                "user_usec" => stats.usage_user = parts[1].parse::<u64>()? * 1000,
                "system_usec" => stats.usage_kernel = parts[1].parse::<u64>()? * 1000,
                "nr_periods" => stats.throttle_periods = parts[1].parse()?,
                "nr_throttled" => stats.throttled_count = parts[1].parse()?,
                "throttled_usec" => stats.throttled_time = parts[1].parse::<u64>()? * 1000,
                _ => {}
            }
        }

        // 读取系统总CPU时间
        if let Ok(cpuinfo) = fs::read_to_string("/proc/stat") {
            if let Some(line) = cpuinfo.lines().next() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() > 1 && parts[0] == "cpu" {
                    let total: u64 = parts[1..]
                        .iter()
                        .filter_map(|p| p.parse::<u64>().ok())
                        .sum();
                    stats.system_usage = total * 10_000_000; // 转换为纳秒
                }
            }
        }

        Ok(stats)
    }

    /// 采集cgroup v1 CPU统计
    fn collect_cpu_stats_v1(&self, cgroup_path: &Path) -> Result<CpuStats> {
        // cpuacct.usage
        let usage_path = cgroup_path.join("cpuacct.usage");
        let usage_total = if usage_path.exists() {
            fs::read_to_string(&usage_path)?.trim().parse::<u64>()?
        } else {
            0
        };

        // cpuacct.usage_percpu
        let usage_percpu_path = cgroup_path.join("cpuacct.usage_percpu");
        let (usage_user, usage_kernel) = if usage_percpu_path.exists() {
            let content = fs::read_to_string(&usage_percpu_path)?;
            let values: Vec<u64> = content
                .split_whitespace()
                .filter_map(|s| s.parse().ok())
                .collect();
            let total: u64 = values.iter().sum();
            // cgroup v1不区分用户态和内核态，这里简单处理
            (total / 2, total / 2)
        } else {
            (0, 0)
        };

        // cpu.stat
        let cpu_stat_path = cgroup_path.join("cpu.stat");
        let mut throttle_periods = 0u64;
        let mut throttled_count = 0u64;
        let mut throttled_time = 0u64;

        if cpu_stat_path.exists() {
            let content = fs::read_to_string(&cpu_stat_path)?;
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() != 2 {
                    continue;
                }
                match parts[0] {
                    "nr_periods" => throttle_periods = parts[1].parse()?,
                    "nr_throttled" => throttled_count = parts[1].parse()?,
                    "throttled_time" => throttled_time = parts[1].parse()?,
                    _ => {}
                }
            }
        }

        Ok(CpuStats {
            usage_total,
            usage_user,
            usage_kernel,
            throttle_periods,
            throttled_count,
            throttled_time,
            system_usage: 0,
        })
    }

    /// 采集内存统计
    fn collect_memory_stats(&self, cgroup_path: &Path) -> Result<MemoryStats> {
        if self.cgroup_v2 {
            self.collect_memory_stats_v2(cgroup_path)
        } else {
            self.collect_memory_stats_v1(cgroup_path)
        }
    }

    /// 采集cgroup v2内存统计
    fn collect_memory_stats_v2(&self, cgroup_path: &Path) -> Result<MemoryStats> {
        let memory_current_path = cgroup_path.join("memory.current");
        let usage = fs::read_to_string(&memory_current_path)?
            .trim()
            .parse::<u64>()?;

        let memory_max_path = cgroup_path.join("memory.max");
        let limit_str = fs::read_to_string(&memory_max_path)?;
        let limit = if limit_str.trim() == "max" {
            u64::MAX
        } else {
            limit_str.trim().parse::<u64>()?
        };

        let memory_peak_path = cgroup_path.join("memory.peak");
        let max_usage = if memory_peak_path.exists() {
            fs::read_to_string(&memory_peak_path)?
                .trim()
                .parse::<u64>()?
        } else {
            usage
        };

        // 读取内存统计详情
        let mut cache = 0u64;
        let mut rss = 0u64;
        let mut pgfault = 0u64;
        let mut pgmajfault = 0u64;

        let memory_stat_path = cgroup_path.join("memory.stat");
        if let Ok(content) = fs::read_to_string(&memory_stat_path) {
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() != 2 {
                    continue;
                }
                match parts[0] {
                    "file" => cache = parts[1].parse()?,
                    "anon" => rss = parts[1].parse()?,
                    "pgfault" => pgfault = parts[1].parse()?,
                    "pgmajfault" => pgmajfault = parts[1].parse()?,
                    _ => {}
                }
            }
        }

        // 读取swap使用
        let memory_swap_path = cgroup_path.join("memory.swap.current");
        let swap = if memory_swap_path.exists() {
            fs::read_to_string(&memory_swap_path)?
                .trim()
                .parse::<u64>()?
        } else {
            0
        };

        Ok(MemoryStats {
            usage,
            max_usage,
            limit,
            cache,
            rss,
            swap,
            pgfault,
            pgmajfault,
            kernel_usage: 0,
            kernel_tcp_usage: 0,
        })
    }

    /// 采集cgroup v1内存统计
    fn collect_memory_stats_v1(&self, cgroup_path: &Path) -> Result<MemoryStats> {
        // memory.usage_in_bytes
        let usage_path = cgroup_path.join("memory.usage_in_bytes");
        let usage = fs::read_to_string(&usage_path)?.trim().parse::<u64>()?;

        // memory.limit_in_bytes
        let limit_path = cgroup_path.join("memory.limit_in_bytes");
        let limit_str = fs::read_to_string(&limit_path)?;
        let limit = if limit_str.trim() == "max" || limit_str.trim() == "9223372036854771712" {
            u64::MAX
        } else {
            limit_str.trim().parse::<u64>()?
        };

        // memory.max_usage_in_bytes
        let max_usage_path = cgroup_path.join("memory.max_usage_in_bytes");
        let max_usage = fs::read_to_string(&max_usage_path)?.trim().parse::<u64>()?;

        // memory.stat
        let mut cache = 0u64;
        let mut rss = 0u64;
        let mut swap = 0u64;
        let mut pgfault = 0u64;
        let mut pgmajfault = 0u64;

        let memory_stat_path = cgroup_path.join("memory.stat");
        if let Ok(content) = fs::read_to_string(&memory_stat_path) {
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() != 2 {
                    continue;
                }
                match parts[0] {
                    "cache" => cache = parts[1].parse()?,
                    "rss" => rss = parts[1].parse()?,
                    "swap" | "swapness" => swap = parts[1].parse()?,
                    "pgfault" => pgfault = parts[1].parse()?,
                    "pgmajfault" => pgmajfault = parts[1].parse()?,
                    _ => {}
                }
            }
        }

        Ok(MemoryStats {
            usage,
            max_usage,
            limit,
            cache,
            rss,
            swap,
            pgfault,
            pgmajfault,
            kernel_usage: 0,
            kernel_tcp_usage: 0,
        })
    }

    /// 采集块IO统计
    fn collect_blkio_stats(&self, cgroup_path: &Path) -> Result<BlkioStats> {
        if self.cgroup_v2 {
            self.collect_blkio_stats_v2(cgroup_path)
        } else {
            self.collect_blkio_stats_v1(cgroup_path)
        }
    }

    /// 采集cgroup v2块IO统计
    fn collect_blkio_stats_v2(&self, cgroup_path: &Path) -> Result<BlkioStats> {
        let io_stat_path = cgroup_path.join("io.stat");
        let content = fs::read_to_string(&io_stat_path).context("Failed to read io.stat")?;

        let mut io_service_bytes = vec![];
        let mut io_serviced = vec![];

        for line in content.lines() {
            // 格式: "major:minor rbytes=xxx wbytes=xxx rios=xxx wios=xxx"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            let device_parts: Vec<&str> = parts[0].split(':').collect();
            if device_parts.len() != 2 {
                continue;
            }

            let major = device_parts[0].parse::<u64>()?;
            let minor = device_parts[1].parse::<u64>()?;

            for part in &parts[1..] {
                let kv: Vec<&str> = part.split('=').collect();
                if kv.len() != 2 {
                    continue;
                }

                let value = kv[1].parse::<u64>()?;
                match kv[0] {
                    "rbytes" => io_service_bytes.push(BlkioDeviceStat {
                        major,
                        minor,
                        op: "read".to_string(),
                        value,
                    }),
                    "wbytes" => io_service_bytes.push(BlkioDeviceStat {
                        major,
                        minor,
                        op: "write".to_string(),
                        value,
                    }),
                    "rios" => io_serviced.push(BlkioDeviceStat {
                        major,
                        minor,
                        op: "read".to_string(),
                        value,
                    }),
                    "wios" => io_serviced.push(BlkioDeviceStat {
                        major,
                        minor,
                        op: "write".to_string(),
                        value,
                    }),
                    _ => {}
                }
            }
        }

        Ok(BlkioStats {
            io_service_bytes,
            io_serviced,
        })
    }

    /// 采集cgroup v1块IO统计
    fn collect_blkio_stats_v1(&self, cgroup_path: &Path) -> Result<BlkioStats> {
        let mut io_service_bytes = vec![];
        let mut io_serviced = vec![];

        // blkio.io_service_bytes
        let bytes_path = cgroup_path.join("blkio.io_service_bytes");
        if let Ok(content) = fs::read_to_string(&bytes_path) {
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() != 3 {
                    continue;
                }

                let device_parts: Vec<&str> = parts[0].split(':').collect();
                if device_parts.len() != 2 {
                    continue;
                }

                let major = device_parts[0].parse::<u64>()?;
                let minor = device_parts[1].parse::<u64>()?;
                let op = parts[1].to_string();
                let value = parts[2].parse::<u64>()?;

                io_service_bytes.push(BlkioDeviceStat {
                    major,
                    minor,
                    op,
                    value,
                });
            }
        }

        // blkio.io_serviced
        let serviced_path = cgroup_path.join("blkio.io_serviced");
        if let Ok(content) = fs::read_to_string(&serviced_path) {
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() != 3 {
                    continue;
                }

                let device_parts: Vec<&str> = parts[0].split(':').collect();
                if device_parts.len() != 2 {
                    continue;
                }

                let major = device_parts[0].parse::<u64>()?;
                let minor = device_parts[1].parse::<u64>()?;
                let op = parts[1].to_string();
                let value = parts[2].parse::<u64>()?;

                io_serviced.push(BlkioDeviceStat {
                    major,
                    minor,
                    op,
                    value,
                });
            }
        }

        Ok(BlkioStats {
            io_service_bytes,
            io_serviced,
        })
    }

    /// 采集PIDs统计
    fn collect_pids_stats(&self, cgroup_path: &Path) -> Result<PidsStats> {
        let pids_current_path = cgroup_path.join("pids.current");
        let current = fs::read_to_string(&pids_current_path)?
            .trim()
            .parse::<u64>()?;

        let pids_max_path = cgroup_path.join("pids.max");
        let limit_str = fs::read_to_string(&pids_max_path)?;
        let limit = if limit_str.trim() == "max" {
            u64::MAX
        } else {
            limit_str.trim().parse::<u64>()?
        };

        Ok(PidsStats { current, limit })
    }

    /// 计算CPU使用率
    pub fn calculate_cpu_percent(
        &self,
        current: &CpuStats,
        previous: &CpuStats,
        duration_secs: f64,
    ) -> f64 {
        if duration_secs <= 0.0 {
            return 0.0;
        }

        let usage_delta = current.usage_total.saturating_sub(previous.usage_total) as f64;
        let system_delta = current.system_usage.saturating_sub(previous.system_usage) as f64;

        if system_delta > 0.0 {
            (usage_delta / system_delta) * 100.0
        } else {
            // 如果没有系统CPU数据，使用简单计算
            let nano_per_sec = 1_000_000_000.0;
            (usage_delta / (duration_secs * nano_per_sec)) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new();
        assert!(collector.is_ok());
    }

    #[test]
    fn test_cpu_percent_calculation() {
        let collector = MetricsCollector::new().unwrap();

        let prev = CpuStats {
            usage_total: 1_000_000_000,   // 1 second
            system_usage: 10_000_000_000, // 10 seconds
            ..Default::default()
        };

        let current = CpuStats {
            usage_total: 1_500_000_000,   // 1.5 seconds
            system_usage: 11_000_000_000, // 11 seconds
            ..Default::default()
        };

        let percent = collector.calculate_cpu_percent(&current, &prev, 1.0);
        assert!(percent > 0.0);
    }

    #[test]
    fn test_memory_stats_default() {
        let stats = MemoryStats::default();
        assert_eq!(stats.usage, 0);
        assert_eq!(stats.limit, 0);
    }
}
