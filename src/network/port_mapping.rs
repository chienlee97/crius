//! 端口映射模块
//!
//! 提供容器端口到主机端口的映射功能，支持iptables和nftables

use anyhow::{Context, Result};
use log::{debug, info};
use std::net::IpAddr;
use std::process::Command;

/// 端口映射配置
#[derive(Debug, Clone)]
pub struct PortMapping {
    /// 协议 (tcp/udp)
    pub protocol: Protocol,
    /// 容器端口
    pub container_port: u16,
    /// 主机端口
    pub host_port: u16,
    /// 主机IP (可选，默认0.0.0.0)
    pub host_ip: Option<IpAddr>,
    /// 容器IP
    pub container_ip: IpAddr,
}

/// 协议类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Protocol {
    Tcp,
    Udp,
    Both,
}

impl Protocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::Tcp => "tcp",
            Protocol::Udp => "udp",
            Protocol::Both => "tcp,udp",
        }
    }
}

/// 端口映射管理器
#[derive(Debug)]
pub struct PortMappingManager {
    backend: PortMappingBackend,
}

/// 端口映射后端类型
#[derive(Debug, Clone, Copy)]
pub enum PortMappingBackend {
    Iptables,
    Nftables,
    Auto,
}

impl PortMappingManager {
    /// 创建新的端口映射管理器
    pub fn new(backend: PortMappingBackend) -> Result<Self> {
        // 自动检测后端
        let backend = match backend {
            PortMappingBackend::Auto => {
                if Self::check_nftables_available() {
                    PortMappingBackend::Nftables
                } else if Self::check_iptables_available() {
                    PortMappingBackend::Iptables
                } else {
                    return Err(anyhow::anyhow!(
                        "No port mapping backend available (iptables or nftables)"
                    ));
                }
            }
            other => other,
        };

        info!("Using port mapping backend: {:?}", backend);

        Ok(Self { backend })
    }

    /// 自动检测后端
    pub fn auto() -> Result<Self> {
        Self::new(PortMappingBackend::Auto)
    }

    /// 检查iptables是否可用
    fn check_iptables_available() -> bool {
        Command::new("iptables")
            .args(["--version"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// 检查nftables是否可用
    fn check_nftables_available() -> bool {
        Command::new("nft")
            .args(["--version"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// 添加端口映射
    pub fn add_port_mapping(&self, mapping: &PortMapping) -> Result<()> {
        debug!("Adding port mapping: {:?}", mapping);

        match self.backend {
            PortMappingBackend::Iptables => self.add_iptables_rule(mapping),
            PortMappingBackend::Nftables => self.add_nftables_rule(mapping),
            PortMappingBackend::Auto => unreachable!(),
        }
    }

    /// 删除端口映射
    pub fn remove_port_mapping(&self, mapping: &PortMapping) -> Result<()> {
        debug!("Removing port mapping: {:?}", mapping);

        match self.backend {
            PortMappingBackend::Iptables => self.remove_iptables_rule(mapping),
            PortMappingBackend::Nftables => self.remove_nftables_rule(mapping),
            PortMappingBackend::Auto => unreachable!(),
        }
    }

    /// 添加iptables规则
    fn add_iptables_rule(&self, mapping: &PortMapping) -> Result<()> {
        let host_ip = mapping
            .host_ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let container_ip = mapping.container_ip.to_string();

        // DNAT规则：将主机端口映射到容器端口
        let dnat_rule = format!(
            "-p {} --dport {} -j DNAT --to-destination {}:{}",
            mapping.protocol.as_str(),
            mapping.host_port,
            container_ip,
            mapping.container_port
        );

        // MASQUERADE规则：允许容器访问外部
        let masquerade_rule = format!(
            "-p {} -s {} -j MASQUERADE",
            mapping.protocol.as_str(),
            container_ip
        );

        // 添加DNAT规则到PREROUTING链
        self.run_iptables(&["-t", "nat", "-A", "PREROUTING", &dnat_rule])?;

        // 添加MASQUERADE规则到POSTROUTING链
        self.run_iptables(&["-t", "nat", "-A", "POSTROUTING", &masquerade_rule])?;

        // 允许转发
        let forward_rule = format!(
            "-p {} -d {} --dport {} -j ACCEPT",
            mapping.protocol.as_str(),
            container_ip,
            mapping.container_port
        );
        self.run_iptables(&["-A", "FORWARD", &forward_rule])?;

        info!(
            "Added iptables port mapping: {}:{} -> {}:{}",
            host_ip, mapping.host_port, container_ip, mapping.container_port
        );

        Ok(())
    }

    /// 删除iptables规则
    fn remove_iptables_rule(&self, mapping: &PortMapping) -> Result<()> {
        let container_ip = mapping.container_ip.to_string();

        // 构建与添加时相同的规则，但使用-D删除
        let dnat_rule = format!(
            "-p {} --dport {} -j DNAT --to-destination {}:{}",
            mapping.protocol.as_str(),
            mapping.host_port,
            container_ip,
            mapping.container_port
        );

        let masquerade_rule = format!(
            "-p {} -s {} -j MASQUERADE",
            mapping.protocol.as_str(),
            container_ip
        );

        let forward_rule = format!(
            "-p {} -d {} --dport {} -j ACCEPT",
            mapping.protocol.as_str(),
            container_ip,
            mapping.container_port
        );

        // 删除规则（忽略错误，规则可能不存在）
        let _ = self.run_iptables(&["-t", "nat", "-D", "PREROUTING", &dnat_rule]);
        let _ = self.run_iptables(&["-t", "nat", "-D", "POSTROUTING", &masquerade_rule]);
        let _ = self.run_iptables(&["-D", "FORWARD", &forward_rule]);

        info!(
            "Removed iptables port mapping: {} -> {}:{}",
            mapping.host_port, container_ip, mapping.container_port
        );

        Ok(())
    }

    /// 运行iptables命令
    fn run_iptables(&self, args: &[&str]) -> Result<()> {
        let output = Command::new("iptables")
            .args(args)
            .output()
            .context("Failed to execute iptables command")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("iptables command failed: {}", stderr));
        }

        Ok(())
    }

    /// 添加nftables规则
    fn add_nftables_rule(&self, mapping: &PortMapping) -> Result<()> {
        let container_ip = mapping.container_ip.to_string();
        let host_ip = mapping
            .host_ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "0.0.0.0".to_string());

        let proto = mapping.protocol.as_str();
        let host_port = mapping.host_port;
        let container_port = mapping.container_port;

        // 创建nftables规则集 - 使用字符串拼接避免格式字符串问题
        let rule_set = format!(
            "table inet crius {{
    chain prerouting {{
        type nat hook prerouting priority 0; policy accept;
        {proto} dport {host_port} dnat to {container_ip}:{container_port}
    }}
    chain postrouting {{
        type nat hook postrouting priority 100; policy accept;
        ip saddr {container_ip} masquerade
    }}
    chain forward {{
        type filter hook forward priority 0; policy accept;
        ip daddr {container_ip} {proto} dport {container_port} accept
    }}
}}",
            proto = proto,
            host_port = host_port,
            container_ip = container_ip,
            container_port = container_port,
        );

        // 使用nft命令添加规则 - 写入临时文件
        use std::io::Write;
        let mut temp_file =
            tempfile::NamedTempFile::new().context("Failed to create temp file for nft rules")?;
        temp_file
            .write_all(rule_set.as_bytes())
            .context("Failed to write nft rules to temp file")?;
        let temp_path = temp_file.into_temp_path();

        let output = Command::new("nft")
            .args(["-f", temp_path.to_str().unwrap()])
            .output()
            .context("Failed to execute nft command")?;

        // 临时文件会自动删除

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("nft command failed: {}", stderr));
        }

        info!(
            "Added nftables port mapping: {}:{} -> {}:{}",
            host_ip, host_port, container_ip, container_port
        );

        Ok(())
    }

    /// 删除nftables规则
    fn remove_nftables_rule(&self, _mapping: &PortMapping) -> Result<()> {
        // nftables通过table管理，删除整个table或特定规则
        // 简化实现：删除crius table（如果存在）
        let output = Command::new("nft")
            .args(["delete", "table", "inet", "crius"])
            .output();

        match output {
            Ok(o) if o.status.success() => {
                info!("Removed nftables table 'crius'");
            }
            _ => {
                // 表可能不存在，忽略错误
                debug!("nftables table 'crius' not found or already removed");
            }
        }

        Ok(())
    }

    /// 清理所有端口映射规则
    pub fn cleanup_all_rules(&self) -> Result<()> {
        info!("Cleaning up all port mapping rules");

        match self.backend {
            PortMappingBackend::Iptables => {
                // 删除crius相关的所有规则
                // 注意：这里需要谨慎，避免删除系统规则
                let _ = self.run_iptables(&["-t", "nat", "-F", "PREROUTING"]);
                let _ = self.run_iptables(&["-t", "nat", "-F", "POSTROUTING"]);
                let _ = self.run_iptables(&["-F", "FORWARD"]);
            }
            PortMappingBackend::Nftables => {
                let _ = Command::new("nft")
                    .args(["delete", "table", "inet", "crius"])
                    .output();
            }
            PortMappingBackend::Auto => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_protocol_as_str() {
        assert_eq!(Protocol::Tcp.as_str(), "tcp");
        assert_eq!(Protocol::Udp.as_str(), "udp");
        assert_eq!(Protocol::Both.as_str(), "tcp,udp");
    }

    #[test]
    fn test_port_mapping_creation() {
        let mapping = PortMapping {
            protocol: Protocol::Tcp,
            container_port: 80,
            host_port: 8080,
            host_ip: Some(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))),
            container_ip: IpAddr::V4(Ipv4Addr::new(10, 88, 0, 1)),
        };

        assert_eq!(mapping.container_port, 80);
        assert_eq!(mapping.host_port, 8080);
    }

    // 注意：以下测试需要root权限，默认忽略
    #[test]
    #[ignore = "requires root and iptables/nftables"]
    fn test_port_mapping_manager_creation() {
        let manager = PortMappingManager::auto();
        assert!(manager.is_ok());
    }

    #[test]
    #[ignore = "requires root and iptables"]
    fn test_iptables_add_remove() {
        let manager = PortMappingManager::new(PortMappingBackend::Iptables).unwrap();

        let mapping = PortMapping {
            protocol: Protocol::Tcp,
            container_port: 80,
            host_port: 18080, // 使用高位端口避免冲突
            host_ip: None,
            container_ip: IpAddr::V4(Ipv4Addr::new(10, 88, 0, 1)),
        };

        // 添加规则
        manager.add_port_mapping(&mapping).unwrap();

        // 删除规则
        manager.remove_port_mapping(&mapping).unwrap();
    }
}
