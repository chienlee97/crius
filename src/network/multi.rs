//! CNI多网络支持模块
//!
//! 提供多网络接口管理，支持：
//! - 多CNI网络配置
//! - 网络策略和选择
//! - 多网卡(eth0, eth1, net1等)
//! - 网络状态聚合

use super::*;
use std::collections::HashMap;
use std::net::IpAddr;
use anyhow::{Context, Result};
use log::{info, debug, error, warn};
use serde::{Serialize, Deserialize};

/// 多网络配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiNetworkConfig {
    /// 网络名称
    pub name: String,
    /// 接口名称 (eth0, net1等)
    pub interface_name: String,
    /// CNI配置名称
    pub cni_config_name: String,
    /// 是否默认网络
    pub is_default: bool,
    /// 带宽限制 (Mbps)
    pub bandwidth_limit: Option<u64>,
    /// IPAM配置
    pub ipam_config: Option<IpamConfig>,
    /// DNS配置
    pub dns_config: Option<DnsConfig>,
}

/// IPAM配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpamConfig {
    pub subnet: String,
    pub range_start: Option<String>,
    pub range_end: Option<String>,
    pub gateway: Option<String>,
    pub routes: Vec<RouteConfig>,
}

/// 路由配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteConfig {
    pub dst: String,
    pub gw: Option<String>,
}

/// DNS配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsConfig {
    pub nameservers: Vec<String>,
    pub search: Vec<String>,
    pub options: Vec<String>,
}

/// 网络接口状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInterfaceStatus {
    /// 接口名称
    pub name: String,
    /// 网络名称
    pub network_name: String,
    /// IP地址
    pub ip_address: Option<IpAddr>,
    /// MAC地址
    pub mac_address: Option<String>,
    /// 网关
    pub gateway: Option<IpAddr>,
    /// MTU
    pub mtu: Option<u32>,
    /// 接口索引
    pub sandbox_index: u32,
}

/// Pod网络状态（聚合）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodNetworkStatus {
    /// Pod ID
    pub pod_id: String,
    /// 网络命名空间
    pub netns: String,
    /// 接口列表
    pub interfaces: Vec<NetworkInterfaceStatus>,
    /// DNS配置
    pub dns_config: Option<DnsConfig>,
}

/// 多网络管理器
pub struct MultiNetworkManager {
    /// 基础CNI管理器
    cni_manager: CniManager,
    /// 网络配置映射 (network_name -> MultiNetworkConfig)
    network_configs: HashMap<String, MultiNetworkConfig>,
    /// Pod网络状态缓存 (pod_id -> PodNetworkStatus)
    pod_networks: HashMap<String, PodNetworkStatus>,
    /// 网络选择器 (基于annotation选择网络)
    network_selector: NetworkSelector,
}

/// 网络选择器
#[derive(Debug, Clone)]
pub struct NetworkSelector {
    /// 默认网络名称
    default_network: String,
    /// 注解到网络的映射
    annotation_mapping: HashMap<String, String>,
}

impl NetworkSelector {
    pub fn new(default_network: impl Into<String>) -> Self {
        Self {
            default_network: default_network.into(),
            annotation_mapping: HashMap::new(),
        }
    }

    /// 添加注解映射
    pub fn add_mapping(&mut self, annotation: impl Into<String>, network: impl Into<String>) {
        self.annotation_mapping.insert(annotation.into(), network.into());
    }

    /// 根据Pod配置选择网络
    pub fn select_networks(&self, labels: &[(String, String)], annotations: &[(String, String)]) -> Vec<String> {
        let mut selected = Vec::new();

        // 检查注解
        for (key, value) in annotations {
            let annotation_key = format!("{}/{}", key, value);
            if let Some(network) = self.annotation_mapping.get(&annotation_key) {
                selected.push(network.clone());
            }
        }

        // 检查特定注解
        if let Some(network) = annotations.iter()
            .find(|(k, _)| k == "cni.networks")
            .map(|(_, v)| v.split(',').map(|s| s.trim().to_string()).collect::<Vec<_>>()) {
            selected.extend(network);
        }

        // 如果没有选择，使用默认网络
        if selected.is_empty() {
            selected.push(self.default_network.clone());
        }

        selected
    }
}

impl MultiNetworkManager {
    /// 创建新的多网络管理器
    pub fn new(cni_manager: CniManager) -> Self {
        Self {
            cni_manager,
            network_configs: HashMap::new(),
            pod_networks: HashMap::new(),
            network_selector: NetworkSelector::new("default"),
        }
    }

    /// 加载所有网络配置
    pub async fn load_network_configs(&mut self) -> Result<()> {
        // 加载CNI基础配置
        self.cni_manager.load_network_configs().await?;

        // 创建默认多网络配置
        let default_config = MultiNetworkConfig {
            name: "default".to_string(),
            interface_name: "eth0".to_string(),
            cni_config_name: "crius-net".to_string(),
            is_default: true,
            bandwidth_limit: None,
            ipam_config: Some(IpamConfig {
                subnet: "10.88.0.0/16".to_string(),
                range_start: None,
                range_end: None,
                gateway: Some("10.88.0.1".to_string()),
                routes: vec![RouteConfig {
                    dst: "0.0.0.0/0".to_string(),
                    gw: None,
                }],
            }),
            dns_config: Some(DnsConfig {
                nameservers: vec!["8.8.8.8".to_string(), "8.8.4.4".to_string()],
                search: vec!["cluster.local".to_string()],
                options: vec!["ndots:5".to_string()],
            }),
        };

        self.network_configs.insert("default".to_string(), default_config);

        // 尝试加载额外的网络配置
        self.load_additional_networks().await?;

        info!("Loaded {} network configurations", self.network_configs.len());
        Ok(())
    }

    /// 加载额外的网络配置
    async fn load_additional_networks(&mut self) -> Result<()> {
        // 这里可以加载额外的网络配置，例如：
        // - Calico网络
        // - Flannel网络
        // - Multus网络
        // 从配置文件或CRD读取

        // 示例：添加一个额外的网络
        let extra_network = MultiNetworkConfig {
            name: "secondary".to_string(),
            interface_name: "net1".to_string(),
            cni_config_name: "bridge".to_string(),
            is_default: false,
            bandwidth_limit: Some(1000), // 1Gbps
            ipam_config: Some(IpamConfig {
                subnet: "10.99.0.0/16".to_string(),
                range_start: Some("10.99.0.10".to_string()),
                range_end: Some("10.99.255.254".to_string()),
                gateway: Some("10.99.0.1".to_string()),
                routes: vec![],
            }),
            dns_config: None,
        };

        self.network_configs.insert("secondary".to_string(), extra_network);
        Ok(())
    }

    /// 为Pod设置多网络
    pub async fn setup_pod_networks(
        &mut self,
        pod_id: &str,
        netns: &str,
        pod_name: &str,
        pod_namespace: &str,
        labels: &[(String, String)],
        annotations: &[(String, String)],
    ) -> Result<PodNetworkStatus> {
        // 选择要配置的网络
        let network_names = self.network_selector.select_networks(labels, annotations);
        info!("Pod {} selected networks: {:?}", pod_id, network_names);

        let mut interfaces = Vec::new();
        let mut dns_config = None;

        // 为每个网络配置接口
        for (idx, network_name) in network_names.iter().enumerate() {
            let network_config = self.network_configs.get(network_name)
                .context(format!("Network config {} not found", network_name))?;

            let if_name = if idx == 0 {
                "eth0".to_string()
            } else {
                format!("net{}", idx)
            };

            // 设置网络接口
            match self.setup_network_interface(
                pod_id,
                netns,
                pod_name,
                pod_namespace,
                network_config,
                &if_name,
            ).await {
                Ok(status) => {
                    if dns_config.is_none() && network_config.dns_config.is_some() {
                        dns_config = network_config.dns_config.clone();
                    }
                    interfaces.push(status);
                }
                Err(e) => {
                    error!("Failed to setup network {} for pod {}: {}", network_name, pod_id, e);
                    // 继续配置其他网络，不中断
                }
            }
        }

        let pod_status = PodNetworkStatus {
            pod_id: pod_id.to_string(),
            netns: netns.to_string(),
            interfaces,
            dns_config,
        };

        // 缓存网络状态
        self.pod_networks.insert(pod_id.to_string(), pod_status.clone());

        info!("Pod {} network setup completed with {} interfaces", pod_id, pod_status.interfaces.len());
        Ok(pod_status)
    }

    /// 设置单个网络接口
    async fn setup_network_interface(
        &self,
        pod_id: &str,
        netns: &str,
        pod_name: &str,
        pod_namespace: &str,
        network_config: &MultiNetworkConfig,
        if_name: &str,
    ) -> Result<NetworkInterfaceStatus> {
        // 使用基础CNI管理器设置网络
        let status = self.cni_manager.setup_pod_network(
            pod_id,
            netns,
            pod_name,
            pod_namespace,
        ).await?;

        Ok(NetworkInterfaceStatus {
            name: if_name.to_string(),
            network_name: network_config.name.clone(),
            ip_address: status.ip,
            mac_address: status.mac,
            gateway: network_config.ipam_config.as_ref()
                .and_then(|c| c.gateway.as_ref())
                .and_then(|g| g.parse().ok()),
            mtu: Some(1500),
            sandbox_index: 0,
        })
    }

    /// 清理Pod的所有网络
    pub async fn teardown_pod_networks(
        &mut self,
        pod_id: &str,
        netns: &str,
    ) -> Result<()> {
        // 获取Pod的网络状态
        if let Some(pod_status) = self.pod_networks.get(pod_id) {
            info!("Tearing down {} networks for pod {}", pod_status.interfaces.len(), pod_id);

            // 清理每个网络接口
            for interface in &pod_status.interfaces {
                if let Err(e) = self.teardown_network_interface(
                    pod_id,
                    netns,
                    &interface.network_name,
                ).await {
                    error!("Failed to teardown network {} for pod {}: {}", interface.network_name, pod_id, e);
                }
            }

            // 从缓存中移除
            self.pod_networks.remove(pod_id);
        } else {
            // 如果没有缓存，尝试清理默认网络
            self.cni_manager.teardown_pod_network(pod_id, netns).await?;
        }

        info!("Pod {} network teardown completed", pod_id);
        Ok(())
    }

    /// 清理单个网络接口
    async fn teardown_network_interface(
        &self,
        pod_id: &str,
        netns: &str,
        network_name: &str,
    ) -> Result<()> {
        // 调用CNI DEL命令
        self.cni_manager.teardown_pod_network(pod_id, netns).await?;
        Ok(())
    }

    /// 获取Pod网络状态
    pub fn get_pod_network_status(&self, pod_id: &str) -> Option<&PodNetworkStatus> {
        self.pod_networks.get(pod_id)
    }

    /// 列出所有Pod的网络状态
    pub fn list_pod_networks(&self) -> Vec<&PodNetworkStatus> {
        self.pod_networks.values().collect()
    }

    /// 添加网络配置
    pub fn add_network_config(&mut self, config: MultiNetworkConfig) {
        self.network_configs.insert(config.name.clone(), config);
    }

    /// 获取网络配置
    pub fn get_network_config(&self, name: &str) -> Option<&MultiNetworkConfig> {
        self.network_configs.get(name)
    }

    /// 列出所有网络配置
    pub fn list_network_configs(&self) -> Vec<&MultiNetworkConfig> {
        self.network_configs.values().collect()
    }

    /// 获取网络选择器
    pub fn network_selector(&mut self) -> &mut NetworkSelector {
        &mut self.network_selector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_selector() {
        let mut selector = NetworkSelector::new("default");
        selector.add_mapping("network/secondary", "secondary");

        let labels = vec![];
        let annotations = vec![("cni.networks".to_string(), "secondary".to_string())];

        let networks = selector.select_networks(&labels, &annotations);
        assert_eq!(networks, vec!["secondary"]);
    }

    #[test]
    fn test_network_selector_default() {
        let selector = NetworkSelector::new("default");

        let networks = selector.select_networks(&[], &[]);
        assert_eq!(networks, vec!["default"]);
    }
}
