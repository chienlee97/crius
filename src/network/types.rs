use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::IpAddr;

/// Pod 主 IP 选择策略。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MainIpPreference {
    Ipv4,
    Ipv6,
    #[default]
    Cni,
}

impl MainIpPreference {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ipv4 => "ipv4",
            Self::Ipv6 => "ipv6",
            Self::Cni => "cni",
        }
    }
}

/// 网络状态
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NetworkStatus {
    /// 网络名称
    pub name: String,

    /// IP 地址
    pub ip: Option<IpAddr>,

    /// MAC 地址
    pub mac: Option<String>,

    /// 网络接口列表
    pub interfaces: Vec<NetworkInterface>,

    /// 原始 CNI 结果，用于恢复 additional IP 顺序和调试
    pub raw_result: Option<Value>,
}

/// 网络接口信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInterface {
    /// 接口名称
    pub name: String,

    /// IP 地址
    pub ip: Option<IpAddr>,

    /// MAC 地址
    pub mac: Option<String>,

    /// 子网掩码
    pub netmask: Option<String>,

    /// 网关
    pub gateway: Option<IpAddr>,
}

/// CNI 网络配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// 网络名称
    pub name: String,

    /// 网络类型 (bridge, macvlan, ipvlan 等)
    pub r#type: String,

    /// 网桥名称 (如果适用)
    pub bridge: Option<String>,

    /// 子网 (CIDR 表示法)
    pub subnet: Option<String>,

    /// 网关地址
    pub gateway: Option<String>,

    /// 是否启用 IP 转发
    pub ip_masq: Option<bool>,

    /// MTU
    pub mtu: Option<u32>,

    /// 其他配置
    #[serde(flatten)]
    pub other: std::collections::HashMap<String, serde_json::Value>,
}
