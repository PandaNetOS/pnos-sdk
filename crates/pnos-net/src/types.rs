//! pnos-net 核心类型
//!
//! 节点 ID、地址、可达性等基础类型，供发现层和连接层共用。

use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};

use serde::{Deserialize, Serialize};

/// 20 字节节点 ID（与 DHT NodeId 兼容）
#[derive(Clone, Copy)]
pub struct NodeId(pub [u8; 20]);

impl NodeId {
    pub fn random() -> Self {
        let mut id = [0u8; 20];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut id);
        Self(id)
    }

    pub fn from_hex(s: &str) -> anyhow::Result<Self> {
        if s.len() != 40 {
            anyhow::bail!(
                "节点 ID 必须是 40 字符十六进制字符串，实际长度: {}",
                s.len()
            );
        }
        let mut id = [0u8; 20];
        for i in 0..20 {
            id[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|e| anyhow::anyhow!("十六进制解析失败: {}", e))?;
        }
        Ok(Self(id))
    }

    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(40);
        for b in &self.0 {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.to_hex())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..8])
    }
}

impl PartialEq for NodeId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for NodeId {}

impl Hash for NodeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        if bytes.len() != 20 {
            return Err(serde::de::Error::custom(format!(
                "NodeId 需要 20 字节，实际 {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes);
        Ok(NodeId(arr))
    }
}

/// 节点可达性等级
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Reachability {
    PublicIpv6,
    Mapped,
    HolePunchable,
    OutboundOnly,
    Unknown,
}

impl fmt::Display for Reachability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reachability::PublicIpv6 => write!(f, "PublicIpv6"),
            Reachability::Mapped => write!(f, "Mapped"),
            Reachability::HolePunchable => write!(f, "HolePunchable"),
            Reachability::OutboundOnly => write!(f, "OutboundOnly"),
            Reachability::Unknown => write!(f, "Unknown"),
        }
    }
}

/// 地址类型分类（用于连接优先级：内网优先）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EndpointKind {
    /// 内网（10.x / 172.16-31.x / 192.168.x）
    Lan,
    /// 公网 IPv4
    Public,
    /// 公网 IPv6
    Ipv6,
    /// 本机回环
    Loopback,
}

impl fmt::Display for EndpointKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EndpointKind::Lan => write!(f, "Lan"),
            EndpointKind::Public => write!(f, "Public"),
            EndpointKind::Ipv6 => write!(f, "Ipv6"),
            EndpointKind::Loopback => write!(f, "Loopback"),
        }
    }
}

/// 单个节点地址（带类型分类和连接质量）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeEndpoint {
    /// 地址
    pub addr: SocketAddr,
    /// 地址类型
    pub kind: EndpointKind,
    /// 发现来源
    pub source: DiscoverySource,
    /// 最后一次连接成功时间（Unix 秒）
    pub last_success: u64,
    /// 累计成功连接次数
    pub success_count: u32,
    /// 连续失败次数
    pub fail_count: u32,
    /// 平均延迟（毫秒，None = 未测过）
    pub latency_ms: Option<u32>,
}

impl NodeEndpoint {
    /// 根据 IP 地址自动分类内网/公网/回环
    pub fn classify(addr: &SocketAddr) -> EndpointKind {
        match addr.ip() {
            IpAddr::V4(ip) => {
                if ip.is_loopback() {
                    EndpointKind::Loopback
                } else if ip.is_private() {
                    EndpointKind::Lan
                } else {
                    EndpointKind::Public
                }
            }
            IpAddr::V6(_) => EndpointKind::Ipv6,
        }
    }

    /// 优先级排序用：数值越小优先级越高
    pub fn priority_weight(&self) -> u8 {
        match self.kind {
            EndpointKind::Lan => 0,
            EndpointKind::Loopback => 1,
            EndpointKind::Public => 2,
            EndpointKind::Ipv6 => 3,
        }
    }
}

/// 节点地址信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAddress {
    pub node_id: [u8; 20],
    pub ipv4_addr: Option<SocketAddr>,
    pub ipv6_addr: Option<SocketAddr>,
    pub reachability: Reachability,
    pub last_seen: u64,
    pub nat_type: Option<String>,
    /// 多 endpoint 列表（内网/公网都存，用于连接优先级选择）
    #[serde(default)]
    pub endpoints: Vec<NodeEndpoint>,
}

impl NodeAddress {
    /// 兼容旧接口：返回第一个可用地址（优先 IPv4）
    pub fn preferred_addr(&self) -> Option<SocketAddr> {
        self.ipv4_addr.or(self.ipv6_addr)
    }

    /// 按优先级选最佳地址：内网优先 → 同类型按延迟升序 → 按成功次数降序
    /// 连续失败超过阈值的 endpoint 排除
    pub fn best_endpoint(&self) -> Option<&NodeEndpoint> {
        self.endpoints
            .iter()
            .filter(|e| e.fail_count < 20)
            .min_by(|a, b| {
                a.priority_weight()
                    .cmp(&b.priority_weight())
                    .then_with(|| a.latency_ms.cmp(&b.latency_ms))
                    .then_with(|| b.success_count.cmp(&a.success_count))
            })
    }
}

/// 发现的节点（发现层输出的统一格式）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredNode {
    /// 节点 ID
    pub node_id: NodeId,
    /// 已知地址列表（可能有公网/内网/IPv6多个）
    pub addresses: Vec<SocketAddr>,
    /// 可达性（如果已知）
    pub reachability: Reachability,
    /// NAT 类型（如果已知）
    pub nat_type: Option<String>,
    /// 发现来源
    pub source: DiscoverySource,
    /// 最后发现时间（Unix 秒）
    pub last_seen: u64,
}

/// 节点发现来源
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiscoverySource {
    /// DHT 魔法 infohash 发现
    Dht,
    /// LPD 局域网多播发现
    Lpd,
    /// 历史节点缓存
    PeerCache,
    /// 配置的种子节点
    Seed,
    /// PEX 节点交换（从已连接节点获取）
    Pex,
    /// MQTT Rendezvous 公共 broker 发现
    Mqtt,
}

impl fmt::Display for DiscoverySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiscoverySource::Dht => write!(f, "DHT"),
            DiscoverySource::Lpd => write!(f, "LPD"),
            DiscoverySource::PeerCache => write!(f, "PeerCache"),
            DiscoverySource::Seed => write!(f, "Seed"),
            DiscoverySource::Pex => write!(f, "PEX"),
            DiscoverySource::Mqtt => write!(f, "MQTT"),
        }
    }
}
