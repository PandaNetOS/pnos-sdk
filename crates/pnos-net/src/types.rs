//! pnos-net 核心类型
//!
//! 节点 ID、地址、可达性等基础类型，供发现层和连接层共用。

use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

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
            anyhow::bail!("节点 ID 必须是 40 字符十六进制字符串，实际长度: {}", s.len());
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

/// 节点地址信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAddress {
    pub node_id: [u8; 20],
    pub ipv4_addr: Option<SocketAddr>,
    pub ipv6_addr: Option<SocketAddr>,
    pub reachability: Reachability,
    pub last_seen: u64,
    pub nat_type: Option<String>,
}

impl NodeAddress {
    pub fn preferred_addr(&self) -> Option<SocketAddr> {
        self.ipv4_addr.or(self.ipv6_addr)
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
