//! PandaNetOS 外部互联 SDK（pnos-net）
//!
//! 提供 Agent 之间在公网/复杂网络环境下的直接 P2P 互联能力：
//! - NAT 探测与穿透（STUN / UPnP / PCP / NAT-PMP / UDP 打洞）
//! - 节点发现（LPD / PeerCache / DHT）
//! - 连接策略引擎（TCP 直连 / 打洞 / 中继自动选择）
//! - NetAgent 统一入口
//!
//! 与 pnos-comm 的区别：
//! - pnos-comm：内部通信（Agent↔Runtime、Agent↔Agent 经 Runtime 中转）
//! - pnos-net：外部互联（Agent↔Agent 公网直连，NAT 穿透）
//!
//! # 快速开始
//!
//! ```ignore
//! use pnos_net::NetAgent;
//!
//! let agent = NetAgent::builder(my_node_id, 6885, &data_dir)
//!     .api_port(6880)
//!     .build()
//!     .await?;
//! agent.start().await?;
//!
//! // 连接到对端（自动尝试直连→打洞）
//! let conn = agent.connect_to(peer_id, &addrs, reachability, None).await?;
//! ```

pub mod connector;
pub mod discovery;
pub mod dns;
pub mod nat;
pub mod net_agent;
pub mod strategy;
pub mod transport;
pub mod types;

// 常用类型 re-export
pub use dns::DnsPool;
pub use net_agent::{NetAgent, NetAgentBuilder, NetAgentConfig, NetEvent};
pub use strategy::{ConnectMethod, ConnectResult, ConnectStrategy, ConnectStrategyConfig};
pub use types::{DiscoveredNode, DiscoverySource, NodeAddress, NodeId, Reachability};
