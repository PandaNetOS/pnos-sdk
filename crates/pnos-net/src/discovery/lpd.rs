//! LPD（Local Peer Discovery）局域网多播发现
//!
//! 通过 UDP 多播在局域网内自动发现同一网络中的其他节点，
//! 无需手动配置 seed_nodes 即可实现零配置局域网组网。
//!
//! 工作原理：
//! 1. 每个节点启动后，每隔固定间隔向 LPD 多播组 announce 自己的身份与端口
//! 2. 同时监听多播组，接收其他节点的 announce
//! 3. 将收到的远端节点通过 discovered_tx 发送给上层
//!
//! 消息格式：4 字节魔数 `b"PDCL"` + bincode 序列化的 [`LpdAnnounceMessage`]。

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{debug, info, trace, warn};

use crate::types::{DiscoveredNode, DiscoverySource, NodeId, Reachability};

/// LPD 多播消息魔数（4 字节），用于过滤非本协议的多播流量
pub const LPD_MAGIC: &[u8; 4] = b"PDCL";

/// 默认 LPD 多播地址（本地管理组）
pub const DEFAULT_LPD_MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(239, 255, 43, 21);

/// 默认广播间隔（秒）
pub const DEFAULT_LPD_BROADCAST_INTERVAL_SECS: u64 = 5;

/// LPD 广播消息（bincode 序列化）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LpdAnnounceMessage {
    /// 发送方节点 ID（20字节）
    pub node_id: [u8; 20],
    /// 联邦监听端口（TCP+UDP）
    pub federation_port: u16,
    /// API/HTTP 监控端口
    pub api_port: u16,
    /// 能力位掩码（预留，当前为 0）
    pub capabilities: u32,
    /// 本地数据条目总数（用于对端判断数据完整度）
    pub data_entry_count: u32,
}

impl LpdAnnounceMessage {
    /// 序列化为「魔数 + payload」的完整多播报文
    pub fn to_wire(&self) -> anyhow::Result<Vec<u8>> {
        let payload = bincode::serialize(self)?;
        let mut wire = Vec::with_capacity(LPD_MAGIC.len() + payload.len());
        wire.extend_from_slice(LPD_MAGIC);
        wire.extend_from_slice(&payload);
        Ok(wire)
    }

    /// 从完整多播报文中解析（校验魔数后反序列化）
    pub fn from_wire(data: &[u8]) -> anyhow::Result<Self> {
        if data.len() < LPD_MAGIC.len() || &data[..LPD_MAGIC.len()] != LPD_MAGIC {
            anyhow::bail!("LPD 魔数不匹配，忽略非本协议多播流量");
        }
        let msg = bincode::deserialize(&data[LPD_MAGIC.len()..])?;
        Ok(msg)
    }
}

/// LPD 局域网多播发现服务
///
/// 发现的节点通过 `discovered_tx` 广播出去，上层订阅后自行处理。
pub struct LpdDiscoveryService {
    /// 本机节点 ID（用于跳过自己的广播）
    my_node_id: [u8; 20],
    /// 本机联邦监听端口（announce 时告知其他节点）
    federation_port: u16,
    /// 本机 API/HTTP 监控端口
    api_port: u16,
    /// 多播地址
    multicast_addr: Ipv4Addr,
    /// 多播端口
    multicast_port: u16,
    /// 广播间隔（秒）
    broadcast_interval_secs: u64,
    /// 发现事件发送端
    discovered_tx: broadcast::Sender<DiscoveredNode>,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
}

impl LpdDiscoveryService {
    /// 创建 LPD 发现服务
    pub fn new(
        my_node_id: [u8; 20],
        federation_port: u16,
        api_port: u16,
        multicast_port: u16,
        discovered_tx: broadcast::Sender<DiscoveredNode>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            my_node_id,
            federation_port,
            api_port,
            multicast_addr: DEFAULT_LPD_MULTICAST_ADDR,
            multicast_port,
            broadcast_interval_secs: DEFAULT_LPD_BROADCAST_INTERVAL_SECS,
            discovered_tx,
            shutdown,
        }
    }

    /// 覆盖默认多播地址
    pub fn with_multicast_addr(mut self, addr: Ipv4Addr) -> Self {
        self.multicast_addr = addr;
        self
    }

    /// 覆盖默认广播间隔（秒）
    pub fn with_broadcast_interval(mut self, secs: u64) -> Self {
        self.broadcast_interval_secs = secs;
        self
    }

    /// 启动后台任务（绑定多播端口 + 加入多播组 + 定期广播 + 接收处理）
    pub fn spawn(self: Arc<Self>) {
        let mut shutdown_rx = self.shutdown.subscribe();
        tokio::spawn(async move {
            // 1. 绑定 0.0.0.0:multicast_port（使用 SO_REUSEADDR 允许多节点同机共享多播端口）
            let bind_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, self.multicast_port));
            let std_socket = match (|| -> anyhow::Result<std::net::UdpSocket> {
                let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
                sock.set_reuse_address(true)?;
                sock.bind(&bind_addr.into())?;
                Ok(sock.into())
            })() {
                Ok(s) => s,
                Err(e) => {
                    warn!("[net-lpd] 绑定多播端口 {} 失败，LPD 未启动: {}", self.multicast_port, e);
                    return;
                }
            };
            let socket = Arc::new(match UdpSocket::from_std(std_socket) {
                Ok(s) => s,
                Err(e) => {
                    warn!("[net-lpd] 转换 tokio UDP socket 失败，LPD 未启动: {}", e);
                    return;
                }
            });

            // 2. 加入多播组
            if let Err(e) = socket.join_multicast_v4(self.multicast_addr, Ipv4Addr::UNSPECIFIED) {
                warn!("[net-lpd] 加入多播组 {} 失败，LPD 未启动: {}", self.multicast_addr, e);
                return;
            }
            let _ = socket.set_multicast_loop_v4(true);

            info!(
                "[net-lpd] LPD 多播发现已启动: group={}:{}, 联邦端口:{}, API端口:{}, 间隔:{}s",
                self.multicast_addr,
                self.multicast_port,
                self.federation_port,
                self.api_port,
                self.broadcast_interval_secs
            );

            // 3. 多播目的地址
            let dst = SocketAddr::new(std::net::IpAddr::V4(self.multicast_addr), self.multicast_port);

            let mut ticker = tokio::time::interval(Duration::from_secs(self.broadcast_interval_secs));
            let mut buf = vec![0u8; 2048];

            // 4. 主循环
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = self.broadcast_once(&socket, &dst).await {
                            debug!("[net-lpd] 广播失败: {}", e);
                        }
                    }
                    recv_result = socket.recv_from(&mut buf) => {
                        match recv_result {
                            Ok((len, src)) => {
                                self.handle_received_message(&buf[..len], src);
                            }
                            Err(e) => {
                                debug!("[net-lpd] 接收失败: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("[net-lpd] 收到关闭信号，退出 LPD 发现任务");
                        break;
                    }
                }
            }

            info!("[net-lpd] LPD 多播发现已停止");
        });
    }

    /// 向多播组发送一次本机 announce
    async fn broadcast_once(&self, socket: &UdpSocket, dst: &SocketAddr) -> anyhow::Result<()> {
        let msg = LpdAnnounceMessage {
            node_id: self.my_node_id,
            federation_port: self.federation_port,
            api_port: self.api_port,
            capabilities: 0,
            data_entry_count: 0,
        };
        let wire = msg.to_wire()?;
        let sent = socket.send_to(&wire, dst).await?;
        trace!(
            "[net-lpd] 已广播 announce，{} 字节 -> {}",
            sent,
            dst
        );
        Ok(())
    }

    /// 处理一条收到的多播消息（纯逻辑，无网络）
    ///
    /// 返回 true 表示成功处理了一个远端节点并发送了发现事件。
    pub fn handle_received_message(&self, data: &[u8], src: SocketAddr) -> bool {
        // 1. 校验魔数并反序列化
        let msg = match LpdAnnounceMessage::from_wire(data) {
            Ok(m) => m,
            Err(_) => return false,
        };

        // 2. 跳过自己
        if msg.node_id == self.my_node_id {
            debug!("[net-lpd] 跳过自己的广播");
            return false;
        }

        // 3. 仅处理 IPv4 源地址
        let peer_ip = match src.ip() {
            std::net::IpAddr::V4(ip) => ip,
            std::net::IpAddr::V6(_) => return false,
        };

        // 4. 构造 DiscoveredNode
        let now = current_unix_secs();
        let discovered = DiscoveredNode {
            node_id: NodeId(msg.node_id),
            addresses: vec![SocketAddr::new(
                std::net::IpAddr::V4(peer_ip),
                msg.federation_port,
            )],
            reachability: Reachability::Unknown,
            nat_type: None,
            source: DiscoverySource::Lpd,
            last_seen: now,
        };

        // 5. 发送发现事件（订阅者可能不存在，忽略错误）
        let is_new = self.discovered_tx.send(discovered).is_ok();
        if is_new {
            info!(
                "[net-lpd] 发现局域网新节点: {} ({}:{}), API端口:{}",
                hex::encode(&msg.node_id[..8]),
                peer_ip,
                msg.federation_port,
                msg.api_port,
            );
        }
        true
    }
}

/// 获取当前 Unix 时间戳（秒）
fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_service(multicast_port: u16) -> LpdDiscoveryService {
        let (shutdown_tx, _rx) = broadcast::channel(1);
        let (discovered_tx, _rx) = broadcast::channel(32);
        LpdDiscoveryService::new(
            [0xAA; 20],
            6885,
            6880,
            multicast_port,
            discovered_tx,
            shutdown_tx,
        )
    }

    fn make_remote_wire(node_id: [u8; 20], federation_port: u16) -> Vec<u8> {
        let msg = LpdAnnounceMessage {
            node_id,
            federation_port,
            api_port: 9090,
            capabilities: 0,
            data_entry_count: 12345,
        };
        msg.to_wire().unwrap()
    }

    #[test]
    fn test_lpd_message_serde_roundtrip() {
        let msg = LpdAnnounceMessage {
            node_id: [0xAB; 20],
            federation_port: 6885,
            api_port: 6880,
            capabilities: 3,
            data_entry_count: 999,
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: LpdAnnounceMessage = bincode::deserialize(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_lpd_message_with_magic() {
        let msg = LpdAnnounceMessage {
            node_id: [0x11; 20],
            federation_port: 6885,
            api_port: 6880,
            capabilities: 0,
            data_entry_count: 42,
        };
        let wire = msg.to_wire().unwrap();
        assert_eq!(&wire[..4], b"PDCL");
        let decoded = LpdAnnounceMessage::from_wire(&wire).unwrap();
        assert_eq!(decoded, msg);
        let mut bad = wire.clone();
        bad[0] = 0x00;
        assert!(LpdAnnounceMessage::from_wire(&bad).is_err());
        assert!(LpdAnnounceMessage::from_wire(b"PD").is_err());
    }

    #[test]
    fn test_service_creation() {
        let svc = make_test_service(6771);
        assert_eq!(svc.multicast_addr, Ipv4Addr::new(239, 255, 43, 21));
        assert_eq!(svc.multicast_port, 6771);
        assert_eq!(svc.broadcast_interval_secs, 5);
        assert_eq!(svc.federation_port, 6885);
        assert_eq!(svc.api_port, 6880);

        let svc2 = make_test_service(16771)
            .with_multicast_addr(Ipv4Addr::new(239, 255, 43, 99))
            .with_broadcast_interval(10);
        assert_eq!(svc2.multicast_addr, Ipv4Addr::new(239, 255, 43, 99));
        assert_eq!(svc2.multicast_port, 16771);
        assert_eq!(svc2.broadcast_interval_secs, 10);
    }

    #[test]
    fn test_skip_self() {
        let svc = make_test_service(6771);
        let self_wire = make_remote_wire([0xAA; 20], 6885);
        let src: SocketAddr = "192.168.1.100:54321".parse().unwrap();
        let processed = svc.handle_received_message(&self_wire, src);
        assert!(!processed, "自己的广播不应被处理");
    }

    #[test]
    fn test_process_remote_node() {
        let (shutdown_tx, _rx) = broadcast::channel(1);
        let (discovered_tx, mut discovered_rx) = broadcast::channel(32);
        let svc = LpdDiscoveryService::new(
            [0xAA; 20],
            6885,
            6880,
            6771,
            discovered_tx,
            shutdown_tx,
        );

        let mut remote_id = [0u8; 20];
        remote_id.copy_from_slice(b"REMOTE_NODE_ID_12345");
        let wire = make_remote_wire(remote_id, 6885);
        let src: SocketAddr = "192.168.1.200:54321".parse().unwrap();

        let processed = svc.handle_received_message(&wire, src);
        assert!(processed, "远端节点应被处理");

        // 验证发现事件
        let discovered = discovered_rx.try_recv().expect("应收到发现事件");
        assert_eq!(discovered.node_id, NodeId(remote_id));
        assert_eq!(discovered.addresses.len(), 1);
        assert_eq!(discovered.addresses[0], "192.168.1.200:6885".parse().unwrap());
        assert_eq!(discovered.source, DiscoverySource::Lpd);

        // 无效魔数报文不应产生事件
        let garbage: SocketAddr = "10.0.0.5:1111".parse().unwrap();
        assert!(!svc.handle_received_message(b"XXXXgarbage", garbage));
        assert!(discovered_rx.try_recv().is_err(), "不应有额外事件");
    }
}
