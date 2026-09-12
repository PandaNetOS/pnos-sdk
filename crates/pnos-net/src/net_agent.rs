//! NetAgent：外部互联统一入口
//!
//! 整合 NAT 探测、节点发现、连接建立为一个统一入口。
//! 生态项目只需创建 NetAgent，调用 connect_to() 即可，
//! 所有"连不上"的问题都在 SDK 内部解决。
//!
//! # 典型用法
//!
//! ```ignore
//! use pnos_net::NetAgent;
//!
//! let agent = NetAgent::builder()
//!     .node_id(my_node_id)
//!     .listen_port(6885)
//!     .data_dir(&data_dir)
//!     .build()?;
//!
//! agent.start().await?;
//!
//! // 订阅发现事件
//! let mut discovered = agent.discovered_nodes();
//! tokio::spawn(async move {
//!     while let Ok(node) = discovered.recv().await {
//!         // 发现新节点，自动尝试连接
//!         agent.connect_to(node.node_id, &node.addresses, node.reachability).await;
//!     }
//! });
//! ```

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::discovery::lpd::{LpdDiscoveryService, DEFAULT_LPD_MULTICAST_ADDR};
use crate::discovery::peer_cache::PeerCache;
use crate::nat::{HolePuncher, HolePunchConfig, NatConfig, NatManager, NatStatus};
use crate::strategy::{ConnectMethod, ConnectStrategy, ConnectStrategyConfig, ConnectResult};
use crate::transport::tcp::TcpTransport;
use crate::transport::{IrohTransport, IrohTransportConfig, Transport, TransportKind, TransportMode, TransportRouter};
use crate::types::{DiscoveredNode, NodeId, Reachability};

/// NetAgent 配置
#[derive(Debug, Clone)]
pub struct NetAgentConfig {
    /// 本机节点 ID（20字节）
    pub node_id: [u8; 20],
    /// 联邦/应用监听端口（TCP）
    pub listen_port: u16,
    /// API/HTTP 监控端口
    pub api_port: u16,
    /// 数据目录（用于 PeerCache 持久化）
    pub data_dir: PathBuf,
    /// LPD 多播端口（默认 6771）
    pub lpd_multicast_port: u16,
    /// 是否启用 LPD（默认 true）
    pub lpd_enabled: bool,
    /// 是否启用 PeerCache（默认 true）
    pub peer_cache_enabled: bool,
    /// 是否启用 NAT 探测和端口映射（默认 true）
    pub nat_enabled: bool,
    /// 是否启用 UDP 打洞（默认 true）
    pub hole_punch_enabled: bool,
    /// 连接策略配置
    pub connect_config: ConnectStrategyConfig,
    /// 传输模式（默认 TcpOnly）
    pub transport_mode: TransportMode,
    /// Iroh 传输配置（transport_mode != TcpOnly 时使用）
    pub iroh_config: Option<IrohTransportConfig>,
}

impl NetAgentConfig {
    /// 创建默认配置
    pub fn new(node_id: [u8; 20], listen_port: u16, data_dir: &Path) -> Self {
        Self {
            node_id,
            listen_port,
            api_port: 0,
            data_dir: data_dir.to_path_buf(),
            lpd_multicast_port: 6771,
            lpd_enabled: true,
            peer_cache_enabled: true,
            nat_enabled: true,
            hole_punch_enabled: true,
            connect_config: ConnectStrategyConfig::default(),
            transport_mode: TransportMode::TcpOnly,
            iroh_config: None,
        }
    }
}

/// NetAgent 事件
#[derive(Debug, Clone)]
pub enum NetEvent {
    /// 发现新节点
    NodeDiscovered(DiscoveredNode),
    /// 连接成功
    Connected {
        node_id: NodeId,
        addr: SocketAddr,
        method: String,
        latency: Duration,
    },
    /// 连接失败
    ConnectFailed {
        node_id: NodeId,
        reason: String,
    },
    /// NAT 状态变化
    NatStatusChanged(NatStatus),
    /// 本机映射地址变化
    MappedAddressChanged(Option<SocketAddr>),
}

/// NetAgent：外部互联统一入口
pub struct NetAgent {
    config: NetAgentConfig,
    /// NAT 管理器
    nat_manager: Option<Arc<NatManager>>,
    /// UDP 打洞器
    hole_puncher: Option<Arc<HolePuncher>>,
    /// 传输路由器
    transport: Arc<TransportRouter>,
    /// 节点缓存
    peer_cache: RwLock<PeerCache>,
    /// 事件广播
    event_tx: broadcast::Sender<NetEvent>,
    /// 发现事件广播（外部发现器可通过此通道输出发现结果）
    discovered_tx: broadcast::Sender<DiscoveredNode>,
    /// 已注册的外部发现器（如 DHT）
    external_discoveries: RwLock<Vec<Arc<dyn crate::discovery::Discovery>>>,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
    /// 是否已启动
    started: RwLock<bool>,
}

impl NetAgent {
    /// 创建 NetAgent
    pub async fn new(config: NetAgentConfig) -> anyhow::Result<Arc<Self>> {
        let (event_tx, _) = broadcast::channel(256);
        let (discovered_tx, _) = broadcast::channel(256);
        let (shutdown, _) = broadcast::channel(1);

        // 加载 PeerCache
        let peer_cache = if config.peer_cache_enabled {
            PeerCache::load(&config.data_dir)
        } else {
            PeerCache::default()
        };

        // 创建 NAT 管理器
        let nat_manager = if config.nat_enabled {
            let nat_config = NatConfig::default();
            Some(Arc::new(NatManager::new(nat_config)))
        } else {
            None
        };

        // 创建 UDP 打洞器
        let hole_puncher = if config.hole_punch_enabled {
            let punch_config = HolePunchConfig::default();
            let bind_addr = format!("0.0.0.0:{}", config.listen_port + 1);
            match HolePuncher::new(&bind_addr, punch_config).await {
                Ok(hp) => Some(Arc::new(hp)),
                Err(e) => {
                    warn!("[net-agent] HolePuncher 创建失败，打洞功能不可用: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // 创建连接策略引擎 → 包装为 TcpTransport → 包装为 TransportRouter
        let mut strategy = ConnectStrategy::new(config.connect_config.clone());
        if let Some(hp) = &hole_puncher {
            strategy = strategy.with_hole_puncher(hp.clone());
        }
        let tcp_transport = TcpTransport::new(strategy);
        let mut router = TransportRouter::new(config.transport_mode)
            .with_tcp(tcp_transport);
        // 注入 IrohTransport（如果配置了）
        if let Some(iroh_cfg) = &config.iroh_config {
            let iroh_transport = IrohTransport::new(iroh_cfg.clone());
            router = router.with_iroh(iroh_transport);
        }
        let transport = router;

        Ok(Arc::new(Self {
            config,
            nat_manager,
            hole_puncher,
            transport: Arc::new(transport),
            peer_cache: RwLock::new(peer_cache),
            event_tx,
            discovered_tx,
            external_discoveries: RwLock::new(Vec::new()),
            shutdown,
            started: RwLock::new(false),
        }))
    }

    /// 仅启动传输层（TCP + Iroh），不启动 NAT 映射和发现服务
    /// 适用于已有独立发现机制的调用方（如 PDC 联邦层）
    pub async fn start_transport_only(self: &Arc<Self>) -> anyhow::Result<()> {
        if *self.started.read() {
            warn!("[net-agent] NetAgent 已启动，忽略重复启动");
            return Ok(());
        }
        info!("[net-agent] 仅启动传输层（TCP + Iroh）...");
        self.transport.start().await?;
        *self.started.write() = true;
        info!("[net-agent] 传输层启动完成");
        Ok(())
    }

    /// 启动 NetAgent（NAT 探测 + 发现机制）
    pub async fn start(self: &Arc<Self>) -> anyhow::Result<()> {
        if *self.started.read() {
            warn!("[net-agent] NetAgent 已启动，忽略重复启动");
            return Ok(());
        }

        info!("[net-agent] 启动外部互联 SDK...");

        // 0. 启动传输层（TCP + Iroh）
        self.transport.start().await?;

        // 1. NAT 端口映射（自动映射 TCP+UDP 的 listen_port）
        if let Some(nat_mgr) = &self.nat_manager {
            let listen_port = self.config.listen_port;
            // TCP 映射
            match nat_mgr
                .map_port(igd::PortMappingProtocol::TCP, listen_port, "pnos-net TCP")
                .await
            {
                Ok(ext) => {
                    info!("[net-agent] TCP 端口映射成功: {} → 外部 {}", listen_port, ext);
                }
                Err(e) => {
                    warn!("[net-agent] TCP 端口映射失败: {}", e);
                }
            }
            // UDP 映射（用于打洞）
            match nat_mgr
                .map_port(igd::PortMappingProtocol::UDP, listen_port, "pnos-net UDP")
                .await
            {
                Ok(ext) => {
                    info!("[net-agent] UDP 端口映射成功: {} → 外部 {}", listen_port, ext);
                }
                Err(e) => {
                    debug!("[net-agent] UDP 端口映射失败: {}", e);
                }
            }
            // 发送 NAT 状态事件
            let status = nat_mgr.status();
            let _ = self.event_tx.send(NetEvent::NatStatusChanged(status));
        }

        // 2. 启动统一的发现事件转发（discovered_tx -> event_tx）
        {
            let mut discovered_rx = self.discovered_tx.subscribe();
            let event_tx = self.event_tx.clone();
            let mut shutdown_rx = self.shutdown.subscribe();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = discovered_rx.recv() => {
                            match result {
                                Ok(node) => {
                                    let _ = event_tx.send(NetEvent::NodeDiscovered(node));
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(_) => break,
                            }
                        }
                        _ = shutdown_rx.recv() => break,
                    }
                }
            });
        }

        // 2.1 启动 LPD 发现
        if self.config.lpd_enabled {
            let lpd = Arc::new(LpdDiscoveryService::new(
                self.config.node_id,
                self.config.listen_port,
                self.config.api_port,
                self.config.lpd_multicast_port,
                self.discovered_tx.clone(),
                self.shutdown.clone(),
            ));
            lpd.spawn();

            info!(
                "[net-agent] LPD 发现已启动: {}:{}",
                DEFAULT_LPD_MULTICAST_ADDR, self.config.lpd_multicast_port
            );
        }

        // 2.2 启动已注册的外部发现器（如 DHT）
        {
            let discoveries = self.external_discoveries.read().clone();
            for discovery in discoveries {
                if discovery.enabled() {
                    match discovery.start().await {
                        Ok(_) => info!("[net-agent] 外部发现器已启动: {}", discovery.name()),
                        Err(e) => warn!("[net-agent] 外部发现器 {} 启动失败: {}", discovery.name(), e),
                    }
                }
            }
        }

        // 3. 启动 PeerCache 定期保存
        if self.config.peer_cache_enabled {
            let self_clone = self.clone();
            let mut shutdown_rx = self.shutdown.subscribe();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(300));
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            if let Err(e) = self_clone.save_peer_cache() {
                                debug!("[net-agent] PeerCache 保存失败: {}", e);
                            }
                        }
                        _ = shutdown_rx.recv() => break,
                    }
                }
            });
        }

        *self.started.write() = true;
        info!("[net-agent] 外部互联 SDK 启动完成");
        Ok(())
    }

    /// 连接到对端（自动选择最优连接方式）
    ///
    /// 这是核心 API：传入对端地址和可达性，SDK 自动尝试
    /// TCP直连 → UDP打洞 → （中继预留），返回第一个成功的连接。
    pub async fn connect_to(
        &self,
        node_id: NodeId,
        addrs: &[SocketAddr],
        peer_reachability: Reachability,
        peer_nat_type: Option<String>,
    ) -> anyhow::Result<ConnectResult> {
        if addrs.is_empty() {
            let reason = "对端没有已知地址".to_string();
            let _ = self.event_tx.send(NetEvent::ConnectFailed {
                node_id,
                reason: reason.clone(),
            });
            anyhow::bail!(reason);
        }

        info!(
            "[net-agent] 开始连接节点 {} (地址数={}, 可达性={})",
            node_id,
            addrs.len(),
            peer_reachability
        );

        match self
            .transport
            .connect(node_id, addrs, peer_reachability, peer_nat_type)
            .await
        {
            Ok(result) => {
                let connected_addr = result.connected_addr.unwrap_or(addrs[0]);
                let method = match result.kind {
                    TransportKind::Tcp => ConnectMethod::TcpDirect,
                    TransportKind::UdpHolePunch => ConnectMethod::UdpHolePunch,
                    TransportKind::TcpRelay => ConnectMethod::Relay,
                    TransportKind::IrohDirect
                    | TransportKind::IrohHolePunch
                    | TransportKind::IrohRelay => ConnectMethod::TcpDirect,
                };
                let connect_result = ConnectResult {
                    connection: result.stream,
                    method,
                    total_latency: result.latency,
                };

                let _ = self.event_tx.send(NetEvent::Connected {
                    node_id,
                    addr: connected_addr,
                    method: method.to_string(),
                    latency: result.latency,
                });

                // 连接成功，更新 PeerCache
                if self.config.peer_cache_enabled {
                    let addr_str = connected_addr.to_string();
                    self.peer_cache.write().upsert(&node_id.0, &addr_str, true);
                }

                Ok(connect_result)
            }
            Err(e) => {
                let reason = e.to_string();
                let _ = self.event_tx.send(NetEvent::ConnectFailed {
                    node_id,
                    reason: reason.clone(),
                });
                Err(e)
            }
        }
    }

    /// 订阅事件流
    pub fn events(&self) -> broadcast::Receiver<NetEvent> {
        self.event_tx.subscribe()
    }

    /// 获取发现事件发送端（供外部发现器输出发现结果）
    ///
    /// 外部发现器（如 DHT）创建时应传入此 sender，
    /// 发现新节点后通过它发送 DiscoveredNode 事件。
    pub fn discovered_tx(&self) -> broadcast::Sender<DiscoveredNode> {
        self.discovered_tx.clone()
    }

    /// 注册外部发现器（如 DHT 发现）
    ///
    /// 注册后，NetAgent.start() 会自动调用 discovery.start()。
    /// 外部发现器应在创建时使用 `agent.discovered_tx()` 作为事件输出通道。
    pub fn register_discovery(&self, discovery: Arc<dyn crate::discovery::Discovery>) {
        let name = discovery.name().to_string();
        self.external_discoveries.write().push(discovery);
        info!("[net-agent] 已注册外部发现器: {}", name);
    }

    /// 订阅发现事件（便捷方法）
    pub fn discovered_nodes(&self) -> broadcast::Receiver<DiscoveredNode> {
        let (tx, rx) = broadcast::channel(64);
        let mut events = self.event_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(event) = events.recv().await {
                if let NetEvent::NodeDiscovered(node) = event {
                    if tx.send(node).is_err() {
                        break;
                    }
                }
            }
        });
        rx
    }

    /// 获取 NAT 状态（如果已探测）
    pub fn nat_status(&self) -> Option<NatStatus> {
        self.nat_manager.as_ref().map(|m| m.status())
    }

    /// 获取本机映射地址（如果 NAT 映射成功）
    ///
    /// 从 NatStatus 的 external_ip 和 mappings 推算。
    pub fn mapped_address(&self) -> Option<SocketAddr> {
        let status = self.nat_status()?;
        let ip = status.external_ip?;
        // 取第一个 TCP 映射的外部端口
        let port = status
            .mappings
            .iter()
            .find(|m| m.protocol == "TCP")
            .map(|m| m.external_port)?;
        let addr = format!("{}:{}", ip, port).parse().ok()?;
        Some(addr)
    }

    /// 获取 NAT 管理器引用（用于高级操作如 init 端口映射）
    pub fn nat_manager(&self) -> Option<&Arc<NatManager>> {
        self.nat_manager.as_ref()
    }

    /// 获取 PeerCache 中成功率最高的前 n 个节点地址
    pub fn cached_peers(&self, n: usize) -> Vec<String> {
        self.peer_cache.read().top_addrs(n)
    }

    /// 保存 PeerCache 到磁盘
    pub fn save_peer_cache(&self) -> anyhow::Result<()> {
        if !self.config.peer_cache_enabled {
            return Ok(());
        }
        let cache = self.peer_cache.read().clone();
        cache.save(&self.config.data_dir)
    }

    /// 关闭 NetAgent
    pub fn shutdown(&self) {
        let _ = self.shutdown.send(());
        let _ = self.save_peer_cache();
        info!("[net-agent] NetAgent 已关闭");
    }
}

/// NetAgent 构建器
pub struct NetAgentBuilder {
    config: NetAgentConfig,
}

impl NetAgentBuilder {
    /// 创建构建器
    pub fn new(node_id: [u8; 20], listen_port: u16, data_dir: &Path) -> Self {
        Self {
            config: NetAgentConfig::new(node_id, listen_port, data_dir),
        }
    }

    /// 设置 API 端口
    pub fn api_port(mut self, port: u16) -> Self {
        self.config.api_port = port;
        self
    }

    /// 设置 LPD 多播端口
    pub fn lpd_multicast_port(mut self, port: u16) -> Self {
        self.config.lpd_multicast_port = port;
        self
    }

    /// 启用/禁用 LPD
    pub fn lpd_enabled(mut self, enabled: bool) -> Self {
        self.config.lpd_enabled = enabled;
        self
    }

    /// 启用/禁用 PeerCache
    pub fn peer_cache_enabled(mut self, enabled: bool) -> Self {
        self.config.peer_cache_enabled = enabled;
        self
    }

    /// 启用/禁用 NAT
    pub fn nat_enabled(mut self, enabled: bool) -> Self {
        self.config.nat_enabled = enabled;
        self
    }

    /// 启用/禁用打洞
    pub fn hole_punch_enabled(mut self, enabled: bool) -> Self {
        self.config.hole_punch_enabled = enabled;
        self
    }

    /// 设置连接策略配置
    pub fn connect_config(mut self, config: ConnectStrategyConfig) -> Self {
        self.config.connect_config = config;
        self
    }

    /// 构建 NetAgent
    pub async fn build(self) -> anyhow::Result<Arc<NetAgent>> {
        NetAgent::new(self.config).await
    }
}

impl NetAgent {
    /// 创建构建器
    pub fn builder(node_id: [u8; 20], listen_port: u16, data_dir: &Path) -> NetAgentBuilder {
        NetAgentBuilder::new(node_id, listen_port, data_dir)
    }
}
