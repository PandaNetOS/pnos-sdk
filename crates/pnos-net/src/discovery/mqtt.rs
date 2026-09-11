//! MQTT Rendezvous 发现（公网零配置主通道）
//!
//! 通过公共 MQTT Broker 实现跨网段、跨 NAT 的节点自动发现。
//! 无需公网服务器、无需用户配置，Broker 地址硬编码在代码中。
//!
//! 工作原理：
//! 1. 节点启动后连接公共 MQTT Broker（自动从列表中选可用的）
//! 2. Subscribe 到 `pnos/discovery/v1` topic
//! 3. Publish 自己的 node_id + 地址列表（retained，新节点连上立即收到）
//! 4. 定期心跳 publish（30s）
//! 5. 收到其他节点的消息后，通过 discovered_tx 发送给上层
//!
//! 免费公共 Broker：
//! - test.mosquitto.org:1883 (Eclipse)
//! - broker.hivemq.com:1883 (HiveMQ)
//! - mqtt.eclipseprojects.io:1883 (Eclipse)

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::types::{DiscoveredNode, DiscoverySource, NodeId, Reachability};

/// MQTT 发现 topic（所有 pnos 生态节点共用）
pub const MQTT_DISCOVERY_TOPIC: &str = "pnos/discovery/v1";

/// 默认心跳间隔（秒）
pub const DEFAULT_MQTT_HEARTBEAT_SECS: u64 = 30;

/// 公共 MQTT Broker 列表（硬编码，零配置）
pub const PUBLIC_BROKERS: &[(&str, u16)] = &[
    ("test.mosquitto.org", 1883),
    ("broker.hivemq.com", 1883),
    ("mqtt.eclipseprojects.io", 1883),
];

/// Broker 域名 → IP 直连 fallback（DNS 解析失败时使用）
/// 某些网络环境下 UDP 53 被封，DNS 解析超时，但 TCP 1883 可直连
pub const BROKER_IP_FALLBACK: &[(&str, &str)] = &[
    ("test.mosquitto.org", "54.36.178.49"),
    ("broker.hivemq.com", "35.156.188.238"),
    ("mqtt.eclipseprojects.io", "137.135.83.217"),
];

/// MQTT 发现消息（JSON 序列化）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttDiscoveryMessage {
    /// 协议版本
    pub version: u8,
    /// 发送方节点 ID（hex 字符串）
    pub node_id: String,
    /// 可连接地址列表（SocketAddr 字符串）
    pub addresses: Vec<String>,
    /// API/HTTP 监控端口
    pub api_port: u16,
    /// 联邦监听端口
    pub federation_port: u16,
    /// 能力位掩码（预留）
    pub capabilities: u32,
    /// 发送时间戳（Unix 秒）
    pub timestamp: u64,
}

impl MqttDiscoveryMessage {
    pub fn new(node_id: [u8; 20], addresses: Vec<SocketAddr>, api_port: u16, federation_port: u16) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            version: 1,
            node_id: hex::encode(node_id),
            addresses: addresses.into_iter().map(|a| a.to_string()).collect(),
            api_port,
            federation_port,
            capabilities: 0,
            timestamp: now,
        }
    }

    pub fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(data: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(data)?)
    }

    /// 解析地址列表为 SocketAddr
    pub fn parsed_addresses(&self) -> Vec<SocketAddr> {
        self.addresses
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect()
    }
}

/// MQTT Rendezvous 发现服务
pub struct MqttDiscoveryService {
    /// 自己的节点 ID
    my_node_id: [u8; 20],
    /// 联邦监听端口
    federation_port: u16,
    /// API/HTTP 监控端口
    api_port: u16,
    /// 自己的可连接地址（公网映射地址等）
    my_addresses: Vec<SocketAddr>,
    /// 心跳间隔（秒）
    heartbeat_secs: u64,
    /// 发现事件发送端
    discovered_tx: broadcast::Sender<DiscoveredNode>,
    /// 关闭信号
    shutdown: broadcast::Sender<()>,
    /// DNS 解析池（用于 DNS 污染/封锁环境下解析 broker 域名）
    dns_pool: Option<Arc<crate::dns::DnsPool>>,
}

impl MqttDiscoveryService {
    /// 创建 MQTT 发现服务
    pub fn new(
        my_node_id: [u8; 20],
        federation_port: u16,
        api_port: u16,
        my_addresses: Vec<SocketAddr>,
        discovered_tx: broadcast::Sender<DiscoveredNode>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        // 初始化 DNS 池（失败则为 None，回退到系统 DNS + IP fallback）
        let dns_pool = match crate::dns::DnsPool::new() {
            Ok(pool) => Some(Arc::new(pool)),
            Err(e) => {
                warn!("[net-mqtt] DNS 池初始化失败，使用系统 DNS: {}", e);
                None
            }
        };

        Self {
            my_node_id,
            federation_port,
            api_port,
            my_addresses,
            heartbeat_secs: DEFAULT_MQTT_HEARTBEAT_SECS,
            discovered_tx,
            shutdown,
            dns_pool,
        }
    }

    /// 覆盖心跳间隔
    pub fn with_heartbeat(mut self, secs: u64) -> Self {
        self.heartbeat_secs = secs;
        self
    }

    /// 启动后台任务（同时连接所有公共 broker + IP fallback，提高发现概率）
    pub fn spawn(self: Arc<Self>) {
        // 构建连接目标列表：域名 + IP fallback（DNS 失败时 IP 可直连）
        let mut targets: Vec<(String, u16)> = PUBLIC_BROKERS
            .iter()
            .map(|(h, p)| (h.to_string(), *p))
            .collect();
        for (host, ip) in BROKER_IP_FALLBACK {
            // 只在域名不在列表中时添加 IP（避免重复）
            if !targets.iter().any(|(h, _)| h == *ip) {
                targets.push((ip.to_string(), 1883));
            }
            let _ = host; // 域名已在上面的列表中
        }

        // 为每个目标创建独立的连接任务
        for (host, port) in targets {
            let self_clone = self.clone();
            tokio::spawn(async move {
                // 连接失败后自动重连（最多重试3次，间隔5秒）
                let mut retries = 0;
                loop {
                    match self_clone.connect_and_run(&host, port).await {
                        Ok(()) => break,  // 正常关闭
                        Err(e) => {
                            retries += 1;
                            if retries >= 3 {
                                warn!("[net-mqtt] broker {}:{} 连续失败3次，停止重试", host, port);
                                break;
                            }
                            warn!("[net-mqtt] broker {}:{} 连接失败({}/3)，5秒后重试: {}", host, port, retries, e);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            });
        }
    }

    /// 连接单个 broker 并运行发现循环
    async fn connect_and_run(&self, host: &str, port: u16) -> anyhow::Result<()> {
        use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};

        // 优先用 DnsPool 解析域名（应对 DNS 污染/封锁环境）
        let connect_host = if let Some(dns) = &self.dns_pool {
            match dns.resolve(host, port).await {
                Ok(addrs) if !addrs.is_empty() => {
                    let ip = addrs[0].ip().to_string();
                    debug!("[net-mqtt] DNS 解析 {} -> {}（使用 DnsPool）", host, ip);
                    ip
                }
                Err(e) => {
                    debug!("[net-mqtt] DnsPool 解析 {} 失败（{}），回退系统 DNS", host, e);
                    host.to_string()
                }
                Ok(_) => host.to_string(),
            }
        } else {
            host.to_string()
        };

        let client_id = format!("pnos-{}", hex::encode(&self.my_node_id[..8]));
        let mut options = MqttOptions::new(client_id, &connect_host, port);
        options.set_keep_alive(Duration::from_secs(60));

        let (client, mut eventloop) = AsyncClient::new(options, 64);

        // 连接 broker（异步，eventloop.poll 会驱动连接）
        info!("[net-mqtt] 正在连接 broker {}:{} ...", host, port);

        // Subscribe
        client.subscribe(MQTT_DISCOVERY_TOPIC, QoS::AtMostOnce).await?;

        // Publish 自己的地址（retained，新节点连上立即收到）
        let msg = MqttDiscoveryMessage::new(
            self.my_node_id,
            self.my_addresses.clone(),
            self.api_port,
            self.federation_port,
        );
        let payload = msg.to_json()?;
        client.publish(MQTT_DISCOVERY_TOPIC, QoS::AtMostOnce, true, payload).await?;

        info!(
            "[net-mqtt] MQTT 发现已启动: broker={}:{}, topic={}, 心跳={}s, 地址={:?}",
            host, port, MQTT_DISCOVERY_TOPIC, self.heartbeat_secs, self.my_addresses
        );

        let my_node_id_hex = hex::encode(self.my_node_id);
        let heartbeat_interval = Duration::from_secs(self.heartbeat_secs);
        let mut heartbeat_timer = tokio::time::interval(heartbeat_interval);
        let mut shutdown_rx = self.shutdown.subscribe();

        loop {
            tokio::select! {
                // MQTT 事件
                event = eventloop.poll() => {
                    match event {
                        Ok(notification) => {
                            self.handle_notification(notification, &my_node_id_hex);
                        }
                        Err(e) => {
                            warn!("[net-mqtt] MQTT 连接错误: {}", e);
                            // 连接断开，返回让上层尝试下一个 broker
                            return Err(anyhow::anyhow!("MQTT 连接断开: {}", e));
                        }
                    }
                }
                // 心跳
                _ = heartbeat_timer.tick() => {
                    let msg = MqttDiscoveryMessage::new(
                        self.my_node_id,
                        self.my_addresses.clone(),
                        self.api_port,
                        self.federation_port,
                    );
                    if let Ok(payload) = msg.to_json() {
                        if let Err(e) = client.publish(MQTT_DISCOVERY_TOPIC, QoS::AtMostOnce, true, payload).await {
                            warn!("[net-mqtt] 心跳 publish 失败: {}", e);
                        } else {
                            debug!("[net-mqtt] 心跳已发送");
                        }
                    }
                }
                // 关闭
                _ = shutdown_rx.recv() => {
                    info!("[net-mqtt] 收到关闭信号，断开连接");
                    let _ = client.disconnect().await;
                    return Ok(());
                }
            }
        }
    }

    /// 处理 MQTT 通知
    fn handle_notification(
        &self,
        notification: rumqttc::Event,
        my_node_id_hex: &str,
    ) {

        if let rumqttc::Event::Incoming(packet) = notification {
            if let rumqttc::Packet::Publish(publish) = packet {
                // 解析消息
                let payload_str = match String::from_utf8(publish.payload.to_vec()) {
                    Ok(s) => s,
                    Err(_) => return,
                };

                match MqttDiscoveryMessage::from_json(&payload_str) {
                    Ok(msg) => {
                        // 过滤自己的消息
                        if msg.node_id == my_node_id_hex {
                            return;
                        }

                        // 过滤过期消息（超过 5 分钟）
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        if now.saturating_sub(msg.timestamp) > 300 {
                            debug!("[net-mqtt] 忽略过期消息 (node_id={})", msg.node_id);
                            return;
                        }

                        let addresses = msg.parsed_addresses();
                        if addresses.is_empty() {
                            return;
                        }

                        info!(
                            "[net-mqtt] 发现节点: node_id={}, 地址={:?}",
                            msg.node_id, addresses
                        );

                        // 发送发现事件
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let node_id = match NodeId::from_hex(&msg.node_id) {
                            Ok(id) => id,
                            Err(_) => return,
                        };
                        let discovered = DiscoveredNode {
                            node_id,
                            addresses: addresses.clone(),
                            reachability: Reachability::Unknown,
                            nat_type: None,
                            source: DiscoverySource::Mqtt,
                            last_seen: now,
                        };
                        let _ = self.discovered_tx.send(discovered);
                    }
                    Err(e) => {
                        debug!("[net-mqtt] 解析消息失败: {}", e);
                    }
                }
            }
        }
    }
}
