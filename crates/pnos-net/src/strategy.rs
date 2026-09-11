//! 连接策略引擎
//!
//! 根据对端的可达性、NAT 类型、已知地址，自动选择最优连接方式：
//! 1. TCP 直连（公网/映射地址）
//! 2. UDP 打洞（对称 NAT 端口预测）
//! 3. 中继（前两者都失败时）
//!
//! 策略目标：在最短时间内建立连接，尝试一切可能的连接方式。

use std::net::SocketAddr;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::connector::{connect_any, TcpConnectConfig, TcpConnection};
use crate::nat::HolePuncher;
use crate::types::Reachability;

/// 连接策略配置
#[derive(Debug, Clone)]
pub struct ConnectStrategyConfig {
    /// TCP 直连配置
    pub tcp_config: TcpConnectConfig,
    /// 打洞超时（默认 10 秒）
    pub hole_punch_timeout: Duration,
    /// 是否启用打洞（默认 true）
    pub hole_punch_enabled: bool,
    /// 直连失败后是否立即打洞（默认 true，false 则只直连）
    pub fallback_to_hole_punch: bool,
}

impl Default for ConnectStrategyConfig {
    fn default() -> Self {
        Self {
            tcp_config: TcpConnectConfig::default(),
            hole_punch_timeout: Duration::from_secs(10),
            hole_punch_enabled: true,
            fallback_to_hole_punch: true,
        }
    }
}

/// 连接方式（用于日志和指标）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectMethod {
    /// TCP 直连
    TcpDirect,
    /// UDP 打洞后升级 TCP
    UdpHolePunch,
    /// 中继连接
    Relay,
}

impl std::fmt::Display for ConnectMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectMethod::TcpDirect => write!(f, "TCP直连"),
            ConnectMethod::UdpHolePunch => write!(f, "UDP打洞"),
            ConnectMethod::Relay => write!(f, "中继"),
        }
    }
}

/// 连接结果
pub struct ConnectResult {
    /// 已建立的 TCP 连接
    pub connection: TcpConnection,
    /// 实际使用的连接方式
    pub method: ConnectMethod,
    /// 总耗时
    pub total_latency: Duration,
}

/// 连接策略引擎
pub struct ConnectStrategy {
    config: ConnectStrategyConfig,
    /// UDP 打洞器（可选，未配置时跳过打洞）
    hole_puncher: Option<std::sync::Arc<HolePuncher>>,
}

impl ConnectStrategy {
    /// 创建连接策略引擎
    pub fn new(config: ConnectStrategyConfig) -> Self {
        Self {
            config,
            hole_puncher: None,
        }
    }

    /// 注入 UDP 打洞器
    pub fn with_hole_puncher(mut self, hole_puncher: std::sync::Arc<HolePuncher>) -> Self {
        self.hole_puncher = Some(hole_puncher);
        self
    }

    /// 尝试连接到对端
    ///
    /// # 参数
    /// - `addrs`: 对端已知地址列表（公网/内网/IPv6）
    /// - `peer_reachability`: 对端可达性（如果已知）
    /// - `peer_nat_type`: 对端 NAT 类型（如果已知）
    ///
    /// # 策略
    /// 1. 优先 TCP 直连所有已知地址
    /// 2. 直连失败且对端可打洞时，尝试 UDP 打洞
    /// 3. 打洞失败后返回错误（中继需外部中继节点，暂不自动触发）
    pub async fn connect(
        &self,
        addrs: &[SocketAddr],
        peer_reachability: Reachability,
        peer_nat_type: Option<String>,
    ) -> anyhow::Result<ConnectResult> {
        let start = std::time::Instant::now();

        if addrs.is_empty() {
            anyhow::bail!("对端没有已知地址，无法连接");
        }

        // 阶段1：TCP 直连
        info!(
            "[net-strategy] 尝试 TCP 直连，地址数={}, 对端可达性={}, NAT={:?}",
            addrs.len(),
            peer_reachability,
            peer_nat_type,
        );

        match connect_any(addrs, &self.config.tcp_config).await {
            Ok(conn) => {
                info!(
                    "[net-strategy] TCP 直连成功: {} (耗时 {:?})",
                    conn.connected_addr,
                    conn.latency
                );
                return Ok(ConnectResult {
                    connection: conn,
                    method: ConnectMethod::TcpDirect,
                    total_latency: start.elapsed(),
                });
            }
            Err(e) => {
                debug!("[net-strategy] TCP 直连全部失败: {}", e);
            }
        }

        // 阶段2：UDP 打洞（如果启用且对端可能可打洞）
        if self.config.fallback_to_hole_punch && self.config.hole_punch_enabled {
            let can_hole_punch = match peer_reachability {
                Reachability::HolePunchable => true,
                Reachability::OutboundOnly => false, // 对端只能出站，打洞也没用
                Reachability::Unknown => true,       // 未知，尝试一下
                _ => false,                           // 公网/映射应该直连成功，不打洞
            };

            if can_hole_punch {
                if let Some(hole_puncher) = &self.hole_puncher {
                    info!("[net-strategy] TCP 直连失败，尝试 UDP 打洞...");
                    // 打洞逻辑：对每个地址尝试打洞
                    for addr in addrs {
                        match self.try_hole_punch(hole_puncher, *addr).await {
                            Ok(conn) => {
                                info!(
                                    "[net-strategy] UDP 打洞成功: {} (总耗时 {:?})",
                                    conn.connected_addr,
                                    start.elapsed()
                                );
                                return Ok(ConnectResult {
                                    connection: conn,
                                    method: ConnectMethod::UdpHolePunch,
                                    total_latency: start.elapsed(),
                                });
                            }
                            Err(e) => {
                                debug!("[net-strategy] 地址 {} 打洞失败: {}", addr, e);
                            }
                        }
                    }
                    warn!("[net-strategy] 所有地址打洞均失败");
                } else {
                    debug!("[net-strategy] 未配置 HolePuncher，跳过打洞");
                }
            } else {
                debug!(
                    "[net-strategy] 对端可达性={}，不适合打洞",
                    peer_reachability
                );
            }
        }

        // 所有方式都失败
        let elapsed = start.elapsed();
        warn!(
            "[net-strategy] 连接失败（直连+打洞均失败），总耗时 {:?}",
            elapsed
        );
        anyhow::bail!(
            "连接失败：TCP直连和UDP打洞均失败（总耗时 {:?}），对端可能不可达或NAT不兼容",
            elapsed
        );
    }

    /// 尝试对单个地址进行 UDP 打洞，成功后升级为 TCP
    async fn try_hole_punch(
        &self,
        hole_puncher: &std::sync::Arc<HolePuncher>,
        addr: SocketAddr,
    ) -> anyhow::Result<TcpConnection> {
        // UDP 打洞：发送 UDP 包到对端，同时对端也在打洞
        let peer_id = addr.to_string();
        let punch_result = hole_puncher.punch(addr, &peer_id).await;

        if punch_result.success {
            debug!("[net-strategy] UDP 打洞成功，尝试 TCP 连接 {}", addr);
            // 打洞后 NAT 映射已建立，TCP 连接可能成功
            crate::connector::connect_one(addr, Duration::from_secs(5)).await
        } else {
            debug!(
                "[net-strategy] UDP 打洞失败 {}: {:?}",
                addr, punch_result.error
            );
            // 即使打洞失败，也尝试一下 TCP（可能对端是公网）
            crate::connector::connect_one(addr, Duration::from_secs(3)).await
        }
    }
}
