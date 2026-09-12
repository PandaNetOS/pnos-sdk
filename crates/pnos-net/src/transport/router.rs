//! 传输路由器
//!
//! 阶段1：仅支持 TcpOnly 模式，行为与现有完全一致。
//! 阶段2：增加 IrohOnly 和 Auto 模式。

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, warn};
use parking_lot::RwLock;

use super::iroh::IrohTransport;
use super::tcp::TcpTransport;
use super::{Transport, TransportConnectResult, TransportKind};
use crate::types::{NodeId, Reachability};

/// 传输模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    TcpOnly,
    IrohOnly,
    Auto,
}

impl Default for TransportMode {
    fn default() -> Self {
        TransportMode::TcpOnly
    }
}

/// 传输统计
#[derive(Debug, Clone, Default)]
pub struct TransportStats {
    pub tcp_attempts: u64,
    pub tcp_success: u64,
    pub iroh_attempts: u64,
    pub iroh_success: u64,
    pub auto_fallback_count: u64,
    pub total_latency_tcp_ms: u64,
    pub total_latency_iroh_ms: u64,
}

/// 传输路由器
pub struct TransportRouter {
    mode: TransportMode,
    tcp: Option<Arc<TcpTransport>>,
    iroh: Option<Arc<IrohTransport>>,
    stats: RwLock<TransportStats>,
}

impl TransportRouter {
    pub fn new(mode: TransportMode) -> Self {
        Self { mode, tcp: None, iroh: None, stats: RwLock::new(TransportStats::default()) }
    }

    pub fn with_tcp(mut self, tcp: TcpTransport) -> Self {
        self.tcp = Some(Arc::new(tcp));
        self
    }

    pub fn with_iroh(mut self, iroh: IrohTransport) -> Self {
        self.iroh = Some(Arc::new(iroh));
        self
    }

    pub fn stats(&self) -> TransportStats {
        self.stats.read().clone()
    }

    async fn connect_with(
        &self,
        transport: &dyn Transport,
        node_id: NodeId,
        addrs: &[SocketAddr],
        reachability: Reachability,
        nat_type: Option<String>,
        is_iroh: bool,
    ) -> anyhow::Result<TransportConnectResult> {
        let result = transport.connect(node_id, addrs, reachability, nat_type).await;
        let mut stats = self.stats.write();
        if is_iroh {
            stats.iroh_attempts += 1;
            if result.is_ok() {
                stats.iroh_success += 1;
                stats.total_latency_iroh_ms += result.as_ref().unwrap().latency.as_millis() as u64;
            }
        } else {
            stats.tcp_attempts += 1;
            if result.is_ok() {
                stats.tcp_success += 1;
                stats.total_latency_tcp_ms += result.as_ref().unwrap().latency.as_millis() as u64;
            }
        }
        result
    }
}

#[async_trait]
impl Transport for TransportRouter {
    fn kind(&self) -> TransportKind {
        TransportKind::Tcp
    }

    async fn start(&self) -> anyhow::Result<()> {
        if let Some(ref tcp) = self.tcp {
            tcp.start().await?;
        }
        if let Some(ref iroh) = self.iroh {
            if let Err(e) = iroh.start().await {
                warn!("[transport-router] Iroh 启动失败，将仅使用 TCP: {}", e);
            }
        }
        Ok(())
    }

    async fn stop(&self) {
        if let Some(ref iroh) = self.iroh {
            iroh.stop().await;
        }
        if let Some(ref tcp) = self.tcp {
            tcp.stop().await;
        }
    }

    async fn connect(
        &self,
        node_id: NodeId,
        addrs: &[SocketAddr],
        reachability: Reachability,
        nat_type: Option<String>,
    ) -> anyhow::Result<TransportConnectResult> {
        match self.mode {
            TransportMode::TcpOnly => {
                let tcp = self.tcp.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("TcpOnly 模式但未配置 TcpTransport"))?;
                self.connect_with(tcp.as_ref(), node_id, addrs, reachability, nat_type, false).await
            }
            TransportMode::IrohOnly => {
                let iroh = self.iroh.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("IrohOnly 模式但未配置 IrohTransport"))?;
                self.connect_with(iroh.as_ref(), node_id, addrs, reachability, nat_type, true).await
            }
            TransportMode::Auto => {
                // 并行尝试 Iroh 和 TCP，哪个先成功用哪个
                let iroh_fut = async {
                    if let Some(ref iroh) = self.iroh {
                        self.connect_with(
                            iroh.as_ref(),
                            node_id,
                            addrs,
                            reachability,
                            nat_type.clone(),
                            true,
                        ).await
                    } else {
                        Err(anyhow::anyhow!("IrohTransport 未配置"))
                    }
                };

                let tcp_fut = async {
                    if let Some(ref tcp) = self.tcp {
                        self.connect_with(
                            tcp.as_ref(),
                            node_id,
                            addrs,
                            reachability,
                            nat_type.clone(),
                            false,
                        ).await
                    } else {
                        Err(anyhow::anyhow!("TcpTransport 未配置"))
                    }
                };

                // race: 先完成的结果 + 标记是哪个
                let (first_result, iroh_first) = tokio::select! {
                    r = iroh_fut => (r, true),
                    r = tcp_fut => (r, false),
                };

                match first_result {
                    Ok(r) => {
                        debug!("[transport-router] {} 连接先成功", if iroh_first { "Iroh" } else { "TCP" });
                        Ok(r)
                    }
                    Err(e) => {
                        // 先完成的失败了，回退到另一个（重新创建 future，因为 select! 已 drop 另一个）
                        if iroh_first {
                            debug!("[transport-router] Iroh 失败({})，回退 TCP", e);
                            if let Some(ref tcp) = self.tcp {
                                self.connect_with(tcp.as_ref(), node_id, addrs, reachability, nat_type, false).await
                            } else {
                                Err(anyhow::anyhow!("TcpTransport 未配置"))
                            }
                        } else {
                            debug!("[transport-router] TCP 失败({})，回退 Iroh", e);
                            if let Some(ref iroh) = self.iroh {
                                self.connect_with(iroh.as_ref(), node_id, addrs, reachability, nat_type, true).await
                            } else {
                                Err(anyhow::anyhow!("IrohTransport 未配置"))
                            }
                        }
                    }
                }
            }
        }
    }
}
