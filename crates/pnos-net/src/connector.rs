//! TCP 连接器
//!
//! 负责建立到对端的原始 TCP 连接，支持多地址尝试、连接超时、快速失败。
//! 不处理应用层协议（握手/编解码），只返回裸 TcpStream。

use std::net::SocketAddr;
use std::time::Duration;

use tracing::{debug, trace, warn};

/// TCP 连接配置
#[derive(Debug, Clone)]
pub struct TcpConnectConfig {
    /// 单地址连接超时（默认 5 秒）
    pub connect_timeout: Duration,
    /// 多地址之间的尝试间隔（默认 0，立即尝试下一个）
    pub retry_interval: Duration,
    /// 最大重试次数（默认 2，即总共尝试 3 次）
    pub max_retries: u32,
}

impl Default for TcpConnectConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            retry_interval: Duration::from_millis(0),
            max_retries: 2,
        }
    }
}

/// TCP 连接结果
pub struct TcpConnection {
    /// 已建立的 TCP 流
    pub stream: tokio::net::TcpStream,
    /// 实际连接成功的地址
    pub connected_addr: SocketAddr,
    /// 连接耗时
    pub latency: Duration,
}

impl TcpConnection {
    /// 获取对端地址
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.stream.peer_addr().ok()
    }

    /// 获取本地地址
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.stream.local_addr().ok()
    }
}

/// 尝试连接到多个地址，返回第一个成功的连接
///
/// 按顺序尝试每个地址，每个地址最多重试 max_retries 次。
/// 所有地址都失败时返回最后一个错误。
pub async fn connect_any(
    addrs: &[SocketAddr],
    config: &TcpConnectConfig,
) -> anyhow::Result<TcpConnection> {
    if addrs.is_empty() {
        anyhow::bail!("没有可连接的地址");
    }

    let mut last_error: Option<anyhow::Error> = None;

    for addr in addrs {
        for attempt in 0..=config.max_retries {
            let start = std::time::Instant::now();
            match tokio::time::timeout(config.connect_timeout, async {
                tokio::net::TcpStream::connect(addr).await
            })
            .await
            {
                Ok(Ok(stream)) => {
                    let latency = start.elapsed();
                    trace!("[net-connector] TCP 连接成功: {} (耗时 {:?}, 第{}次尝试)", addr, latency, attempt + 1);
                    // 设置 TCP_NODELAY 降低延迟
                    let _ = stream.set_nodelay(true);
                    return Ok(TcpConnection {
                        stream,
                        connected_addr: *addr,
                        latency,
                    });
                }
                Ok(Err(e)) => {
                    debug!("[net-connector] TCP 连接 {} 失败(第{}次): {}", addr, attempt + 1, e);
                    last_error = Some(anyhow::anyhow!("连接 {} 失败: {}", addr, e));
                }
                Err(_) => {
                    debug!("[net-connector] TCP 连接 {} 超时(第{}次)", addr, attempt + 1);
                    last_error = Some(anyhow::anyhow!("连接 {} 超时({:?})", addr, config.connect_timeout));
                }
            }

            if attempt < config.max_retries && !config.retry_interval.is_zero() {
                tokio::time::sleep(config.retry_interval).await;
            }
        }
    }

    warn!("[net-connector] 所有 {} 个地址连接均失败", addrs.len());
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("连接失败")))
}

/// 尝试连接单个地址
pub async fn connect_one(addr: SocketAddr, timeout: Duration) -> anyhow::Result<TcpConnection> {
    let start = std::time::Instant::now();
    let stream = tokio::time::timeout(timeout, async {
        tokio::net::TcpStream::connect(addr).await
    })
    .await
    .map_err(|_| anyhow::anyhow!("连接 {} 超时({:?})", addr, timeout))?
    .map_err(|e| anyhow::anyhow!("连接 {} 失败: {}", addr, e))?;

    let _ = stream.set_nodelay(true);
    Ok(TcpConnection {
        stream,
        connected_addr: addr,
        latency: start.elapsed(),
    })
}
