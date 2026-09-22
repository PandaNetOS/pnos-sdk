//! Iroh 传输后端
//!
//! 基于 Iroh 1.x 的 QUIC 传输，支持：
//! - Dial by NodeId（不需要知道 IP）
//! - QNT NAT 穿透
//! - DERP 中继兜底
//! - QUIC multipath（内外网同时连）
//! - Noise 加密

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::{debug, info};

use super::{Transport, TransportConnectResult, TransportKind, TransportStream};
use crate::types::{NodeId, Reachability};

/// Iroh 传输配置
#[derive(Debug, Clone)]
pub struct IrohTransportConfig {
    /// 本机节点 ID（20字节，用于派生 Iroh NodeId）
    pub node_id: [u8; 20],
    /// 监听端口（UDP，QUIC 基于 UDP）
    pub listen_port: u16,
    /// 数据目录（Iroh 持久化节点身份）
    pub data_dir: std::path::PathBuf,
    /// 是否启用 DERP 中继（默认 true）
    pub derp_enabled: bool,
    /// 自定义 DERP 服务器列表（空则用官方）
    pub derp_urls: Vec<String>,
    /// 连接超时（默认 10 秒）
    pub connect_timeout: Duration,
    /// ALPN 协议标识
    pub alpn: Vec<u8>,
    /// DNS 解析配置
    ///
    /// 默认使用 pnos-net 内置的 5 个公共 DNS，**不读宿主系统 DNS 配置**。
    /// 注入后会覆盖 N0 preset 的全部四条解析路径（pkarr 发布/解析、
    /// DnsAddressLookup TXT 查询、DERP 中继主机名）。
    pub dns: crate::dns::DnsConfig,
}

impl Default for IrohTransportConfig {
    fn default() -> Self {
        Self {
            node_id: [0u8; 20],
            listen_port: 6885,
            data_dir: std::path::PathBuf::from("./data"),
            derp_enabled: true,
            derp_urls: vec![],
            connect_timeout: Duration::from_secs(10),
            alpn: b"pnos/federation/1".to_vec(),
            dns: crate::dns::DnsConfig::default(),
        }
    }
}

/// Iroh 节点身份映射
///
/// pnos-net 的 NodeId 是 20 字节，Iroh 的 PublicKey 是 Ed25519 公钥（32字节）。
/// 使用 HKDF 从 pnos NodeId 派生确定性的 Ed25519 密钥对。
pub struct IrohIdentity {
    /// pnos 20字节 NodeId
    pub pnos_node_id: [u8; 20],
    /// Iroh 端点
    pub endpoint: iroh::Endpoint,
    /// 派生的 Iroh 公钥（32字节）
    pub iroh_node_id: iroh::PublicKey,
}

impl IrohIdentity {
    /// 从 pnos NodeId 派生 Iroh 身份
    ///
    /// `dns` 决定 iroh 端点的 DNS 解析器：默认只走 pnos-net 内置公共 DNS，
    /// 不读宿主系统 DNS 配置（系统 DNS 整台不可用时会导致端点发现全线瘫痪）。
    pub async fn from_pnos_node_id(
        node_id: [u8; 20],
        data_dir: &std::path::Path,
        alpn: &[u8],
        dns: &crate::dns::DnsConfig,
    ) -> anyhow::Result<Self> {
        // 尝试从磁盘加载已有的密钥对
        let key_path = data_dir.join("iroh_identity");
        let secret_key = if key_path.exists() {
            let bytes = std::fs::read(&key_path)?;
            let key_array: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("Iroh 密钥文件长度错误，期望32字节"))?;
            iroh::SecretKey::from_bytes(&key_array)
        } else {
            // 从 pnos NodeId 派生新密钥（HKDF-SHA256）
            let seed = Self::derive_seed(&node_id);
            let sk = iroh::SecretKey::from_bytes(&seed);
            // 持久化
            std::fs::create_dir_all(data_dir)?;
            std::fs::write(&key_path, sk.to_bytes())?;
            sk
        };

        let iroh_node_id = secret_key.public();

        // DNS 解析器：一处注入覆盖 N0 preset 的四条解析路径
        //   ① pkarr 发布（HTTPS → dns.iroh.link）
        //   ② pkarr 解析（HTTPS → dns.iroh.link）
        //   ③ DnsAddressLookup（_iroh.<id>.dns.iroh.link 的 TXT 查询）
        //   ④ DERP 中继主机名
        // 不注入则 iroh 默认 `DnsResolver::new()` 会读宿主系统 DNS 配置。
        //
        // 这里直接从 `DnsConfig` 构造解析器，**不**建 `DnsPool`：本函数是
        // `async fn`，若在函数体内持有池，池会在异步上下文里析构
        // （hickory 同步 Resolver 内含 Runtime → 析构即 panic）。
        let server_list: Vec<String> = dns.parse_servers().iter().map(|a| a.to_string()).collect();
        info!(
            "[iroh-transport] DNS 解析器已注入（不读系统 DNS）: servers={:?}, 系统回退={}",
            server_list, dns.allow_system_fallback
        );
        let dns_resolver = dns.to_iroh_resolver();

        // 创建 Iroh Endpoint（N0 preset = 标准 QUIC 配置）
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
            .secret_key(secret_key)
            .alpns(vec![alpn.to_vec()])
            .dns_resolver(dns_resolver)
            .bind()
            .await
            .map_err(|e| anyhow::anyhow!("Iroh Endpoint 绑定失败: {}", e))?;

        Ok(Self {
            pnos_node_id: node_id,
            endpoint,
            iroh_node_id,
        })
    }

    /// HKDF 派生 32 字节种子
    fn derive_seed(node_id: &[u8; 20]) -> [u8; 32] {
        use hkdf::Hkdf;
        use sha2::Sha256;
        let hk = Hkdf::<Sha256>::new(Some(b"pnos-iroh-identity-v1"), node_id);
        let mut seed = [0u8; 32];
        hk.expand(b"ed25519-seed", &mut seed)
            .expect("HKDF expand 失败");
        seed
    }

    /// pnos NodeId → Iroh PublicKey 转换（对端拨号用）
    pub fn pnos_to_iroh_node_id(node_id: &NodeId) -> anyhow::Result<iroh::PublicKey> {
        let seed = Self::derive_seed(&node_id.0);
        let secret = iroh::SecretKey::from_bytes(&seed);
        Ok(secret.public())
    }
}

/// Iroh 连接流（封装 bi-stream）
///
/// 注意：noq 的 RecvStream/SendStream 有 inherent poll_read/poll_write 方法，
/// 会遮蔽 tokio trait 方法，因此必须用 fully qualified syntax 调用 trait 方法。
pub struct IrohStream {
    recv: iroh::endpoint::RecvStream,
    send: iroh::endpoint::SendStream,
    kind: TransportKind,
}

impl AsyncRead for IrohStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        <iroh::endpoint::RecvStream as tokio::io::AsyncRead>::poll_read(
            Pin::new(&mut self.recv),
            cx,
            buf,
        )
    }
}

impl AsyncWrite for IrohStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        <iroh::endpoint::SendStream as tokio::io::AsyncWrite>::poll_write(
            Pin::new(&mut self.send),
            cx,
            buf,
        )
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        <iroh::endpoint::SendStream as tokio::io::AsyncWrite>::poll_flush(
            Pin::new(&mut self.send),
            cx,
        )
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        <iroh::endpoint::SendStream as tokio::io::AsyncWrite>::poll_shutdown(
            Pin::new(&mut self.send),
            cx,
        )
    }
}

impl TransportStream for IrohStream {
    fn peer_addr(&self) -> Option<SocketAddr> {
        // Iroh 连接可能是 multipath，对端地址不唯一
        None
    }
    fn local_addr(&self) -> Option<SocketAddr> {
        None
    }
    fn kind(&self) -> TransportKind {
        self.kind
    }
}

/// Iroh 传输后端
pub struct IrohTransport {
    config: IrohTransportConfig,
    identity: RwLock<Option<Arc<IrohIdentity>>>,
}

impl IrohTransport {
    pub fn new(config: IrohTransportConfig) -> Self {
        Self {
            config,
            identity: RwLock::new(None),
        }
    }

    fn endpoint(&self) -> anyhow::Result<Arc<IrohIdentity>> {
        self.identity
            .read()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("IrohTransport 未启动"))
    }
}

#[async_trait]
impl Transport for IrohTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::IrohDirect
    }

    async fn start(&self) -> anyhow::Result<()> {
        let identity = IrohIdentity::from_pnos_node_id(
            self.config.node_id,
            &self.config.data_dir,
            &self.config.alpn,
            &self.config.dns,
        )
        .await?;
        info!(
            "[iroh-transport] Iroh 端点已启动, pnos_node_id={}, iroh_node_id={}",
            hex::encode(self.config.node_id),
            identity.iroh_node_id,
        );
        *self.identity.write() = Some(Arc::new(identity));
        Ok(())
    }

    async fn stop(&self) {
        if self.identity.write().take().is_some() {
            info!("[iroh-transport] Iroh 端点关闭");
        }
    }

    async fn connect(
        &self,
        node_id: NodeId,
        addrs: &[SocketAddr],
        _reachability: Reachability,
        _nat_type: Option<String>,
    ) -> anyhow::Result<TransportConnectResult> {
        let identity = self.endpoint()?;
        let start = Instant::now();

        // 1. 从 pnos NodeId 派生 Iroh PublicKey
        let iroh_node_id = IrohIdentity::pnos_to_iroh_node_id(&node_id)?;

        // 2. 构建 EndpointAddr：PublicKey + 已知 IP 地址
        let mut endpoint_addr = iroh::EndpointAddr::new(iroh_node_id);
        for addr in addrs {
            endpoint_addr = endpoint_addr.with_ip_addr(*addr);
        }

        // 3. 连接（Iroh 内部自动尝试直连/打洞/中继）
        let connection = tokio::time::timeout(
            self.config.connect_timeout,
            identity.endpoint.connect(endpoint_addr, &self.config.alpn),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Iroh 连接超时"))?
        .map_err(|e| anyhow::anyhow!("Iroh 连接失败: {}", e))?;

        // 4. 打开 bi-stream
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("Iroh 打开流失败: {}", e))?;

        let latency = start.elapsed();
        let kind = TransportKind::IrohDirect;

        debug!(
            "[iroh-transport] 连接成功: node_id={}, kind={}, latency={:?}",
            node_id, kind, latency
        );

        Ok(TransportConnectResult {
            stream: Box::new(IrohStream { recv, send, kind }),
            connected_addr: addrs.first().copied(),
            latency,
            kind,
        })
    }
}
