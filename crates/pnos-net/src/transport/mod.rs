//! 传输层抽象
//!
//! 统一 TCP 和 Iroh 两种传输后端的连接接口。
//! NetAgent 通过 Transport trait 建立连接，不关心底层实现。

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;

use crate::types::{NodeId, Reachability};

pub mod iroh;
pub mod router;
pub mod tcp;

/// 连接方式（用于日志和指标）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Tcp,
    UdpHolePunch,
    TcpRelay,
    IrohDirect,
    IrohHolePunch,
    IrohRelay,
}

impl std::fmt::Display for TransportKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportKind::Tcp => write!(f, "TCP直连"),
            TransportKind::UdpHolePunch => write!(f, "UDP打洞"),
            TransportKind::TcpRelay => write!(f, "TCP中继"),
            TransportKind::IrohDirect => write!(f, "Iroh直连"),
            TransportKind::IrohHolePunch => write!(f, "Iroh打洞"),
            TransportKind::IrohRelay => write!(f, "Iroh中继"),
        }
    }
}

/// 传输连接抽象
pub trait TransportStream:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static
{
    fn peer_addr(&self) -> Option<SocketAddr>;
    fn local_addr(&self) -> Option<SocketAddr>;
    fn kind(&self) -> TransportKind;
}

/// 连接结果
pub struct TransportConnectResult {
    pub stream: Box<dyn TransportStream>,
    pub connected_addr: Option<SocketAddr>,
    pub latency: Duration,
    pub kind: TransportKind,
}

/// 传输后端 trait
#[async_trait]
pub trait Transport: Send + Sync {
    fn kind(&self) -> TransportKind;
    async fn connect(
        &self,
        node_id: NodeId,
        addrs: &[SocketAddr],
        reachability: Reachability,
        nat_type: Option<String>,
    ) -> anyhow::Result<TransportConnectResult>;
    async fn start(&self) -> anyhow::Result<()>;
    async fn stop(&self);
}

// 统一 re-export，外部通过 crate::transport:: 访问
pub use router::{TransportMode, TransportRouter, TransportStats};
pub use tcp::{TcpTransport, TcpTransportStream};
pub use iroh::{IrohIdentity, IrohTransport, IrohTransportConfig};
