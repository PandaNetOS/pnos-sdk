//! TCP 传输后端
//!
//! 封装现有的 ConnectStrategy（直连→打洞），实现 Transport trait。
//! 阶段1行为与现有完全一致。

use std::net::SocketAddr;
use std::time::Instant;

use async_trait::async_trait;
use tracing::debug;

use super::{Transport, TransportConnectResult, TransportKind, TransportStream};
use crate::strategy::{ConnectMethod, ConnectStrategy};
use crate::types::{NodeId, Reachability};

/// 封装 tokio::net::TcpStream，实现 TransportStream
pub struct TcpTransportStream {
    inner: tokio::net::TcpStream,
    kind: TransportKind,
}

impl TcpTransportStream {
    pub fn new(stream: tokio::net::TcpStream, kind: TransportKind) -> Self {
        Self { inner: stream, kind }
    }
    pub fn into_inner(self) -> tokio::net::TcpStream {
        self.inner
    }
}

impl tokio::io::AsyncRead for TcpTransportStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for TcpTransportStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl TransportStream for TcpTransportStream {
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.inner.peer_addr().ok()
    }
    fn local_addr(&self) -> Option<SocketAddr> {
        self.inner.local_addr().ok()
    }
    fn kind(&self) -> TransportKind {
        self.kind
    }
}

/// TCP 传输后端
pub struct TcpTransport {
    strategy: ConnectStrategy,
}

impl TcpTransport {
    pub fn new(strategy: ConnectStrategy) -> Self {
        Self { strategy }
    }
}

#[async_trait]
impl Transport for TcpTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Tcp
    }

    async fn connect(
        &self,
        _node_id: NodeId,
        addrs: &[SocketAddr],
        reachability: Reachability,
        nat_type: Option<String>,
    ) -> anyhow::Result<TransportConnectResult> {
        let start = Instant::now();
        let result = self.strategy.connect(addrs, reachability, nat_type).await?;
        let kind = match result.method {
            ConnectMethod::TcpDirect => TransportKind::Tcp,
            ConnectMethod::UdpHolePunch => TransportKind::UdpHolePunch,
            ConnectMethod::Relay => TransportKind::TcpRelay,
        };
        let connected_addr = result.connection.peer_addr();
        Ok(TransportConnectResult {
            stream: result.connection,
            connected_addr,
            latency: start.elapsed(),
            kind,
        })
    }

    async fn start(&self) -> anyhow::Result<()> {
        debug!("[tcp-transport] 启动（无状态，无需操作）");
        Ok(())
    }

    async fn stop(&self) {
        debug!("[tcp-transport] 停止（无状态，无需操作）");
    }
}
