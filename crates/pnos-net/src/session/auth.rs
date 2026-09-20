//! 对端身份与握手抽象
//!
//! 会话层**不定义**握手内容——它只规定"何时、在哪个流上、超时多久"，
//! 具体字节由使用方通过 [`PeerAuthenticator`] 实现。
//!
//! 使用方通常在实现里完成：交换握手帧 → 校验签名/身份绑定/重放窗口 →
//! 返回 [`PeerIdentity`]。

use std::time::Duration;

use async_trait::async_trait;

use crate::session::frame::FrameTransport;
use crate::types::NodeId;

/// 握手默认超时：实现方未覆盖 [`PeerAuthenticator::timeout`] 时生效
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// 握手结果：对端身份
#[derive(Debug, Clone)]
pub struct PeerIdentity {
    /// 对端真实节点 ID（握手校验通过后得到的，而非拨号时猜测的临时 ID）
    pub peer_id: NodeId,
    /// 是否通过校验（签名、身份绑定、重放窗口等）
    ///
    /// `authenticate_*` 返回 `Ok` 但 `verified == false` 时，会话层会以
    /// [`RejectReason::Unauthenticated`] 拒绝该连接。
    ///
    /// [`RejectReason::Unauthenticated`]: crate::session::RejectReason
    pub verified: bool,
    /// 使用方自定义的握手元数据（例如协议版本号）。
    ///
    /// 会话层原样携带至 [`SessionInfo::metadata`]，不做解释——这为
    /// "握手期产生的应用层元数据"提供了正式出口。
    ///
    /// [`SessionInfo::metadata`]: crate::session::SessionInfo::metadata
    pub metadata: Option<Vec<u8>>,
}

impl PeerIdentity {
    pub fn new(peer_id: NodeId, verified: bool) -> Self {
        Self {
            peer_id,
            verified,
            metadata: None,
        }
    }

    pub fn with_metadata(mut self, metadata: Vec<u8>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// 便捷包装：已校验
    pub fn verified(peer_id: NodeId) -> Self {
        Self::new(peer_id, true)
    }
}

/// 握手实现（由使用方提供）
///
/// 实现方拿到的是已经建好的 [`FrameTransport`]，可直接用
/// [`send_frame`](FrameTransport::send_frame) /
/// [`recv_frame`](FrameTransport::recv_frame) 交换握手帧。
///
/// # 时序保证
///
/// 会话层保证：握手在**注册表登记之前、接收循环启动之前**完成。
/// 因此实现方在握手期间独占该连接的读写，不会与业务帧竞争。
#[async_trait]
pub trait PeerAuthenticator: Send + Sync + 'static {
    /// 主动方向（我拨号，由我先发握手）
    async fn authenticate_outbound(&self, io: &FrameTransport) -> anyhow::Result<PeerIdentity>;

    /// 被动方向（对端拨号进来，我先读后发）
    async fn authenticate_inbound(&self, io: &FrameTransport) -> anyhow::Result<PeerIdentity>;

    /// 握手超时（默认 [`DEFAULT_HANDSHAKE_TIMEOUT`]）
    fn timeout(&self) -> Duration {
        DEFAULT_HANDSHAKE_TIMEOUT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_identity_builder() {
        let id = NodeId([7u8; 20]);
        let pi = PeerIdentity::new(id, true).with_metadata(vec![6, 0, 0, 0]);
        assert_eq!(pi.peer_id, id);
        assert!(pi.verified);
        assert_eq!(pi.metadata.as_deref(), Some(&[6u8, 0, 0, 0][..]));

        let pi2 = PeerIdentity::verified(id);
        assert!(pi2.verified);
        assert!(pi2.metadata.is_none());
    }
}
