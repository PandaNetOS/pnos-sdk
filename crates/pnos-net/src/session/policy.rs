//! 候选策略抽象
//!
//! 会话层负责"怎么连"（去重、冷却、仲裁、拨号），但"该连谁"由使用方决定。
//! 使用方通过 [`PeerPolicy`] 提供候选池与打分函数：
//!
//! - SDK 每轮 [`tick`](crate::session::SessionManager::tick) 调用 [`PeerPolicy::candidates`]
//!   获取候选池，按 [`PeerPolicy::score`] 降序排序，补齐到
//!   [`PeerPolicy::target_sessions`] 指定的连接数；
//! - 打分所需的实时信息（是否已连接、RTT）由 SDK 提供，使用方**不持有连接状态**。
//!
//! 这样，"多久挑一次 / 挑几个 / 挑完怎么拨"全部收敛在 SDK 内部，
//! 使用方只留一个纯函数——从根本上消除"每轮用临时 ID 重拨同一地址"这类缺陷。

use std::net::SocketAddr;

use crate::session::session::SessionInfo;
use crate::types::{NodeId, Reachability};

/// 候选对端（由使用方提供）
#[derive(Debug, Clone)]
pub struct PeerCandidate {
    pub peer_id: NodeId,
    /// 已知地址列表（SDK 会按顺序尝试，并在拨号前做地址级去重）
    pub addrs: Vec<SocketAddr>,
    pub reachability: Reachability,
    pub nat_type: Option<String>,
    /// 使用方给出的静态权重（SDK 不解释，仅参与排序）
    pub weight: i64,
}

impl PeerCandidate {
    pub fn new(peer_id: NodeId, addrs: Vec<SocketAddr>) -> Self {
        Self {
            peer_id,
            addrs,
            reachability: Reachability::Unknown,
            nat_type: None,
            weight: 0,
        }
    }

    pub fn with_reachability(mut self, r: Reachability) -> Self {
        self.reachability = r;
        self
    }

    pub fn with_nat_type(mut self, n: Option<String>) -> Self {
        self.nat_type = n;
        self
    }

    pub fn with_weight(mut self, w: i64) -> Self {
        self.weight = w;
        self
    }
}

/// 候选与打分策略（由使用方提供）
pub trait PeerPolicy: Send + Sync + 'static {
    /// 当前候选池
    ///
    /// 每轮维护都会被调用，实现方应保证开销可控（例如读一个内存快照）。
    fn candidates(&self) -> Vec<PeerCandidate>;

    /// 打分：**越大越优先**
    ///
    /// `live` 为该对端当前会话信息；未连接时为 `None`。
    fn score(&self, candidate: &PeerCandidate, live: Option<&SessionInfo>) -> i64;

    /// 期望维持的会话数（tick 补齐到此数量）
    fn target_sessions(&self) -> usize {
        8
    }
}

/// 什么都不做的策略（用于仅需手动 `connect()` 的场景）
#[derive(Debug, Default)]
pub struct NoPolicy;

impl PeerPolicy for NoPolicy {
    fn candidates(&self) -> Vec<PeerCandidate> {
        Vec::new()
    }

    fn score(&self, _candidate: &PeerCandidate, _live: Option<&SessionInfo>) -> i64 {
        0
    }

    fn target_sessions(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candidate_builder() {
        let c = PeerCandidate::new(NodeId([1u8; 20]), vec!["10.0.0.1:6885".parse().unwrap()])
            .with_reachability(Reachability::Mapped)
            .with_nat_type(Some("FullCone".into()))
            .with_weight(42);
        assert_eq!(c.addrs.len(), 1);
        assert_eq!(c.weight, 42);
        assert_eq!(c.reachability, Reachability::Mapped);
    }

    #[test]
    fn test_no_policy_is_inert() {
        let p = NoPolicy;
        assert!(p.candidates().is_empty());
        assert_eq!(p.target_sessions(), 0);
        assert_eq!(
            p.score(&PeerCandidate::new(NodeId([0u8; 20]), vec![]), None),
            0
        );
    }
}
