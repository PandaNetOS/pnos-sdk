//! 会话统计
//!
//! 维护连接生命周期计数，并保证不变量：
//!
//! ```text
//! established - closed == active
//! ```
//!
//! 该不变量在每次生命周期变更后以 `debug_assert!` 自检。若出现漂移，
//! 说明存在"连接被静默摘除"之类的缺陷——这类缺陷曾导致连接表被摘空、
//! 表现为"收得到、发不出"的半死状态。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// 会话统计句柄（克隆共享同一份计数）
#[derive(Clone, Debug, Default)]
pub struct SessionStats {
    inner: Arc<StatsInner>,
}

#[derive(Debug, Default)]
struct StatsInner {
    established: AtomicU64,
    closed: AtomicU64,
    active: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    frames_sent: AtomicU64,
    frames_recv: AtomicU64,
    duplicate_discarded: AtomicU64,
    inbound_rejected: AtomicU64,
}

/// 统计快照（可序列化，供上层 API 输出）
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionStatsSnapshot {
    pub established: u64,
    pub closed: u64,
    pub active: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub frames_sent: u64,
    pub frames_recv: u64,
    /// 地址级去重命中次数（本可发起连接，但目标地址已存在会话）
    pub duplicate_discarded: u64,
    /// 入站被拒次数（限额 / 鉴权失败 / 地址冲突）
    pub inbound_rejected: u64,
}

impl SessionStats {
    pub fn new() -> Self {
        Self::default()
    }

    // ---------- 生命周期 ----------

    /// 记录一次会话建立
    pub fn record_established(&self) {
        self.inner.established.fetch_add(1, Ordering::Relaxed);
        self.inner.active.fetch_add(1, Ordering::Relaxed);
        debug_assert!(
            self.is_consistent(),
            "会话计数漂移：{} - {} != {}",
            self.established(),
            self.closed(),
            self.active()
        );
    }

    /// 记录一次会话关闭
    pub fn record_closed(&self) {
        self.inner.closed.fetch_add(1, Ordering::Relaxed);
        // 饱和递减，避免计数下溢成 u64::MAX
        let _ = self
            .inner
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
        debug_assert!(
            self.is_consistent(),
            "会话计数漂移：{} - {} != {}",
            self.established(),
            self.closed(),
            self.active()
        );
    }

    // ---------- 流量 ----------

    pub fn record_bytes_sent(&self, n: u64) {
        self.inner.bytes_sent.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_bytes_recv(&self, n: u64) {
        self.inner.bytes_recv.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_frame_sent(&self) {
        self.inner.frames_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_frame_recv(&self) {
        self.inner.frames_recv.fetch_add(1, Ordering::Relaxed);
    }

    // ---------- 拒绝类 ----------

    /// 入口地址级去重命中（未建 TCP、未握手）
    pub fn record_duplicate_discarded(&self) {
        self.inner
            .duplicate_discarded
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 入站被拒
    pub fn record_inbound_rejected(&self) {
        self.inner.inbound_rejected.fetch_add(1, Ordering::Relaxed);
    }

    // ---------- 读取 ----------

    pub fn established(&self) -> u64 {
        self.inner.established.load(Ordering::Relaxed)
    }

    pub fn closed(&self) -> u64 {
        self.inner.closed.load(Ordering::Relaxed)
    }

    pub fn active(&self) -> u64 {
        self.inner.active.load(Ordering::Relaxed)
    }

    pub fn bytes_sent(&self) -> u64 {
        self.inner.bytes_sent.load(Ordering::Relaxed)
    }

    pub fn bytes_recv(&self) -> u64 {
        self.inner.bytes_recv.load(Ordering::Relaxed)
    }

    pub fn frames_sent(&self) -> u64 {
        self.inner.frames_sent.load(Ordering::Relaxed)
    }

    pub fn frames_recv(&self) -> u64 {
        self.inner.frames_recv.load(Ordering::Relaxed)
    }

    pub fn duplicate_discarded(&self) -> u64 {
        self.inner.duplicate_discarded.load(Ordering::Relaxed)
    }

    pub fn inbound_rejected(&self) -> u64 {
        self.inner.inbound_rejected.load(Ordering::Relaxed)
    }

    /// 不变量自检：`established - closed == active`
    pub fn is_consistent(&self) -> bool {
        self.established().saturating_sub(self.closed()) == self.active()
    }

    pub fn snapshot(&self) -> SessionStatsSnapshot {
        SessionStatsSnapshot {
            established: self.established(),
            closed: self.closed(),
            active: self.active(),
            bytes_sent: self.bytes_sent(),
            bytes_recv: self.bytes_recv(),
            frames_sent: self.frames_sent(),
            frames_recv: self.frames_recv(),
            duplicate_discarded: self.duplicate_discarded(),
            inbound_rejected: self.inbound_rejected(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lifecycle_invariant_holds() {
        let s = SessionStats::new();
        assert!(s.is_consistent());

        for _ in 0..5 {
            s.record_established();
        }
        assert_eq!(s.established(), 5);
        assert_eq!(s.active(), 5);
        assert!(s.is_consistent());

        for _ in 0..3 {
            s.record_closed();
        }
        assert_eq!(s.closed(), 3);
        assert_eq!(s.active(), 2);
        assert!(s.is_consistent());

        for _ in 0..2 {
            s.record_closed();
        }
        assert_eq!(s.active(), 0);
        assert!(s.is_consistent());
    }

    #[test]
    fn test_close_never_underflows() {
        let s = SessionStats::new();
        // 无建立记录时关闭，active 不得下溢
        s.record_closed();
        assert_eq!(s.active(), 0);
        assert_eq!(s.closed(), 1);
        assert!(s.is_consistent());
    }

    #[test]
    fn test_share_across_clones() {
        let a = SessionStats::new();
        let b = a.clone();
        a.record_established();
        b.record_established();
        assert_eq!(a.established(), 2);
        a.record_bytes_sent(100);
        assert_eq!(b.bytes_sent(), 100);
        assert!(a.is_consistent());
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let s = SessionStats::new();
        s.record_established();
        s.record_bytes_sent(7);
        s.record_frame_recv();
        s.record_duplicate_discarded();
        s.record_inbound_rejected();
        let snap = s.snapshot();
        assert_eq!(snap.established, 1);
        assert_eq!(snap.active, 1);
        assert_eq!(snap.bytes_sent, 7);
        assert_eq!(snap.frames_recv, 1);
        assert_eq!(snap.duplicate_discarded, 1);
        assert_eq!(snap.inbound_rejected, 1);
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"duplicate_discarded\":1"));
    }
}
