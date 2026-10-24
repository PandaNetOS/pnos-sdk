//! 保活与维护
//!
//! 本模块实现 [`SessionManager::tick`]——**幂等、不含内部 sleep**，
//! 由调用方的调度器周期驱动。
//!
//! 每轮 `tick` 做三件事：
//!
//! 1. 空闲回收：超过 `idle_timeout` 未收到任何数据的会话断开
//! 2. 保活探测：超过 `heartbeat_interval` 未发送任何数据的会话发一个探测帧
//! 3. 候选补齐：向 [`PeerPolicy`] 要候选池，按打分排序后补齐会话数
//!
//! 保活帧的 `kind` 由配置给出，本层**不解释其语义**——只是在收到探测帧时
//! 自动回一个应答帧，并据此估算 RTT。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tracing::{debug, info, warn};

use crate::session::frame::Frame;
use crate::session::session::{now_ms, DisconnectReason, Session, SessionId};
use crate::session::SessionManager;

/// 单条会话保活探测的硬超时。
///
/// 取 5s：远大于正常握手/首包 RTT，又远小于「一个坏对端拖死整轮 tick」的量级。
/// 未设超时时最坏 = `write_timeout`（默认 30s，Q批后不再有内联重试）；
/// 串行遍历下 N 个停读对端就是 `N × 30s`，整轮 tick 停摆。
const PROBE_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// 保活状态：记录每个会话最近一次探测的发送时间
#[derive(Default)]
pub(crate) struct HeartbeatTracker {
    probe_sent_ms: Mutex<HashMap<SessionId, u64>>,
}

impl HeartbeatTracker {
    pub(crate) fn mark_probe(&self, id: SessionId) {
        self.probe_sent_ms.lock().insert(id, now_ms());
    }

    /// 取出并清除探测记录（应答到达或会话结束时调用）
    pub(crate) fn take_probe(&self, id: SessionId) -> Option<u64> {
        self.probe_sent_ms.lock().remove(&id)
    }

    pub(crate) fn forget(&self, id: SessionId) {
        self.probe_sent_ms.lock().remove(&id);
    }
}

impl SessionManager {
    /// 一轮心跳维护（仅保活探测 + 空闲回收，不含补链）
    ///
    /// **必须由调用方的调度器驱动**，本层不自行 spawn 定时循环。
    /// 轻量任务：只操作已连接的会话，不发起新连接。
    ///
    /// # 会话之间必须并行，且每个会话都要有超时
    ///
    /// 串行遍历 `registry.all()` 时，只要有一个对端停止读数据（TCP 缓冲填满、
    /// 对端挂起、网络分区），`send_frame_high_priority` 就会卡在 writer 锁 +
    /// `write` 上最长 `write_timeout` 秒（Q批后超时即致命错误返回，但一次仍要等满
    /// 30s）。**后面所有会话的保活与空闲回收全部停摆**——包括那些完全健康的会话。
    ///
    /// 因此这里改为：
    ///
    /// 1. 探测帧对每个会话单独加 [`PROBE_SEND_TIMEOUT`] 超时兜底；
    /// 2. 各会话用独立 task 并发执行，坏对端只拖累自己那一条。
    ///
    /// 用 `tokio::spawn` 而非 `join_all`：调用方（pdc 的 `fed_heartbeat`）在
    /// 本 task 之外还有一层超时包裹，本层只需要保证「单条会话不阻塞其他会话」。
    pub async fn tick(self: &Arc<Self>) {
        if self.is_shutting_down() {
            return;
        }

        self.prune_cooldown();

        let idle_limit = self.cfg.idle_timeout.as_millis() as u64;
        let hb_interval = self.cfg.heartbeat_interval.as_millis() as u64;
        let probe_kind = self.cfg.heartbeat_kind;

        // 先取出快照：registry.all() 内部持读锁，不能跨 await。
        let sessions = self.registry.all();

        for s in sessions {
            let mgr = Arc::clone(self);
            tokio::spawn(async move {
                // 1. 空闲回收
                if s.idle_ms() > idle_limit {
                    info!(
                        "[session] {} 空闲超时（{}ms > {}ms），断开",
                        s.id,
                        s.idle_ms(),
                        idle_limit
                    );
                    mgr.disconnect(s.id, DisconnectReason::IdleTimeout).await;
                    return;
                }

                // 2. 保活探测
                let Some(probe_kind) = probe_kind else {
                    return;
                };
                if s.since_send_ms() < hb_interval {
                    return;
                }
                let sent = tokio::time::timeout(
                    PROBE_SEND_TIMEOUT,
                    s.transport.send_frame_high_priority(probe_kind, &[]),
                )
                .await;
                match sent {
                    Ok(Ok(())) => {
                        s.touch_send();
                        mgr.hb.mark_probe(s.id);
                        debug!(
                            "[session] {} 保活探测已发出 (idle_ms={}, since_send_ms={})",
                            s.id,
                            s.idle_ms(),
                            s.since_send_ms()
                        );
                    }
                    Ok(Err(e)) => {
                        warn!("[session] {} 保活探测失败，断开: {}", s.id, e);
                        mgr.disconnect(s.id, DisconnectReason::Io(e.to_string()))
                            .await;
                    }
                    Err(_elapsed) => {
                        // 超时 = 对端不读（或 writer 锁被长同步消息长期占用）。
                        // 不再等它，直接摘除这条会话，避免拖垮其他会话的 tick。
                        warn!(
                            "[session] {} 保活探测超过 {}ms 未完成，断开（对端可能已停止读取）",
                            s.id,
                            PROBE_SEND_TIMEOUT.as_millis()
                        );
                        mgr.disconnect(s.id, DisconnectReason::Io("保活探测超时".to_string()))
                            .await;
                    }
                }
            });
        }
    }

    /// 按策略补齐会话数（补链任务，可单独调度）
    ///
    /// 与 tick() 分离：本函数会发起新连接，耗时较长（TCP 握手/超时）。
    /// **并行连接**：同时 spawn 多个连接任务，不串行等待。
    /// 每轮最多补 max_per_tick 个连接，避免一次连太多。
    pub async fn replenish(self: &Arc<Self>, max_per_tick: usize) {
        if self.is_shutting_down() {
            return;
        }

        let target = self.policy.target_sessions().min(self.cfg.max_sessions);
        if target == 0 {
            return;
        }
        let current = self.registry.len();
        if current >= target {
            return;
        }

        let need = (target - current).min(max_per_tick);

        let mut scored: Vec<(i64, crate::session::PeerCandidate)> = self
            .policy
            .candidates()
            .into_iter()
            .filter(|c| !c.addrs.is_empty())
            .filter(|c| !self.registry.contains_peer(&c.peer_id))
            .map(|c| {
                let live = self.registry.get_by_peer(&c.peer_id).map(|s| s.info());
                let sc = self.policy.score(&c, live.as_ref());
                (sc, c)
            })
            .collect();

        // 打分降序；同分时按 node_id 升序，保证两端判定一致
        scored.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| a.1.peer_id.0.cmp(&b.1.peer_id.0))
        });

        // 取前 need 个候选，并行连接
        let to_connect: Vec<_> = scored.into_iter().take(need).collect();

        if to_connect.is_empty() {
            return;
        }

        debug!(
            "[session] 补链：准备并行连接 {} 个候选节点（当前 {} / 目标 {}）",
            to_connect.len(),
            current,
            target
        );

        // 并行 spawn 所有连接任务（fire-and-forget：不等它们完成）
        // 补链是后台任务，连接结果由 SessionManager 内部管理
        for (sc, c) in to_connect {
            let mgr = self.clone();
            let peer_id = c.peer_id;
            let addrs = c.addrs;
            let reach = c.reachability;
            tokio::spawn(async move {
                match mgr.connect(peer_id, &addrs, reach).await {
                    Ok(_) => debug!("[session] 候选 {} 已就位（score={}）", peer_id, sc),
                    Err(e) => debug!("[session] 候选 {} 拨号失败: {}", peer_id, e),
                }
            });
        }
    }

    /// 保活帧内部消化：返回 `true` 表示该帧已被本层处理，不应上抛业务
    ///
    /// - 收到探测帧 → 自动回一个应答帧，并估算 RTT
    /// - 收到应答帧 → 估算 RTT
    ///
    /// 两种帧都**不会**出现在 [`SessionEvent::Frame`] 里。
    ///
    /// [`SessionEvent::Frame`]: crate::session::SessionEvent::Frame
    pub(crate) async fn handle_keepalive(&self, session: &Arc<Session>, frame: &Frame) -> bool {
        let is_probe = Some(frame.kind) == self.cfg.heartbeat_kind;
        let is_reply = Some(frame.kind) == self.cfg.heartbeat_reply_kind;
        if !is_probe && !is_reply {
            return false;
        }

        // RTT：若我方正在等待应答，用探测发出到收到回音的时间差估算
        if let Some(sent_ms) = self.hb.take_probe(session.id) {
            let rtt = now_ms().saturating_sub(sent_ms);
            session.set_rtt_ms(rtt.min(u32::MAX as u64) as u32);
            info!("[session] {} 收到保活应答 (rtt={}ms)", session.id, rtt);
        }

        if is_probe {
            info!(
                "[session] {} 收到对端保活探测 (kind={})，准备回 Pong",
                session.id, frame.kind
            );
            if let Some(reply_kind) = self.cfg.heartbeat_reply_kind {
                if reply_kind != frame.kind {
                    if let Err(e) = session
                        .transport
                        .send_frame_high_priority(reply_kind, &[])
                        .await
                    {
                        debug!("[session] {} 保活应答发送失败: {}", session.id, e);
                    } else {
                        session.touch_send();
                    }
                }
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker_mark_take() {
        let t = HeartbeatTracker::default();
        let id = SessionId(1);
        assert_eq!(t.take_probe(id), None, "未记录时应为空");
        t.mark_probe(id);
        assert!(t.take_probe(id).is_some(), "记录后应可取到");
        assert_eq!(t.take_probe(id), None, "取过后应被清除");
        assert_eq!(t.probe_sent_ms.lock().len(), 0);
    }

    #[test]
    fn test_tracker_forget() {
        let t = HeartbeatTracker::default();
        let id = SessionId(2);
        t.mark_probe(id);
        assert_eq!(t.probe_sent_ms.lock().len(), 1);
        t.forget(id);
        assert_eq!(t.probe_sent_ms.lock().len(), 0);
    }
}
