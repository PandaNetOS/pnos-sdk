//! 通用连接会话层
//!
//! 本层负责「**谁能连上谁**」的全部基础能力，对使用方（任意 Agent）保持零业务耦合：
//!
//! - 入站受理（accept）与限额
//! - 连接注册表：`node_id` 主键 + `addr → session` 反向索引
//! - 地址级去重、冷却、双边仲裁
//! - 帧编解码与收发（线缆格式由使用方约定，本层只搬运 `kind + payload`）
//! - 心跳保活、空闲回收、候选补齐（**逻辑在此，调度权在调用方**）
//! - 连接生命周期统计（`established - closed == active` 恒等）
//!
//! # 设计约束
//!
//! 1. **零业务语义**：本层不认识任何具体的消息类型、协议版本或身份格式。
//!    握手内容由使用方通过 [`PeerAuthenticator`] 注入；
//!    "该连谁"由使用方通过 [`PeerPolicy`] 注入。
//! 2. **不自跑定时**：本层不 `spawn` 任何 `interval` 循环。所有周期动作
//!    由调用方驱动 [`SessionManager::tick`] 完成。
//!    （`accept` 与 `recv` 是**事件驱动**的长驻任务，而非周期任务。）
//!
//! # 例子
//!
//! ```ignore
//! let sessions = SessionManager::bind(local_id, cfg, dialer, auth, policy).await?;
//! sessions.connect(peer, &addrs, Reachability::Mapped).await?;
//! sessions.send_to(&peer, Frame::new(10, payload)).await?;
//! // 由调用方的调度器周期驱动：
//! scheduler.register("session_tick", { let s = sessions.clone(); move || { let s = s.clone(); async move { s.tick().await } } });
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch, Mutex as TokioMutex};
use tracing::{debug, info, warn};

use crate::net_agent::NetAgent;
use crate::session::session::now_ms;
use crate::transport::TransportStream;
use crate::types::{NodeId, Reachability};

pub mod acceptor;
pub mod auth;
pub mod frame;
pub mod heartbeat;
pub mod policy;
pub mod registry;
pub mod session;
pub mod stats;

pub use auth::{PeerAuthenticator, PeerIdentity};
pub use frame::{
    decode_frame, encode_frame, frame_size_in_buffer, transport_from_stream, Frame, FrameTransport,
    FRAME_HEADER_SIZE, MAX_FRAME_SIZE,
};
pub use policy::{NoPolicy, PeerCandidate, PeerPolicy};
pub use registry::SessionRegistry;
pub use session::{Direction, DisconnectReason, Session, SessionConfig, SessionId, SessionInfo};
pub use stats::{SessionStats, SessionStatsSnapshot};

/// 入站被拒的原因（仅用于事件与日志，不参与协议）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// 会话数已达上限
    MaxSessions,
    /// 握手未通过校验
    Unauthenticated,
    /// 与已有会话重复（仲裁判定保留旧连接）
    Duplicate(SessionId),
    /// 握手过程失败
    HandshakeFailed(String),
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectReason::MaxSessions => write!(f, "会话数已达上限"),
            RejectReason::Unauthenticated => write!(f, "身份校验未通过"),
            RejectReason::Duplicate(id) => write!(f, "重复连接（保留 {}）", id),
            RejectReason::HandshakeFailed(e) => write!(f, "握手失败: {}", e),
        }
    }
}

/// 会话事件（使用方订阅后自行分派业务）
#[derive(Debug, Clone)]
pub enum SessionEvent {
    /// 会话建立
    Connected {
        session: SessionId,
        peer: NodeId,
        addr: SocketAddr,
        direction: Direction,
    },
    /// 会话断开
    Disconnected {
        session: SessionId,
        peer: NodeId,
        reason: DisconnectReason,
    },
    /// 收到业务帧（保活帧已被本层内部消化，不会出现在此）
    Frame {
        session: SessionId,
        peer: NodeId,
        frame: Frame,
    },
    /// 入站被拒
    Rejected {
        addr: SocketAddr,
        reason: RejectReason,
    },
}

// ---------------------------------------------------------------------------
// 拨号抽象
// ---------------------------------------------------------------------------

/// 拨号抽象：把「如何建立一条到对端的字节流」交给实现方
///
/// SDK 自带的 [`NetAgent`] 已实现该 trait（TCP 直连 → UDP 打洞 → 中继）。
/// 使用方也可以提供自定义实现——测试桩、或另一套传输栈。
///
/// 会话层只关心"给我一条可读写的字节流"，不关心它是怎么来的。
#[async_trait::async_trait]
pub trait Dialer: Send + Sync + 'static {
    async fn dial(
        &self,
        peer: NodeId,
        addrs: &[SocketAddr],
        reachability: Reachability,
    ) -> anyhow::Result<Box<dyn TransportStream>>;
}

#[async_trait::async_trait]
impl Dialer for NetAgent {
    async fn dial(
        &self,
        peer: NodeId,
        addrs: &[SocketAddr],
        reachability: Reachability,
    ) -> anyhow::Result<Box<dyn TransportStream>> {
        let result = self.connect_to(peer, addrs, reachability, None).await?;
        Ok(result.connection)
    }
}

/// 会话管理器
pub struct SessionManager {
    pub(crate) local_id: NodeId,
    pub(crate) cfg: SessionConfig,
    pub(crate) dialer: Arc<dyn Dialer>,
    pub(crate) auth: Arc<dyn PeerAuthenticator>,
    pub(crate) policy: Arc<dyn PeerPolicy>,
    pub(crate) registry: SessionRegistry,
    pub(crate) stats: SessionStats,
    pub(crate) events: broadcast::Sender<SessionEvent>,
    /// 地址冷却表：(node_id, addr) → 冷却到期时间（UNIX ms）。
    /// 粒度按身份区分：同地址的不同身份互不影响（批次M #7，v9 遗留 #3）
    pub(crate) cooldown: Mutex<HashMap<(NodeId, SocketAddr), u64>>,
    /// 监听器（由 accept loop 取走）
    pub(crate) listener: TokioMutex<Option<TcpListener>>,
    /// 实际监听地址
    pub(crate) bound_addr: Mutex<Option<SocketAddr>>,
    /// 保活状态
    pub(crate) hb: heartbeat::HeartbeatTracker,
    /// 关闭信号（watch 通道，唤醒不丢失）
    pub(crate) shutdown_tx: watch::Sender<bool>,
    pub(crate) shutting_down: AtomicBool,
}

impl SessionManager {
    /// 创建管理器；若 `cfg.listen_addr` 非空则同时开始受理入站
    pub async fn bind(
        local_id: NodeId,
        cfg: SessionConfig,
        dialer: Arc<dyn Dialer>,
        auth: Arc<dyn PeerAuthenticator>,
        policy: Arc<dyn PeerPolicy>,
    ) -> anyhow::Result<Arc<Self>> {
        let (events, _) = broadcast::channel(1024);
        let (shutdown_tx, _) = watch::channel(false);

        let mgr = Arc::new(Self {
            local_id,
            cfg,
            dialer,
            auth,
            policy,
            registry: SessionRegistry::new(),
            stats: SessionStats::new(),
            events,
            cooldown: Mutex::new(HashMap::new()),
            listener: TokioMutex::new(None),
            bound_addr: Mutex::new(None),
            hb: heartbeat::HeartbeatTracker::default(),
            shutdown_tx,
            shutting_down: AtomicBool::new(false),
        });

        if let Some(addr) = mgr.cfg.listen_addr {
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|e| anyhow::anyhow!("监听失败 {}: {}", addr, e))?;
            let actual = listener.local_addr()?;
            *mgr.bound_addr.lock() = Some(actual);
            *mgr.listener.lock().await = Some(listener);
            info!("[session] 开始受理入站连接于 {}", actual);
            acceptor::spawn_accept_loop(mgr.clone());
        }

        Ok(mgr)
    }

    // ------------------------------------------------------------------
    // 对外查询
    // ------------------------------------------------------------------

    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self.bound_addr.lock()
    }

    pub fn stats(&self) -> SessionStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn sessions(&self) -> Vec<SessionInfo> {
        self.registry.all().iter().map(|s| s.info()).collect()
    }

    pub fn session_ids(&self) -> Vec<SessionId> {
        self.registry.all().iter().map(|s| s.id).collect()
    }

    pub fn has_session(&self, peer: &NodeId) -> bool {
        self.registry.contains_peer(peer)
    }

    pub fn is_addr_connected(&self, addr: &SocketAddr) -> bool {
        self.registry.is_addr_connected(addr)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        self.events.subscribe()
    }

    // ------------------------------------------------------------------
    // 连接
    // ------------------------------------------------------------------

    /// 主动连接对端
    ///
    /// 两级去重（**在建立 TCP 之前**完成）：
    /// 1. `node_id` 已有会话 → 直接复用，返回既有 `SessionId`
    /// 2. 任一候选地址已有会话 → 直接复用（这就是"重复握手"的根治点）
    pub async fn connect(
        self: &Arc<Self>,
        peer: NodeId,
        addrs: &[SocketAddr],
        reachability: Reachability,
    ) -> anyhow::Result<SessionId> {
        if self.shutting_down.load(Ordering::Relaxed) {
            anyhow::bail!("会话管理器正在关闭");
        }

        // ① node_id 级去重
        if let Some(existing) = self.registry.get_by_peer(&peer) {
            self.stats.record_duplicate_discarded();
            debug!("[session] 对端 {} 已有会话 {}，直接复用", peer, existing.id);
            return Ok(existing.id);
        }

        // ② 地址级去重（不建 TCP、不握手）
        for a in addrs {
            if let Some(existing) = self.registry.session_of_addr(a) {
                self.stats.record_duplicate_discarded();
                debug!(
                    "[session] 地址 {} 已被会话 {}（对端 {}）占用，跳过拨号",
                    a, existing.id, existing.peer_id
                );
                return Ok(existing.id);
            }
        }

        // ③ 限额
        if self.registry.len() >= self.cfg.max_sessions {
            anyhow::bail!("会话数已达上限 {}", self.cfg.max_sessions);
        }

        // ④ 冷却过滤
        let usable: Vec<SocketAddr> = addrs
            .iter()
            .copied()
            .filter(|a| !self.in_cooldown(&peer, a))
            .collect();
        if usable.is_empty() {
            anyhow::bail!("候选地址均在冷却期: {:?}", addrs);
        }

        // ⑤ 三级选路拨号（本机回环 > 局域网 > 公网）
        //
        // 首选路径优先拨号；首选失败再尝试被过滤的兜底地址（如局域网失败回退公网）。
        let preferred = crate::types::select_preferred_addrs(&usable);
        let fallback: Vec<SocketAddr> = crate::types::fallback_addrs(&usable)
            .into_iter()
            .filter(|a| !self.in_cooldown(&peer, a))
            .collect();

        let stream = match self.dialer.dial(peer, &preferred, reachability).await {
            Ok(s) => s,
            Err(e) => {
                // 首选失败：标记首选冷却，尝试兜底地址
                self.mark_cooldown_all(peer, &preferred);
                if fallback.is_empty() {
                    return Err(e);
                }
                debug!(
                    "[session] 对端 {} 首选路径 {:?} 失败（{}），尝试兜底 {:?}",
                    peer, preferred, e, fallback
                );
                match self.dialer.dial(peer, &fallback, reachability).await {
                    Ok(s) => s,
                    Err(e2) => {
                        self.mark_cooldown_all(peer, &fallback);
                        return Err(e2);
                    }
                }
            }
        };

        let transport = Arc::new(
            FrameTransport::from_stream(stream)
                .with_stats(self.stats.clone())
                .with_write_timeout(self.cfg.write_timeout)
                .with_retry_config(self.cfg.write_max_retries, self.cfg.write_retry_base_ms),
        );
        let addr = transport.peer_addr().unwrap_or(usable[0]);

        // ⑥ 握手（在注册与接收循环启动之前完成，实现方独占读写）
        let identity = match tokio::time::timeout(
            self.auth.timeout(),
            self.auth.authenticate_outbound(&transport),
        )
        .await
        {
            Ok(Ok(id)) => id,
            Ok(Err(e)) => {
                self.mark_cooldown(peer, addr);
                let _ = transport.close().await;
                return Err(anyhow::anyhow!("握手失败: {}", e));
            }
            Err(_) => {
                self.mark_cooldown(peer, addr);
                let _ = transport.close().await;
                return Err(anyhow::anyhow!("握手超时: {}", peer));
            }
        };

        if !identity.verified {
            self.mark_cooldown(identity.peer_id, addr);
            let _ = transport.close().await;
            anyhow::bail!("对端身份校验未通过: {}", identity.peer_id);
        }

        let session = Session::new(
            identity.peer_id,
            addr,
            Direction::Outbound,
            transport,
            identity.metadata,
        );

        self.activate(session).await
    }

    /// 登记会话：仲裁 → 注册 → 启动接收循环
    pub(crate) async fn activate(
        self: &Arc<Self>,
        session: Arc<Session>,
    ) -> anyhow::Result<SessionId> {
        // 双边仲裁：双方同时拨号时只保留一条，且两端判定一致
        if let Some(existing) = self.registry.get_by_peer(&session.peer_id) {
            let expected = if self.local_id.0 < session.peer_id.0 {
                Direction::Outbound
            } else {
                Direction::Inbound
            };
            let new_wins = session.direction == expected && existing.direction != expected;

            if !new_wins {
                self.stats.record_duplicate_discarded();
                session.mark_closed();
                let _ = session.transport.close().await;
                let _ = self.events.send(SessionEvent::Rejected {
                    addr: session.addr,
                    reason: RejectReason::Duplicate(existing.id),
                });
                return Ok(existing.id);
            }

            // 新连接胜出：**完整断开**旧连接（计数一致，不做静默摘除）
            info!(
                "[session] 对端 {} 的新连接 {} 取代旧连接 {}",
                session.peer_id, session.id, existing.id
            );
            self.disconnect(existing.id, DisconnectReason::Replaced)
                .await;
        }

        let id = session.id;
        let peer = session.peer_id;
        let addr = session.addr;
        let direction = session.direction;

        if let Err(existing) = self.registry.insert(session.clone()) {
            self.stats.record_duplicate_discarded();
            session.mark_closed();
            let _ = session.transport.close().await;
            let _ = self.events.send(SessionEvent::Rejected {
                addr,
                reason: RejectReason::Duplicate(existing.id),
            });
            return Ok(existing.id);
        }

        self.stats.record_established();
        info!(
            "[session] {} 建立 {} 对端={} 地址={}",
            id, direction, peer, addr
        );
        let _ = self.events.send(SessionEvent::Connected {
            session: id,
            peer,
            addr,
            direction,
        });

        acceptor::spawn_recv_loop(self.clone(), session);

        Ok(id)
    }

    /// 断开会话（幂等）：摘表 → 关传输 → 计 closed → 发事件
    pub async fn disconnect(&self, id: SessionId, reason: DisconnectReason) {
        let Some(session) = self.registry.get_by_id(id) else {
            return;
        };
        // 带身份校验：只有注册表里仍是这一条时才摘除
        let Some(removed) = self.registry.remove_if(id, &session) else {
            return;
        };

        removed.mark_closed();
        let _ = removed.transport.close().await;
        self.stats.record_closed();
        debug!(
            "[session] {} 关闭 对端={} 原因={}",
            id, removed.peer_id, reason
        );
        let _ = self.events.send(SessionEvent::Disconnected {
            session: id,
            peer: removed.peer_id,
            reason,
        });
    }

    // ------------------------------------------------------------------
    // 收发
    // ------------------------------------------------------------------

    pub async fn send(self: &Arc<Self>, session: SessionId, frame: Frame) -> anyhow::Result<()> {
        let s = self
            .registry
            .get_by_id(session)
            .ok_or_else(|| anyhow::anyhow!("会话不存在: {}", session))?;
        s.transport.send_frame(frame.kind, &frame.payload).await?;
        s.touch_send();
        Ok(())
    }

    pub async fn send_to(self: &Arc<Self>, peer: &NodeId, frame: Frame) -> anyhow::Result<()> {
        let s = self
            .registry
            .get_by_peer(peer)
            .ok_or_else(|| anyhow::anyhow!("对端无活跃会话: {}", peer))?;
        s.transport.send_frame(frame.kind, &frame.payload).await?;
        s.touch_send();
        Ok(())
    }

    /// 广播给所有会话，返回成功条数
    ///
    /// 单条失败**不在此处摘除会话**——收尾由该会话自己的接收循环负责，
    /// 避免"发送失败误删别人的连接"。
    pub async fn broadcast(self: &Arc<Self>, frame: Frame, exclude: Option<SessionId>) -> usize {
        let all = self.registry.all();
        let mut ok = 0usize;
        for s in all {
            if Some(s.id) == exclude {
                continue;
            }
            match s.transport.send_frame(frame.kind, &frame.payload).await {
                Ok(()) => {
                    s.touch_send();
                    ok += 1;
                }
                Err(e) => {
                    warn!("[session] {} 发送失败（交给接收循环收尾）: {}", s.id, e);
                }
            }
        }
        ok
    }

    pub async fn broadcast_encoded(
        self: &Arc<Self>,
        encoded: &[u8],
        exclude: Option<SessionId>,
    ) -> usize {
        let all = self.registry.all();
        let mut ok = 0usize;
        for s in all {
            if Some(s.id) == exclude {
                continue;
            }
            match s.transport.send_encoded(encoded).await {
                Ok(()) => {
                    s.touch_send();
                    ok += 1;
                }
                Err(e) => {
                    warn!("[session] {} 批量发送失败（交给接收循环收尾）: {}", s.id, e);
                }
            }
        }
        ok
    }

    // ------------------------------------------------------------------
    // 关闭
    // ------------------------------------------------------------------

    pub async fn shutdown(self: &Arc<Self>) {
        if self.shutting_down.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.shutdown_tx.send(true);

        for s in self.registry.drain() {
            s.mark_closed();
            let _ = s.transport.close().await;
            self.stats.record_closed();
            let _ = self.events.send(SessionEvent::Disconnected {
                session: s.id,
                peer: s.peer_id,
                reason: DisconnectReason::Shutdown,
            });
        }
        info!("[session] 已关闭全部会话");
    }

    /// 等待关闭信号（供 accept / 其他长驻任务使用）
    pub(crate) async fn wait_shutdown(&self) {
        let mut rx = self.shutdown_tx.subscribe();
        if *rx.borrow() {
            return;
        }
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Relaxed)
    }

    // ------------------------------------------------------------------
    // 冷却
    // ------------------------------------------------------------------

    pub(crate) fn in_cooldown(&self, peer: &NodeId, addr: &SocketAddr) -> bool {
        let mut g = self.cooldown.lock();
        let key = (*peer, *addr);
        match g.get(&key) {
            Some(until) => {
                if *until <= now_ms() {
                    g.remove(&key);
                    false
                } else {
                    true
                }
            }
            None => false,
        }
    }

    pub(crate) fn mark_cooldown(&self, peer: NodeId, addr: SocketAddr) {
        let until = now_ms() + self.cfg.addr_cooldown.as_millis() as u64;
        self.cooldown.lock().insert((peer, addr), until);
    }

    pub(crate) fn mark_cooldown_all(&self, peer: NodeId, addrs: &[SocketAddr]) {
        for a in addrs {
            self.mark_cooldown(peer, *a);
        }
    }

    /// 清理过期冷却项（由 `tick` 调用，避免表无界增长）
    pub(crate) fn prune_cooldown(&self) {
        let now = now_ms();
        let mut g = self.cooldown.lock();
        g.retain(|_, until| *until > now);
    }
}

impl std::fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionManager")
            .field("local_id", &self.local_id)
            .field("listen", &self.local_addr())
            .field("sessions", &self.registry.len())
            .finish()
    }
}

/// 供调用方在 `TaskScheduler` 中注册的便捷函数
///
/// 使用方注册时直接包一层闭包即可：
/// ```ignore
/// let s = sessions.clone();
/// scheduler.register("session_tick", move || { let s = s.clone(); async move { s.tick().await } });
/// ```
pub async fn tick_once(mgr: Arc<SessionManager>) {
    mgr.tick().await;
}

/// 默认关闭超时
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
