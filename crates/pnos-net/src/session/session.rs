//! 会话实体
//!
//! 一个 [`Session`] 表示**一条已经完成握手、正在工作**的连接。
//! 它持有帧传输句柄、对端真实身份、以及保活所需的活性时间戳。
//!
//! 注意：`Session` **不含任何业务字段**——没有 gossip 缓冲、没有协议版本、
//! 没有业务队列。业务状态全部留在使用方。

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::session::frame::FrameTransport;
use crate::types::NodeId;

/// 当前 UNIX 毫秒时间戳
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 会话标识（进程内唯一、单调递增）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SessionId(pub u64);

impl SessionId {
    pub fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}", self.0)
    }
}

/// 连接方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Direction {
    /// 我方主动拨号
    Outbound,
    /// 对端拨号进来
    Inbound,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Outbound => write!(f, "outbound"),
            Direction::Inbound => write!(f, "inbound"),
        }
    }
}

/// 断开原因
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DisconnectReason {
    /// 本地主动断开
    Local,
    /// 对端关闭 / 读循环结束
    Remote,
    /// 空闲超时
    IdleTimeout,
    /// 被更新的会话替换
    Replaced,
    /// 服务关闭
    Shutdown,
    /// IO 错误
    Io(String),
}

impl std::fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DisconnectReason::Local => write!(f, "local"),
            DisconnectReason::Remote => write!(f, "remote"),
            DisconnectReason::IdleTimeout => write!(f, "idle-timeout"),
            DisconnectReason::Replaced => write!(f, "replaced"),
            DisconnectReason::Shutdown => write!(f, "shutdown"),
            DisconnectReason::Io(e) => write!(f, "io: {}", e),
        }
    }
}

/// 一条活跃会话
pub struct Session {
    pub id: SessionId,
    /// 对端**真实**节点 ID（握手校验后得到）
    pub peer_id: NodeId,
    /// 本条连接使用的对端地址
    pub addr: SocketAddr,
    pub direction: Direction,
    pub transport: Arc<FrameTransport>,
    pub connected_at: Instant,
    /// 握手期产生的应用层元数据（会话层原样携带，不解释）
    pub metadata: Option<Vec<u8>>,
    last_recv_ms: AtomicU64,
    last_send_ms: AtomicU64,
    rtt_ms: AtomicU32,
    closed: AtomicBool,
    closed_tx: watch::Sender<bool>,
    closed_rx: watch::Receiver<bool>,
}

impl Session {
    pub fn new(
        peer_id: NodeId,
        addr: SocketAddr,
        direction: Direction,
        transport: Arc<FrameTransport>,
        metadata: Option<Vec<u8>>,
    ) -> Arc<Self> {
        let now = now_ms();
        let (closed_tx, closed_rx) = watch::channel(false);
        Arc::new(Self {
            id: SessionId::next(),
            peer_id,
            addr,
            direction,
            transport,
            connected_at: Instant::now(),
            metadata,
            last_recv_ms: AtomicU64::new(now),
            last_send_ms: AtomicU64::new(now),
            rtt_ms: AtomicU32::new(0),
            closed: AtomicBool::new(false),
            closed_tx,
            closed_rx,
        })
    }

    pub fn touch_recv(&self) {
        self.last_recv_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub fn touch_send(&self) {
        self.last_send_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub fn set_rtt_ms(&self, rtt: u32) {
        self.rtt_ms.store(rtt, Ordering::Relaxed);
    }

    pub fn rtt_ms(&self) -> Option<u32> {
        let v = self.rtt_ms.load(Ordering::Relaxed);
        if v == 0 {
            None
        } else {
            Some(v)
        }
    }

    /// 距上次收到数据经过的毫秒数
    pub fn idle_ms(&self) -> u64 {
        now_ms().saturating_sub(self.last_recv_ms.load(Ordering::Relaxed))
    }

    /// 距上次发送数据经过的毫秒数
    pub fn since_send_ms(&self) -> u64 {
        now_ms().saturating_sub(self.last_send_ms.load(Ordering::Relaxed))
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// 标记关闭并唤醒所有等待者（幂等）
    pub fn mark_closed(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self.closed_tx.send(true);
        }
    }

    /// 等待会话被标记关闭
    ///
    /// 使用 `watch` 通道而非 `Notify`：唤醒不会因注册时序而丢失，
    /// 因此可以安全地在 `select!` 循环中反复调用。
    pub async fn wait_closed(&self) {
        let mut rx = self.closed_rx.clone();
        if *rx.borrow() {
            return;
        }
        // 通道关闭或值变为 true 都视为已关闭
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }

    /// 只读快照
    pub fn info(&self) -> SessionInfo {
        SessionInfo {
            id: self.id.0,
            peer_id: self.peer_id,
            addr: self.addr,
            direction: self.direction,
            uptime_secs: self.connected_at.elapsed().as_secs(),
            idle_ms: self.idle_ms(),
            rtt_ms: self.rtt_ms(),
            metadata: self.metadata.clone(),
        }
    }
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id)
            .field("peer_id", &self.peer_id)
            .field("addr", &self.addr)
            .field("direction", &self.direction)
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// 会话快照（可序列化）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub id: u64,
    pub peer_id: NodeId,
    pub addr: SocketAddr,
    pub direction: Direction,
    pub uptime_secs: u64,
    pub idle_ms: u64,
    pub rtt_ms: Option<u32>,
    pub metadata: Option<Vec<u8>>,
}

/// 会话配置
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// 监听地址（`None` = 不监听入站）
    pub listen_addr: Option<SocketAddr>,
    /// 最大并发会话数
    pub max_sessions: usize,
    /// 心跳探测间隔：超过该间隔未发送任何数据则发送一个探测帧
    pub heartbeat_interval: Duration,
    /// 空闲超时：超过该时长未收到任何数据则断开
    pub idle_timeout: Duration,
    /// 地址冷却期：拨号失败后该地址在此期限内不再尝试
    pub addr_cooldown: Duration,
    /// 保活探测帧的 kind（`None` = 不主动发探测，仅做空闲回收）
    ///
    /// 会话层**不解释**该 kind 的含义，只是周期发送一个空载荷帧。
    pub heartbeat_kind: Option<u16>,
    /// 保活探测的应答帧 kind（`None` = 不自动应答）
    ///
    /// 收到 `heartbeat_kind` 的帧时，会话层自动回一个同 kind 的应答帧，
    /// 并据此计算 RTT。使用方无需处理保活帧。
    pub heartbeat_reply_kind: Option<u16>,
    /// 写入超时
    pub write_timeout: Duration,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            listen_addr: None,
            max_sessions: 64,
            heartbeat_interval: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(60),
            addr_cooldown: Duration::from_secs(30),
            heartbeat_kind: None,
            heartbeat_reply_kind: None,
            write_timeout: Duration::from_secs(30),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::frame::FrameTransport;
    use crate::transport::{TcpTransportStream, TransportKind};

    fn dummy_transport() -> Arc<FrameTransport> {
        // 造一对 loopback 连接，仅用于拿到一个合法的 FrameTransport
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = std_listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let (server, _) = std_listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        client.set_nonblocking(true).unwrap();
        let s = tokio::net::TcpStream::from_std(server).unwrap();
        // client 端只需存在以建立连接，不参与通信
        let _client = tokio::net::TcpStream::from_std(client).unwrap();
        Arc::new(FrameTransport::from_stream(Box::new(
            TcpTransportStream::new(s, TransportKind::Tcp),
        )))
    }

    #[test]
    fn test_session_id_monotonic() {
        let a = SessionId::next();
        let b = SessionId::next();
        assert!(b.0 > a.0);
    }

    #[tokio::test]
    async fn test_session_lifecycle() {
        let sess = Session::new(
            NodeId([3u8; 20]),
            "10.0.0.5:6885".parse().unwrap(),
            Direction::Outbound,
            dummy_transport(),
            Some(vec![6, 0, 0, 0]),
        );
        assert!(!sess.is_closed());
        assert_eq!(sess.rtt_ms(), None);
        sess.set_rtt_ms(12);
        assert_eq!(sess.rtt_ms(), Some(12));

        let info = sess.info();
        assert_eq!(info.peer_id, NodeId([3u8; 20]));
        assert_eq!(info.direction, Direction::Outbound);
        assert_eq!(info.metadata.as_deref(), Some(&[6u8, 0, 0, 0][..]));

        sess.mark_closed();
        assert!(sess.is_closed());
    }

    #[tokio::test]
    async fn test_wait_closed_wakes() {
        let sess = Session::new(
            NodeId([4u8; 20]),
            "10.0.0.6:6885".parse().unwrap(),
            Direction::Inbound,
            dummy_transport(),
            None,
        );
        let s2 = sess.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            s2.mark_closed();
        });
        tokio::time::timeout(Duration::from_secs(2), sess.wait_closed())
            .await
            .expect("应被唤醒");
        assert!(sess.is_closed());
    }

    #[test]
    fn test_default_config_is_sane() {
        let c = SessionConfig::default();
        assert!(c.max_sessions > 0);
        assert!(c.idle_timeout > c.heartbeat_interval);
        assert!(c.heartbeat_kind.is_none(), "默认不主动发保活帧");
    }
}
