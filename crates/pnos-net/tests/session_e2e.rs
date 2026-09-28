//! 会话层端到端集成测试
//!
//! 用真实 loopback TCP 起两个独立的 [`SessionManager`] 互连，覆盖：
//!
//! 1. 建立 + 双向收发
//! 2. **重复 connect 被拒且不建 TCP**（本轮线上故障的根治点）
//! 3. 断开后重连
//! 4. 优雅关闭
//! 5. `tick` 驱动的候选补齐（不重复建连）
//! 6. `established - closed == active` 恒等

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pnos_net::session::{
    Dialer, Direction, DisconnectReason, Frame, FrameTransport, PeerAuthenticator, PeerCandidate,
    PeerIdentity, PeerPolicy, SessionConfig, SessionEvent, SessionInfo, SessionManager,
};
use pnos_net::transport::{TcpTransportStream, TransportKind, TransportStream};
use pnos_net::types::{NodeId, Reachability};

// ---------------------------------------------------------------------------
// 测试时间参数（集中定义，便于统一调整）
// ---------------------------------------------------------------------------

/// 拨号 / 事件等待超时
const TEST_IO_TIMEOUT: Duration = Duration::from_secs(3);
/// 测试用空闲超时
const TEST_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// 测试用心跳间隔
const TEST_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// 等待「重复 connect 被拒」生效的静默期
const SETTLE_DUPLICATE_REJECT: Duration = Duration::from_millis(150);
/// 轮询等待对端清理掉线会话的间隔
const POLL_PEER_CLEANUP: Duration = Duration::from_millis(25);
/// replenish 后台拨号落地等待时间
const REPLENISH_SETTLE: Duration = Duration::from_millis(100);
/// 双向拨号仲裁收敛的静默期
const SETTLE_BIDIRECTIONAL: Duration = Duration::from_millis(250);
/// 极短空闲超时（空闲回收用例）
const TEST_IDLE_TIMEOUT_SHORT: Duration = Duration::from_millis(120);
/// 极短心跳间隔（空闲回收用例）
const TEST_HEARTBEAT_INTERVAL_SHORT: Duration = Duration::from_millis(60);
/// 静默等待后触发 tick 的时长
const SETTLE_IDLE_REAP: Duration = Duration::from_millis(200);

// ---------------------------------------------------------------------------
// 测试替身
// ---------------------------------------------------------------------------

/// 直连拨号器：按顺序尝试 TCP 连接（等同 NetAgent 的直连分支）
struct DirectDialer;

#[async_trait]
impl Dialer for DirectDialer {
    async fn dial(
        &self,
        _peer: NodeId,
        addrs: &[SocketAddr],
        _reachability: Reachability,
    ) -> anyhow::Result<Box<dyn TransportStream>> {
        let mut last = String::from("无可用地址");
        for a in addrs {
            match tokio::time::timeout(TEST_IO_TIMEOUT, tokio::net::TcpStream::connect(a)).await {
                Ok(Ok(s)) => {
                    let _ = s.set_nodelay(true);
                    return Ok(Box::new(TcpTransportStream::new(s, TransportKind::Tcp)));
                }
                Ok(Err(e)) => last = format!("{}: {}", a, e),
                Err(_) => last = format!("{}: 超时", a),
            }
        }
        anyhow::bail!("拨号失败 - {}", last)
    }
}

/// 极简握手：双方各发一帧 `kind=1` 携带自己的 20 字节 node_id
struct FixedAuth {
    node_id: NodeId,
}

#[async_trait]
impl PeerAuthenticator for FixedAuth {
    async fn authenticate_outbound(&self, io: &FrameTransport) -> anyhow::Result<PeerIdentity> {
        io.send_frame(1, self.node_id.as_bytes()).await?;
        let f = io.recv_frame().await?;
        anyhow::ensure!(f.kind == 1, "握手帧类型错误: {}", f.kind);
        anyhow::ensure!(f.payload.len() == 20, "握手载荷长度错误");
        let mut b = [0u8; 20];
        b.copy_from_slice(&f.payload);
        Ok(PeerIdentity::new(NodeId(b), true))
    }

    async fn authenticate_inbound(&self, io: &FrameTransport) -> anyhow::Result<PeerIdentity> {
        let f = io.recv_frame().await?;
        anyhow::ensure!(f.kind == 1, "握手帧类型错误: {}", f.kind);
        anyhow::ensure!(f.payload.len() == 20, "握手载荷长度错误");
        let mut b = [0u8; 20];
        b.copy_from_slice(&f.payload);
        io.send_frame(1, self.node_id.as_bytes()).await?;
        Ok(PeerIdentity::new(NodeId(b), true))
    }
}

/// 空策略：不做候选补齐
struct EmptyPolicy;

impl PeerPolicy for EmptyPolicy {
    fn candidates(&self) -> Vec<PeerCandidate> {
        Vec::new()
    }
    fn score(&self, _c: &PeerCandidate, _l: Option<&SessionInfo>) -> i64 {
        0
    }
    fn target_sessions(&self) -> usize {
        0
    }
}

/// 静态候选策略：用于验证 tick 补齐
struct StaticPolicy {
    target: usize,
    peers: Vec<PeerCandidate>,
}

impl PeerPolicy for StaticPolicy {
    fn candidates(&self) -> Vec<PeerCandidate> {
        self.peers.clone()
    }
    fn score(&self, c: &PeerCandidate, _l: Option<&SessionInfo>) -> i64 {
        c.weight
    }
    fn target_sessions(&self) -> usize {
        self.target
    }
}

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

fn listen_cfg() -> SessionConfig {
    SessionConfig {
        listen_addr: Some("127.0.0.1:0".parse().unwrap()),
        idle_timeout: TEST_IDLE_TIMEOUT,
        heartbeat_interval: TEST_HEARTBEAT_INTERVAL,
        ..Default::default()
    }
}

fn dial_cfg() -> SessionConfig {
    SessionConfig {
        listen_addr: None,
        idle_timeout: TEST_IDLE_TIMEOUT,
        heartbeat_interval: TEST_HEARTBEAT_INTERVAL,
        ..Default::default()
    }
}

async fn spawn_listener(id: NodeId, cfg: SessionConfig) -> Arc<SessionManager> {
    SessionManager::bind(
        id,
        cfg,
        Arc::new(DirectDialer),
        Arc::new(FixedAuth { node_id: id }),
        Arc::new(EmptyPolicy),
    )
    .await
    .expect("bind 失败")
}

async fn spawn_dialer(
    id: NodeId,
    cfg: SessionConfig,
    policy: Arc<dyn PeerPolicy>,
) -> Arc<SessionManager> {
    SessionManager::bind(
        id,
        cfg,
        Arc::new(DirectDialer),
        Arc::new(FixedAuth { node_id: id }),
        policy,
    )
    .await
    .expect("bind 失败")
}

/// 接收下一条事件（带超时）
async fn next_event(rx: &mut tokio::sync::broadcast::Receiver<SessionEvent>) -> SessionEvent {
    tokio::time::timeout(TEST_IO_TIMEOUT, rx.recv())
        .await
        .expect("等待事件超时")
        .expect("事件通道关闭")
}

fn assert_stats_consistent(m: &SessionManager, who: &str) {
    let s = m.stats();
    assert_eq!(
        s.established - s.closed,
        s.active,
        "{} 的计数漂移: established={} closed={} active={}",
        who,
        s.established,
        s.closed,
        s.active
    );
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_connect_and_exchange() {
    let a_id = NodeId([0xaa; 20]);
    let b_id = NodeId([0xbb; 20]);

    let a = spawn_listener(a_id, listen_cfg()).await;
    let a_addr = a.local_addr().expect("A 未监听");
    let b = spawn_dialer(b_id, dial_cfg(), Arc::new(EmptyPolicy)).await;

    let mut a_ev = a.subscribe();
    let mut b_ev = b.subscribe();

    let sid = b
        .connect(a_id, &[a_addr], Reachability::Unknown)
        .await
        .expect("B 连接 A 失败");

    // A 侧：入站
    match next_event(&mut a_ev).await {
        SessionEvent::Connected {
            peer, direction, ..
        } => {
            assert_eq!(peer, b_id);
            assert_eq!(direction, Direction::Inbound);
        }
        other => panic!("A 期望 Connected，实际 {:?}", other),
    }
    // B 侧：出站
    match next_event(&mut b_ev).await {
        SessionEvent::Connected {
            peer, direction, ..
        } => {
            assert_eq!(peer, a_id);
            assert_eq!(direction, Direction::Outbound);
        }
        other => panic!("B 期望 Connected，实际 {:?}", other),
    }

    // B → A
    b.send(sid, Frame::new(10, b"hello-a".to_vec()))
        .await
        .unwrap();
    match next_event(&mut a_ev).await {
        SessionEvent::Frame { peer, frame, .. } => {
            assert_eq!(peer, b_id);
            assert_eq!(frame.kind, 10);
            assert_eq!(&frame.payload[..], b"hello-a");
        }
        other => panic!("A 期望 Frame，实际 {:?}", other),
    }

    // A → B（按 node_id 直接发，不持有 SessionId）
    a.send_to(&b_id, Frame::new(11, b"hi-b".to_vec()))
        .await
        .unwrap();
    match next_event(&mut b_ev).await {
        SessionEvent::Frame { frame, .. } => {
            assert_eq!(frame.kind, 11);
            assert_eq!(&frame.payload[..], b"hi-b");
        }
        other => panic!("B 期望 Frame，实际 {:?}", other),
    }

    assert_eq!(a.stats().active, 1);
    assert_eq!(b.stats().active, 1);
    assert_stats_consistent(&a, "A");
    assert_stats_consistent(&b, "B");

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn test_e2e_duplicate_connect_is_discarded_without_tcp() {
    let a_id = NodeId([0x01; 20]);
    let b_id = NodeId([0x02; 20]);

    let a = spawn_listener(a_id, listen_cfg()).await;
    let a_addr = a.local_addr().unwrap();
    let b = spawn_dialer(b_id, dial_cfg(), Arc::new(EmptyPolicy)).await;

    let sid1 = b
        .connect(a_id, &[a_addr], Reachability::Unknown)
        .await
        .unwrap();

    // 再次连接同一地址：必须复用，不得新建 TCP、不得重复握手
    let sid2 = b
        .connect(a_id, &[a_addr], Reachability::Unknown)
        .await
        .unwrap();
    let sid3 = b
        .connect(a_id, &[a_addr], Reachability::Unknown)
        .await
        .unwrap();

    assert_eq!(sid1, sid2);
    assert_eq!(sid1, sid3);

    let s = b.stats();
    assert_eq!(s.established, 1, "不得建立第二条会话");
    assert_eq!(s.active, 1);
    assert_eq!(s.duplicate_discarded, 2, "两次重复都应被计数");
    assert!(b.is_addr_connected(&a_addr), "地址索引应命中");
    assert_stats_consistent(&b, "B");

    // A 侧也不应出现第二条会话
    tokio::time::sleep(SETTLE_DUPLICATE_REJECT).await;
    assert_eq!(a.stats().established, 1, "A 不得收到第二次入站");
    assert_eq!(a.stats().active, 1);
    assert_stats_consistent(&a, "A");

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn test_e2e_disconnect_then_reconnect() {
    let a_id = NodeId([0x11; 20]);
    let b_id = NodeId([0x12; 20]);

    let a = spawn_listener(a_id, listen_cfg()).await;
    let a_addr = a.local_addr().unwrap();
    let b = spawn_dialer(b_id, dial_cfg(), Arc::new(EmptyPolicy)).await;

    let sid1 = b
        .connect(a_id, &[a_addr], Reachability::Unknown)
        .await
        .unwrap();
    assert_eq!(b.stats().active, 1);

    // B 主动断开
    b.disconnect(sid1, DisconnectReason::Local).await;
    assert_eq!(b.stats().active, 0);
    assert_eq!(b.stats().closed, 1);
    assert!(b.sessions().is_empty());
    assert_stats_consistent(&b, "B");

    // 等 A 侧感知到连接关闭并清理
    for _ in 0..40 {
        if a.stats().active == 0 {
            break;
        }
        tokio::time::sleep(POLL_PEER_CLEANUP).await;
    }
    assert_eq!(a.stats().active, 0, "A 应已清理掉线会话");
    assert_stats_consistent(&a, "A");

    // 重连
    let sid2 = b
        .connect(a_id, &[a_addr], Reachability::Unknown)
        .await
        .unwrap();
    assert_ne!(sid1, sid2, "应是新会话");
    assert_eq!(b.stats().active, 1);
    assert_eq!(b.stats().established - b.stats().closed, 1);
    assert_stats_consistent(&b, "B");

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn test_e2e_shutdown_is_idempotent() {
    let a_id = NodeId([0x21; 20]);
    let b_id = NodeId([0x22; 20]);

    let a = spawn_listener(a_id, listen_cfg()).await;
    let a_addr = a.local_addr().unwrap();
    let b = spawn_dialer(b_id, dial_cfg(), Arc::new(EmptyPolicy)).await;

    b.connect(a_id, &[a_addr], Reachability::Unknown)
        .await
        .unwrap();
    assert_eq!(b.stats().active, 1);

    b.shutdown().await;
    assert_eq!(b.stats().active, 0);
    assert!(b.sessions().is_empty());
    assert_eq!(b.stats().closed, 1);

    // 幂等：重复关闭不改变计数
    b.shutdown().await;
    assert_eq!(b.stats().closed, 1);
    assert_stats_consistent(&b, "B");

    a.shutdown().await;
}

#[tokio::test]
async fn test_e2e_tick_replenishes_without_duplicating() {
    let a_id = NodeId([0x31; 20]);
    let b_id = NodeId([0x32; 20]);

    let a = spawn_listener(a_id, listen_cfg()).await;
    let a_addr = a.local_addr().unwrap();

    let policy = Arc::new(StaticPolicy {
        target: 1,
        peers: vec![PeerCandidate::new(a_id, vec![a_addr]).with_weight(10)],
    });
    let b = spawn_dialer(b_id, dial_cfg(), policy).await;
    assert_eq!(b.stats().active, 0);

    // 一轮 replenish 应补齐到 1 条（replenish 内部 spawn 后台连接，等待事件落地）
    b.replenish(1).await;
    tokio::time::sleep(REPLENISH_SETTLE).await;
    assert_eq!(b.stats().active, 1, "replenish 应补齐会话");
    assert_eq!(b.stats().established, 1);

    // 再 tick 两轮：不得重复建连（候选已被 node_id 级去重排除）
    b.tick().await;
    b.tick().await;
    assert_eq!(b.stats().established, 1, "tick 不得重复建连");
    assert_eq!(b.stats().active, 1);
    assert_stats_consistent(&b, "B");

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn test_e2e_bidirectional_simultaneous_dial_converges() {
    // 双方同时拨号：双边仲裁后两端都应只保留一条会话，且方向判定一致
    let a_id = NodeId([0x41; 20]);
    let b_id = NodeId([0x42; 20]);

    let a = spawn_listener(a_id, listen_cfg()).await;
    let a_addr = a.local_addr().unwrap();
    let b = spawn_listener(b_id, listen_cfg()).await;
    let b_addr = b.local_addr().unwrap();

    let a_targets = [b_addr];
    let b_targets = [a_addr];
    let (ra, rb) = tokio::join!(
        a.connect(b_id, &a_targets, Reachability::Unknown),
        b.connect(a_id, &b_targets, Reachability::Unknown),
    );

    // 至少有一侧拨号成功（另一侧可能因仲裁复用既有会话）
    assert!(
        ra.is_ok() || rb.is_ok(),
        "双向拨号至少一侧应成功: ra={:?} rb={:?}",
        ra.err(),
        rb.err()
    );

    tokio::time::sleep(SETTLE_BIDIRECTIONAL).await;

    // 两端各自至多一条会话
    assert!(
        a.stats().active <= 1,
        "A 会话数应 <= 1，实际 {}",
        a.stats().active
    );
    assert!(
        b.stats().active <= 1,
        "B 会话数应 <= 1，实际 {}",
        b.stats().active
    );
    assert_stats_consistent(&a, "A");
    assert_stats_consistent(&b, "B");
}

#[tokio::test]
async fn test_e2e_idle_timeout_reaps_session() {
    let a_id = NodeId([0x51; 20]);
    let b_id = NodeId([0x52; 20]);

    // 极短空闲超时，验证 tick 回收
    let mut acfg = listen_cfg();
    acfg.idle_timeout = TEST_IDLE_TIMEOUT_SHORT;
    acfg.heartbeat_interval = TEST_HEARTBEAT_INTERVAL_SHORT;
    let a = spawn_listener(a_id, acfg).await;
    let a_addr = a.local_addr().unwrap();
    let b = spawn_dialer(b_id, dial_cfg(), Arc::new(EmptyPolicy)).await;

    b.connect(a_id, &[a_addr], Reachability::Unknown)
        .await
        .unwrap();
    assert_eq!(a.stats().active, 1);

    // 静默一段时间后 tick，A 应收掉这条空闲会话
    tokio::time::sleep(SETTLE_IDLE_REAP).await;
    a.tick().await;
    assert_eq!(a.stats().active, 0, "空闲会话应被回收");
    assert!(a.stats().closed >= 1);
    assert_stats_consistent(&a, "A");

    b.shutdown().await;
    a.shutdown().await;
}
