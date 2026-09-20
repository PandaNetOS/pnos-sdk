//! 会话注册表
//!
//! 以**对端真实 `node_id`** 为主键，并维护 `addr → session` 反向索引。
//!
//! # 为什么必须两张索引
//!
//! - **`by_peer`（node_id 主键）**：连接的真实身份。握手拿到真实 `node_id` 后
//!   才登记，因此"用临时 ID 反复重拨同一地址"在结构上不可能再产生重复条目。
//! - **`by_addr`（addr → session）**：拨号入口的去重依据。`connect()` 一进来先查该表，
//!   命中即拒绝，**不建 TCP、不握手**——这是消除"重复握手"的关键。
//!
//! # 移除的原子性
//!
//! [`remove`](SessionRegistry::remove) 只在**索引仍指向被删除的会话**时才清理，
//! 因此"旧连接的读循环结束"不会误删"已经替换上来的新连接"，
//! `established - closed == active` 也不会漂移。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::session::session::{Session, SessionId};
use crate::types::NodeId;

#[derive(Default)]
struct Inner {
    /// 对端 node_id → 当前会话
    by_peer: HashMap<NodeId, SessionId>,
    /// 对端地址 → 当前会话（拨号入口去重）
    by_addr: HashMap<SocketAddr, SessionId>,
    /// 会话表
    by_id: HashMap<SessionId, Arc<Session>>,
}

/// 会话注册表
#[derive(Default)]
pub struct SessionRegistry {
    inner: RwLock<Inner>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记一条会话
    ///
    /// 若该 `peer_id` 已有会话，返回 `Err(已有会话)`，调用方应据此走仲裁流程。
    pub fn insert(&self, session: Arc<Session>) -> Result<(), Arc<Session>> {
        let mut g = self.inner.write();
        if let Some(existing_id) = g.by_peer.get(&session.peer_id) {
            if let Some(existing) = g.by_id.get(existing_id) {
                return Err(existing.clone());
            }
        }
        g.by_peer.insert(session.peer_id, session.id);
        g.by_addr.insert(session.addr, session.id);
        g.by_id.insert(session.id, session);
        Ok(())
    }

    /// 移除会话（按 id 精确移除，并清理仍指向它的索引）
    pub fn remove(&self, id: SessionId) -> Option<Arc<Session>> {
        let mut g = self.inner.write();
        let removed = g.by_id.remove(&id)?;

        // 仅当索引仍指向被删会话时才清理——避免误删已替换上来的新会话
        if g.by_peer.get(&removed.peer_id) == Some(&id) {
            g.by_peer.remove(&removed.peer_id);
        }
        if g.by_addr.get(&removed.addr) == Some(&id) {
            g.by_addr.remove(&removed.addr);
        }

        Some(removed)
    }

    /// 带身份校验的移除：只有注册表里恰好是 `expect` 这一条时才移除
    ///
    /// 读循环 / 心跳 / 发送失败路径都应使用该入口，杜绝"旧连接的失败回调
    /// 把新连接摘掉"。
    pub fn remove_if(&self, id: SessionId, expect: &Arc<Session>) -> Option<Arc<Session>> {
        {
            let g = self.inner.read();
            match g.by_id.get(&id) {
                Some(cur) if Arc::ptr_eq(cur, expect) => {}
                _ => return None,
            }
        }
        self.remove(id)
    }

    pub fn get_by_id(&self, id: SessionId) -> Option<Arc<Session>> {
        self.inner.read().by_id.get(&id).cloned()
    }

    pub fn get_by_peer(&self, peer: &NodeId) -> Option<Arc<Session>> {
        let g = self.inner.read();
        let id = g.by_peer.get(peer)?;
        g.by_id.get(id).cloned()
    }

    /// 按对端地址查找会话
    pub fn session_of_addr(&self, addr: &SocketAddr) -> Option<Arc<Session>> {
        let g = self.inner.read();
        let id = g.by_addr.get(addr)?;
        g.by_id.get(id).cloned()
    }

    /// `addr` 是否已有活跃会话（拨号入口去重依据）
    pub fn is_addr_connected(&self, addr: &SocketAddr) -> bool {
        self.inner.read().by_addr.contains_key(addr)
    }

    /// 该地址已被哪个对端占用（用于日志说明"复用的是谁"）
    pub fn peer_of_addr(&self, addr: &SocketAddr) -> Option<NodeId> {
        let g = self.inner.read();
        let id = g.by_addr.get(addr)?;
        g.by_id.get(id).map(|s| s.peer_id)
    }

    pub fn addr_of_peer(&self, peer: &NodeId) -> Option<SocketAddr> {
        let g = self.inner.read();
        let id = g.by_peer.get(peer)?;
        g.by_id.get(id).map(|s| s.addr)
    }

    pub fn contains_peer(&self, peer: &NodeId) -> bool {
        self.inner.read().by_peer.contains_key(peer)
    }

    pub fn all(&self) -> Vec<Arc<Session>> {
        self.inner.read().by_id.values().cloned().collect()
    }

    pub fn peers(&self) -> Vec<NodeId> {
        self.inner.read().by_peer.keys().copied().collect()
    }

    pub fn len(&self) -> usize {
        self.inner.read().by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 清空并返回全部会话（服务关闭时使用）
    pub fn drain(&self) -> Vec<Arc<Session>> {
        let mut g = self.inner.write();
        let all: Vec<Arc<Session>> = g.by_id.drain().map(|(_, v)| v).collect();
        g.by_peer.clear();
        g.by_addr.clear();
        all
    }
}

impl std::fmt::Debug for SessionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.inner.read();
        f.debug_struct("SessionRegistry")
            .field("sessions", &g.by_id.len())
            .field("peers", &g.by_peer.len())
            .field("addrs", &g.by_addr.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::frame::FrameTransport;
    use crate::session::session::Direction;
    use crate::transport::{TcpTransportStream, TransportKind};

    /// 构造一条 loopback 连接以拿到合法的 FrameTransport
    fn dummy_transport() -> Arc<FrameTransport> {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = std_listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let (server, _) = std_listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        client.set_nonblocking(true).unwrap();
        let s = tokio::net::TcpStream::from_std(server).unwrap();
        let _client = tokio::net::TcpStream::from_std(client).unwrap();
        Arc::new(FrameTransport::from_stream(Box::new(
            TcpTransportStream::new(s, TransportKind::Tcp),
        )))
    }

    fn mk_session(peer: u8, addr: &str) -> Arc<Session> {
        Session::new(
            NodeId([peer; 20]),
            addr.parse().unwrap(),
            Direction::Outbound,
            dummy_transport(),
            None,
        )
    }

    #[tokio::test]
    async fn test_insert_and_lookup() {
        let reg = SessionRegistry::new();
        let s = mk_session(1, "10.0.0.1:6885");
        let id = s.id;
        assert!(reg.insert(s.clone()).is_ok());

        assert_eq!(reg.len(), 1);
        assert!(reg.contains_peer(&NodeId([1u8; 20])));
        assert_eq!(reg.get_by_peer(&NodeId([1u8; 20])).unwrap().id, id);
        assert_eq!(reg.get_by_id(id).unwrap().id, id);
        assert_eq!(
            reg.session_of_addr(&"10.0.0.1:6885".parse().unwrap())
                .unwrap()
                .id,
            id
        );
        assert!(reg.is_addr_connected(&"10.0.0.1:6885".parse().unwrap()));
        assert_eq!(
            reg.peer_of_addr(&"10.0.0.1:6885".parse().unwrap()),
            Some(NodeId([1u8; 20]))
        );
        assert_eq!(
            reg.addr_of_peer(&NodeId([1u8; 20])),
            Some("10.0.0.1:6885".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn test_duplicate_peer_rejected() {
        let reg = SessionRegistry::new();
        let a = mk_session(2, "10.0.0.2:6885");
        let b = mk_session(2, "10.0.0.3:6885"); // 同 peer，不同地址
        assert!(reg.insert(a.clone()).is_ok());
        let err = reg.insert(b.clone()).unwrap_err();
        assert_eq!(err.id, a.id, "应返回已存在的那条会话");
        assert_eq!(reg.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_cleans_indexes() {
        let reg = SessionRegistry::new();
        let s = mk_session(3, "10.0.0.4:6885");
        let id = s.id;
        reg.insert(s).unwrap();
        assert!(reg.remove(id).is_some());

        assert_eq!(reg.len(), 0);
        assert!(reg.get_by_id(id).is_none());
        assert!(reg.get_by_peer(&NodeId([3u8; 20])).is_none());
        assert!(!reg.is_addr_connected(&"10.0.0.4:6885".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_stale_removal_does_not_evict_new_session() {
        // 模拟：旧会话被新会话替换后，旧会话的清理不应影响新会话
        let reg = SessionRegistry::new();
        let old = mk_session(4, "10.0.0.5:6885");
        let old_id = old.id;
        reg.insert(old.clone()).unwrap();

        // 强制替换：手工摘掉旧会话，再插入新会话
        reg.remove(old_id);
        let new = mk_session(4, "10.0.0.5:6885");
        let new_id = new.id;
        reg.insert(new.clone()).unwrap();

        // 旧会话的读循环此刻才收尾，调 remove(old_id)：必须什么都不做
        assert!(reg.remove(old_id).is_none());
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.get_by_peer(&NodeId([4u8; 20])).unwrap().id, new_id);
    }

    #[tokio::test]
    async fn test_remove_if_checks_identity() {
        let reg = SessionRegistry::new();
        let s = mk_session(5, "10.0.0.6:6885");
        let id = s.id;
        reg.insert(s.clone()).unwrap();

        // 传入另一条会话做身份校验 → 不匹配，不应移除
        let other = mk_session(6, "10.0.0.7:6885");
        assert!(reg.remove_if(id, &other).is_none());
        assert_eq!(reg.len(), 1);

        // 传入自身 → 移除成功
        assert!(reg.remove_if(id, &s).is_some());
        assert_eq!(reg.len(), 0);
    }

    #[tokio::test]
    async fn test_drain_clears_everything() {
        let reg = SessionRegistry::new();
        for i in 1..=3u8 {
            reg.insert(mk_session(i, &format!("10.0.0.{}:6885", i)))
                .unwrap();
        }
        assert_eq!(reg.len(), 3);
        let all = reg.drain();
        assert_eq!(all.len(), 3);
        assert_eq!(reg.len(), 0);
        assert!(reg.peers().is_empty());
        assert!(!reg.is_addr_connected(&"10.0.0.1:6885".parse().unwrap()));
    }
}
