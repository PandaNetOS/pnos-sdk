//! PDC 超级 Tracker UDP 压测场景
//!
//! 实现 BEP 15 UDP Tracker 协议。
//! 优化：Socket 池复用 + std Mutex（无 AsyncMutex）+ 预生成随机数。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::net::UdpSocket;
use tracing::debug;

use crate::scenario::{BenchContext, Scenario};

/// BEP 15 协议常量
const PROTOCOL_ID: u64 = 0x41727101980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_SCRAPE: u32 = 2;

/// UDP Tracker 压测场景
pub struct UdpTrackerScenario {
    /// 连接池
    connections: Mutex<Vec<u64>>,
    /// Socket 池（setup 后不变）
    socket_pool: OnceLock<Arc<Vec<Arc<UdpSocket>>>>,
    /// Socket 轮转索引
    socket_idx: AtomicUsize,
    /// infohash 池
    infohashes: Vec<[u8; 20]>,
    /// peer_id 池
    peer_ids: Vec<[u8; 20]>,
    /// 预生成随机数池
    tx_ids: Vec<u32>,
    keys: Vec<u32>,
    tx_idx: AtomicUsize,
    key_idx: AtomicUsize,
    /// 目标地址
    target: OnceLock<SocketAddr>,
}

impl Default for UdpTrackerScenario {
    fn default() -> Self {
        let mut rng = StdRng::from_entropy();
        let infohashes: Vec<[u8; 20]> = (0..1000).map(|_| { let mut h = [0u8; 20]; rng.fill(&mut h); h }).collect();
        let peer_ids: Vec<[u8; 20]> = (0..1000).map(|_| { let mut h = [0u8; 20]; rng.fill(&mut h); h }).collect();
        let tx_ids: Vec<u32> = (0..65536).map(|_| rng.gen()).collect();
        let keys: Vec<u32> = (0..65536).map(|_| rng.gen()).collect();

        Self {
            connections: Mutex::new(Vec::new()),
            socket_pool: OnceLock::new(),
            socket_idx: AtomicUsize::new(0),
            infohashes,
            peer_ids,
            tx_ids,
            keys,
            tx_idx: AtomicUsize::new(0),
            key_idx: AtomicUsize::new(0),
            target: OnceLock::new(),
        }
    }
}

impl UdpTrackerScenario {
    fn next_tx_id(&self) -> u32 {
        self.tx_ids[self.tx_idx.fetch_add(1, Ordering::Relaxed) % self.tx_ids.len()]
    }

    fn next_key(&self) -> u32 {
        self.keys[self.key_idx.fetch_add(1, Ordering::Relaxed) % self.keys.len()]
    }

    /// 发送 connect 请求
    async fn udp_connect(socket: &UdpSocket, target: SocketAddr, tx_id: u32) -> anyhow::Result<u64> {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
        buf[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
        buf[12..16].copy_from_slice(&tx_id.to_be_bytes());
        socket.send_to(&buf, target).await?;

        let mut recv_buf = [0u8; 16];
        let (len, _) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut recv_buf)).await??;
        if len < 16 { return Err(anyhow::anyhow!("connect 响应太短")); }
        Ok(u64::from_be_bytes(recv_buf[8..16].try_into().unwrap()))
    }

    /// 发送 announce 请求
    async fn udp_announce(
        socket: &UdpSocket,
        target: SocketAddr,
        connection_id: u64,
        info_hash: &[u8; 20],
        peer_id: &[u8; 20],
        port: u16,
        tx_id: u32,
        key: u32,
    ) -> anyhow::Result<()> {
        let mut buf = [0u8; 98];
        buf[0..8].copy_from_slice(&connection_id.to_be_bytes());
        buf[8..12].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        buf[12..16].copy_from_slice(&tx_id.to_be_bytes());
        buf[16..36].copy_from_slice(info_hash);
        buf[36..56].copy_from_slice(peer_id);
        buf[64..72].copy_from_slice(&1000u64.to_be_bytes());
        buf[88..92].copy_from_slice(&key.to_be_bytes());
        buf[92..96].copy_from_slice(&(-1i32).to_be_bytes());
        buf[96..98].copy_from_slice(&port.to_be_bytes());

        socket.send_to(&buf, target).await?;

        let mut recv_buf = [0u8; 1024];
        let (len, _) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut recv_buf)).await??;
        if len < 20 { return Err(anyhow::anyhow!("announce 响应太短: {}", len)); }
        let action = u32::from_be_bytes(recv_buf[0..4].try_into().unwrap());
        if action != ACTION_ANNOUNCE { return Err(anyhow::anyhow!("announce action 不匹配: {}", action)); }
        Ok(())
    }

    /// 发送 scrape 请求
    async fn udp_scrape(
        socket: &UdpSocket,
        target: SocketAddr,
        connection_id: u64,
        info_hashes: &[[u8; 20]],
        tx_id: u32,
    ) -> anyhow::Result<()> {
        let mut buf = Vec::with_capacity(16 + info_hashes.len() * 20);
        buf.extend_from_slice(&connection_id.to_be_bytes());
        buf.extend_from_slice(&ACTION_SCRAPE.to_be_bytes());
        buf.extend_from_slice(&tx_id.to_be_bytes());
        for ih in info_hashes {
            buf.extend_from_slice(ih);
        }

        socket.send_to(&buf, target).await?;

        let mut recv_buf = [0u8; 1024];
        let (len, _) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut recv_buf)).await??;
        if len < 8 { return Err(anyhow::anyhow!("scrape 响应太短")); }
        let action = u32::from_be_bytes(recv_buf[0..4].try_into().unwrap());
        if action != ACTION_SCRAPE { return Err(anyhow::anyhow!("scrape action 不匹配: {}", action)); }
        Ok(())
    }
}

#[async_trait]
impl Scenario for UdpTrackerScenario {
    fn name(&self) -> &str { "udp_tracker" }

    fn description(&self) -> &str { "PDC 超级 Tracker UDP 压测（BEP15，Socket池复用）" }

    async fn setup(&self, ctx: &BenchContext) -> anyhow::Result<()> {
        let target: SocketAddr = ctx.args.get("target")
            .map(|s| s.parse().unwrap_or_else(|_| "127.0.0.1:6880".parse().unwrap()))
            .unwrap_or_else(|| "127.0.0.1:6880".parse().unwrap());
        let _ = self.target.set(target);

        // 创建 Arc<UdpSocket> 池
        let pool_size = std::cmp::max(ctx.concurrency as usize, 64);
        debug!("创建 Socket 池，大小: {}", pool_size);
        let mut sockets = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            sockets.push(Arc::new(UdpSocket::bind("0.0.0.0:0").await?));
        }
        let pool = Arc::new(sockets);
        let _ = self.socket_pool.set(pool.clone());

        // 预建立 connection_id
        let mut connections = Vec::new();
        for i in 0..200 {
            let tx_id = self.tx_ids[i % self.tx_ids.len()];
            match Self::udp_connect(&pool[0], target, tx_id).await {
                Ok(cid) => connections.push(cid),
                Err(e) => debug!("connect 失败: {}", e),
            }
        }
        debug!("预建立 {} 个 connection_id", connections.len());
        *self.connections.lock().unwrap() = connections;

        Ok(())
    }

    async fn request(&self, ctx: &BenchContext) -> anyhow::Result<()> {
        let target = *self.target.get().ok_or_else(|| anyhow::anyhow!("target 未初始化"))?;
        let pool = self.socket_pool.get().ok_or_else(|| anyhow::anyhow!("Socket 池未初始化"))?;

        let idx = self.socket_idx.fetch_add(1, Ordering::Relaxed) % pool.len();
        let socket = &pool[idx];

        // 获取 connection_id（std Mutex，无 await）
        let connection_id = {
            let conns = self.connections.lock().unwrap();
            if conns.is_empty() { None } else {
                let idx = ctx.next_request_id() as usize % conns.len();
                Some(conns[idx])
            }
        };

        let connection_id = match connection_id {
            Some(cid) => cid,
            None => {
                let cid = Self::udp_connect(socket, target, self.next_tx_id()).await?;
                self.connections.lock().unwrap().push(cid);
                cid
            }
        };

        let mode = ctx.args.get("mode").map(|s| s.as_str()).unwrap_or("announce");
        let tx_id = self.next_tx_id();

        let do_announce = match mode {
            "announce" => true,
            "scrape" => false,
            "mixed" => (self.tx_ids[self.tx_idx.load(Ordering::Relaxed) % self.tx_ids.len()] % 10) < 8,
            _ => true,
        };

        if do_announce {
            let req_id = ctx.next_request_id() as usize;
            let ih = &self.infohashes[req_id % self.infohashes.len()];
            let pid = &self.peer_ids[req_id % self.peer_ids.len()];
            let port = 6881u16 + (req_id % 1000) as u16;
            let key = self.next_key();
            Self::udp_announce(socket, target, connection_id, ih, pid, port, tx_id, key).await?;
        } else {
            let req_id = ctx.next_request_id() as usize;
            let count = (self.next_key() as usize % 5) + 1;
            let start = req_id % self.infohashes.len();
            let mut ihs = Vec::with_capacity(count);
            for i in 0..count {
                ihs.push(self.infohashes[(start + i) % self.infohashes.len()]);
            }
            Self::udp_scrape(socket, target, connection_id, &ihs, tx_id).await?;
        }

        Ok(())
    }
}
