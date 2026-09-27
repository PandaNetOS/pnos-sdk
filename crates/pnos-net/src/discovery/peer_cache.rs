//! 节点缓存持久化
//!
//! 将已连接过的节点地址持久化到磁盘，重启后自动尝试重连，
//! 实现"默认启动自动连接其他节点"的零配置体验。
//!
//! 每个节点保存多个 endpoint（内网/公网），连接时按优先级选择。
//! 兼容旧版单地址格式文件，load 时自动迁移。
//!
//! 缓存文件路径：`{data_dir}/federation_peers.json`

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::{DiscoverySource, EndpointKind, NodeEndpoint};

/// 缓存的单个节点地址
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedEndpoint {
    /// 地址（"ip:port" 字符串）
    pub addr: String,
    /// 地址类型（"Lan" / "Public" / "Ipv6" / "Loopback"）
    pub kind: String,
    /// 发现来源
    pub source: String,
    /// 最后一次连接成功时间（Unix 秒）
    pub last_success: u64,
    /// 累计成功连接次数
    pub success_count: u32,
    /// 连续失败次数
    pub fail_count: u32,
    /// 平均延迟（毫秒）
    pub latency_ms: Option<u32>,
}

/// 缓存的节点信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedNode {
    /// 节点 ID（20 字节）
    pub node_id: [u8; 20],
    /// 多 endpoint 列表（内网/公网都存）
    #[serde(default)]
    pub endpoints: Vec<CachedEndpoint>,
    /// 旧版单地址（已废弃，仅用于兼容旧文件迁移）
    #[serde(default)]
    pub addr: String,
    /// 最后一次活跃时间（Unix 时间戳，秒）
    #[serde(default)]
    pub last_seen: u64,
    /// 连接成功次数（旧版字段，兼容用）
    #[serde(default)]
    pub success_count: u32,
}

/// 节点缓存
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PeerCache {
    /// 最后更新时间（Unix 时间戳，秒）
    pub last_updated: u64,
    /// 缓存的节点列表
    pub nodes: Vec<CachedNode>,
}

impl PeerCache {
    /// 缓存文件名
    const CACHE_FILE: &'static str = "federation_peers.json";

    /// 从文件加载缓存，文件不存在或解析失败返回空
    pub fn load(data_dir: &Path) -> Self {
        let path = data_dir.join(Self::CACHE_FILE);
        match std::fs::read_to_string(&path) {
            Ok(content) => match serde_json::from_str::<PeerCache>(&content) {
                Ok(mut cache) => {
                    // 旧格式迁移：如果 endpoints 为空但 addr 有值，自动升级
                    for node in &mut cache.nodes {
                        if node.endpoints.is_empty() && !node.addr.is_empty() {
                            let kind = classify_addr_string(&node.addr);
                            node.endpoints.push(CachedEndpoint {
                                addr: node.addr.clone(),
                                kind: kind.to_string(),
                                source: "PeerCache".to_string(),
                                last_success: node.last_seen,
                                success_count: node.success_count,
                                fail_count: 0,
                                latency_ms: None,
                            });
                        }
                    }
                    tracing::debug!("[net] 节点缓存加载成功，共 {} 个节点", cache.nodes.len());
                    cache
                }
                Err(e) => {
                    tracing::warn!("[net] 节点缓存解析失败，使用空缓存: {}", e);
                    Self::default()
                }
            },
            Err(_) => {
                tracing::debug!("[net] 节点缓存文件不存在，使用空缓存");
                Self::default()
            }
        }
    }

    /// 原子写入文件（先写 .tmp 再 rename，防止崩溃导致文件损坏）
    pub fn save(&self, data_dir: &Path) -> anyhow::Result<()> {
        let path = data_dir.join(Self::CACHE_FILE);
        let tmp_path = path.with_extension("json.tmp");

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp_path, json)?;
        std::fs::rename(&tmp_path, &path)?;

        tracing::debug!("[net] 节点缓存已保存，共 {} 个节点", self.nodes.len());
        Ok(())
    }

    /// 添加或更新节点地址
    ///
    /// 如果该节点已有相同地址的 endpoint，更新统计信息；
    /// 如果是新地址，追加到 endpoints 列表。
    /// 同时更新旧字段 addr / success_count 保持兼容。
    pub fn upsert(&mut self, node_id: &[u8; 20], addr: &str, success: bool) {
        let now = current_unix_secs();
        let kind = classify_addr_string(addr);

        if let Some(node) = self.nodes.iter_mut().find(|n| &n.node_id == node_id) {
            // 更新旧字段（兼容）
            node.addr = addr.to_string();
            node.last_seen = now;

            // 在 endpoints 里找匹配的地址
            if let Some(ep) = node.endpoints.iter_mut().find(|e| e.addr == addr) {
                ep.last_success = now;
                if success {
                    ep.success_count = ep.success_count.saturating_add(1);
                    ep.fail_count = 0; // 连续失败清零
                } else {
                    ep.fail_count = ep.fail_count.saturating_add(1);
                }
            } else {
                // 新地址，追加
                node.endpoints.push(CachedEndpoint {
                    addr: addr.to_string(),
                    kind: kind.to_string(),
                    source: "PeerCache".to_string(),
                    last_success: if success { now } else { 0 },
                    success_count: if success { 1 } else { 0 },
                    fail_count: if success { 0 } else { 1 },
                    latency_ms: None,
                });
            }

            // 更新旧字段 success_count（取所有 endpoint 的最大值）
            node.success_count = node
                .endpoints
                .iter()
                .map(|e| e.success_count)
                .max()
                .unwrap_or(0);
        } else {
            // 新节点
            let ep = CachedEndpoint {
                addr: addr.to_string(),
                kind: kind.to_string(),
                source: "PeerCache".to_string(),
                last_success: if success { now } else { 0 },
                success_count: if success { 1 } else { 0 },
                fail_count: if success { 0 } else { 1 },
                latency_ms: None,
            };
            self.nodes.push(CachedNode {
                node_id: *node_id,
                endpoints: vec![ep],
                addr: addr.to_string(),
                last_seen: now,
                success_count: if success { 1 } else { 0 },
            });
        }
        self.last_updated = now;
    }

    /// 清理过期节点
    ///
    /// 只保留最近 7 天内有过成功连接（任一 endpoint success_count > 0）的节点，
    /// 按 success_count 降序排列后最多保留 max_nodes 个。
    /// 同时清理连续失败超过 20 次的 endpoint。
    pub fn prune(&mut self, max_nodes: usize) {
        let now = current_unix_secs();
        let seven_days = 7 * 24 * 3600;

        // 清理失败太多的 endpoint
        for node in &mut self.nodes {
            node.endpoints.retain(|e| e.fail_count < 20);
        }

        // 只保留有成功连接且最近活跃的节点
        self.nodes.retain(|n| {
            let has_success = n.endpoints.iter().any(|e| e.success_count > 0);
            let recent = now.saturating_sub(n.last_seen) < seven_days;
            has_success && recent
        });

        // 按总成功次数降序
        self.nodes.sort_by(|a, b| {
            let a_total: u32 = a.endpoints.iter().map(|e| e.success_count).sum();
            let b_total: u32 = b.endpoints.iter().map(|e| e.success_count).sum();
            b_total.cmp(&a_total)
        });
        self.nodes.truncate(max_nodes);
    }

    /// 返回前 n 个节点的最佳地址（内网优先）
    ///
    /// 对每个节点，从 endpoints 里选优先级最高的地址（内网 > 公网 > IPv6）。
    /// 然后按总成功次数降序排序。
    pub fn top_addrs(&self, n: usize) -> Vec<String> {
        let mut scored: Vec<(u32, String)> = self
            .nodes
            .iter()
            .filter_map(|node| {
                node.best_addr().map(|addr| {
                    let total_success: u32 = node.endpoints.iter().map(|e| e.success_count).sum();
                    (total_success, addr)
                })
            })
            .collect();

        scored.sort_by(|a, b| b.0.cmp(&a.0));
        scored.into_iter().take(n).map(|(_, addr)| addr).collect()
    }

    /// 返回所有节点的所有 endpoint 地址（用于启动时全量连接）
    pub fn all_endpoints(&self) -> Vec<(Vec<u8>, String, String)> {
        self.nodes
            .iter()
            .flat_map(|node| {
                node.endpoints
                    .iter()
                    .map(move |ep| (node.node_id.to_vec(), ep.addr.clone(), ep.kind.clone()))
            })
            .collect()
    }
}

impl CachedNode {
    /// 选最佳地址：内网优先 → 同类型按延迟升序 → 按成功次数降序
    pub fn best_addr(&self) -> Option<String> {
        self.endpoints
            .iter()
            .filter(|e| e.fail_count < 20)
            .min_by(|a, b| {
                let a_weight = kind_weight(&a.kind);
                let b_weight = kind_weight(&b.kind);
                a_weight
                    .cmp(&b_weight)
                    .then_with(|| a.latency_ms.cmp(&b.latency_ms))
                    .then_with(|| b.success_count.cmp(&a.success_count))
            })
            .map(|e| e.addr.clone())
    }
}

/// 地址类型优先级权重（数值越小优先级越高）
fn kind_weight(kind: &str) -> u8 {
    match kind {
        "Lan" => 0,
        "Loopback" => 1,
        "Public" => 2,
        "Ipv6" => 3,
        _ => 4,
    }
}

/// 根据地址字符串判断类型
fn classify_addr_string(addr: &str) -> &'static str {
    if let Ok(sa) = addr.parse::<SocketAddr>() {
        let kind = NodeEndpoint::classify(&sa);
        match kind {
            EndpointKind::Lan => "Lan",
            EndpointKind::Public => "Public",
            EndpointKind::Ipv6 => "Ipv6",
            EndpointKind::Loopback => "Loopback",
        }
    } else {
        "Public" // 解析失败默认公网
    }
}

/// 获取当前 Unix 时间戳（秒）
fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upsert_new_node() {
        let mut cache = PeerCache::default();
        let id = [1u8; 20];
        cache.upsert(&id, "192.168.1.1:6885", true);
        assert_eq!(cache.nodes.len(), 1);
        assert_eq!(cache.nodes[0].endpoints.len(), 1);
        assert_eq!(cache.nodes[0].endpoints[0].addr, "192.168.1.1:6885");
        assert_eq!(cache.nodes[0].endpoints[0].success_count, 1);
        assert_eq!(cache.nodes[0].endpoints[0].kind, "Lan");
    }

    #[test]
    fn test_upsert_existing_node_new_endpoint() {
        let mut cache = PeerCache::default();
        let id = [1u8; 20];
        cache.upsert(&id, "192.168.1.1:6885", true);
        // 同一节点加一个公网地址
        cache.upsert(&id, "1.2.3.4:6885", true);
        assert_eq!(cache.nodes.len(), 1);
        assert_eq!(cache.nodes[0].endpoints.len(), 2);
    }

    #[test]
    fn test_upsert_same_endpoint_increments() {
        let mut cache = PeerCache::default();
        let id = [1u8; 20];
        cache.upsert(&id, "192.168.1.1:6885", true);
        cache.upsert(&id, "192.168.1.1:6885", true);
        assert_eq!(cache.nodes[0].endpoints.len(), 1);
        assert_eq!(cache.nodes[0].endpoints[0].success_count, 2);
    }

    #[test]
    fn test_upsert_failure_increments_fail_count() {
        let mut cache = PeerCache::default();
        let id = [1u8; 20];
        cache.upsert(&id, "1.2.3.4:6885", false);
        assert_eq!(cache.nodes[0].endpoints[0].fail_count, 1);
        assert_eq!(cache.nodes[0].endpoints[0].success_count, 0);
    }

    #[test]
    fn test_best_addr_prefers_lan() {
        let mut cache = PeerCache::default();
        let id = [1u8; 20];
        cache.upsert(&id, "1.2.3.4:6885", true); // 公网
        cache.upsert(&id, "192.168.1.1:6885", true); // 内网
        let best = cache.nodes[0].best_addr().unwrap();
        assert_eq!(best, "192.168.1.1:6885");
    }

    #[test]
    fn test_top_addrs_prefers_lan() {
        let mut cache = PeerCache::default();
        for i in 0..3u8 {
            let id = [i; 20];
            cache.upsert(&id, &format!("1.2.3.{}:6885", i), true); // 公网
            cache.upsert(&id, &format!("192.168.1.{}:6885", i), true); // 内网
        }
        let top = cache.top_addrs(3);
        // 每个节点都应该选内网地址
        for addr in &top {
            assert!(addr.starts_with("192.168.1."), "应该选内网地址: {}", addr);
        }
    }

    #[test]
    fn test_prune_removes_old_and_zero_success() {
        let mut cache = PeerCache::default();
        let good_id = [1u8; 20];
        cache.upsert(&good_id, "1.1.1.1:6885", true);
        let zero_id = [2u8; 20];
        cache.upsert(&zero_id, "2.2.2.2:6885", false);
        let old_id = [3u8; 20];
        cache.upsert(&old_id, "3.3.3.3:6885", true);
        cache.nodes[2].last_seen = 0;

        cache.prune(100);
        assert_eq!(cache.nodes.len(), 1);
        assert_eq!(cache.nodes[0].node_id, good_id);
    }

    #[test]
    fn test_prune_removes_failed_endpoints() {
        let mut cache = PeerCache::default();
        let id = [1u8; 20];
        cache.upsert(&id, "192.168.1.1:6885", true); // 内网地址，保留
        cache.upsert(&id, "1.2.3.4:6885", true); // 公网地址，会失败
                                                 // 公网地址连续失败 20 次
        for _ in 0..20 {
            cache.upsert(&id, "1.2.3.4:6885", false);
        }
        cache.prune(100);
        // 节点还在（内网 endpoint 保留）
        assert_eq!(cache.nodes.len(), 1);
        // 公网 endpoint 应该被清理
        let still_has_public = cache.nodes[0]
            .endpoints
            .iter()
            .any(|e| e.addr == "1.2.3.4:6885");
        assert!(!still_has_public, "连续失败20次的endpoint应被清理");
        // 内网 endpoint 应该保留
        let still_has_lan = cache.nodes[0]
            .endpoints
            .iter()
            .any(|e| e.addr == "192.168.1.1:6885");
        assert!(still_has_lan, "内网endpoint应保留");
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("pnos_net_cache_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut cache = PeerCache::default();
        let id = [0xAB; 20];
        cache.upsert(&id, "192.168.1.1:6885", true);
        cache.upsert(&id, "192.168.1.1:6885", true);
        cache.upsert(&id, "1.2.3.4:6885", true);

        cache.save(&dir).unwrap();

        let loaded = PeerCache::load(&dir);
        assert_eq!(loaded.nodes.len(), 1);
        assert_eq!(loaded.nodes[0].endpoints.len(), 2);
        assert_eq!(loaded.nodes[0].endpoints[0].success_count, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_legacy_format_migrates() {
        let dir =
            std::env::temp_dir().join(format!("pnos_net_cache_legacy_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // 写旧格式文件（只有 addr，没有 endpoints）
        let legacy = r#"{
            "last_updated": 1700000000,
            "nodes": [
                {
                    "node_id": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20],
                    "addr": "192.168.1.1:6885",
                    "last_seen": 1700000000,
                    "success_count": 5
                }
            ]
        }"#;
        std::fs::write(dir.join("federation_peers.json"), legacy).unwrap();

        let loaded = PeerCache::load(&dir);
        assert_eq!(loaded.nodes.len(), 1);
        // 旧格式应该自动迁移成 endpoints
        assert_eq!(loaded.nodes[0].endpoints.len(), 1);
        assert_eq!(loaded.nodes[0].endpoints[0].addr, "192.168.1.1:6885");
        assert_eq!(loaded.nodes[0].endpoints[0].success_count, 5);
        assert_eq!(loaded.nodes[0].endpoints[0].kind, "Lan");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_nonexistent_returns_empty() {
        let dir =
            std::env::temp_dir().join(format!("pnos_net_cache_nonexist_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let cache = PeerCache::load(&dir);
        assert!(cache.nodes.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
