//! 节点发现 trait
//!
//! 定义统一的节点发现接口，允许外部（如 pdc 的 DHT 发现）接入 NetAgent。
//!
//! # 示例
//!
//! ```ignore
//! use pnos_net::discovery::Discovery;
//! use pnos_net::types::DiscoveredNode;
//! use async_trait::async_trait;
//!
//! pub struct MyDhtDiscovery {
//!     discovered_tx: broadcast::Sender<DiscoveredNode>,
//! }
//!
//! #[async_trait]
//! impl Discovery for MyDhtDiscovery {
//!     fn name(&self) -> &str { "DHT" }
//!     async fn start(&self) -> anyhow::Result<()> { /* 启动 DHT 发现 */ Ok(()) }
//!     fn stop(&self) { /* 停止 */ }
//! }
//! ```

use async_trait::async_trait;

/// 节点发现接口
///
/// 实现此 trait 的发现器可以注册到 NetAgent，
/// 发现的节点通过构造时传入的 `broadcast::Sender<DiscoveredNode>` 输出。
#[async_trait]
pub trait Discovery: Send + Sync {
    /// 发现器名称（用于日志和指标）
    fn name(&self) -> &str;

    /// 启动发现（非阻塞，内部应 spawn 后台任务）
    async fn start(&self) -> anyhow::Result<()>;

    /// 停止发现
    fn stop(&self);

    /// 是否启用
    fn enabled(&self) -> bool {
        true
    }
}
