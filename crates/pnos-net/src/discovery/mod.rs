//! 节点发现层
//!
//! 多种节点发现机制的统一封装：
//! - `lpd`：局域网多播发现（同子网零配置）
//! - `peer_cache`：历史节点缓存持久化（重启自动重连）
//! - `traits`：Discovery trait，供外部发现器（如 DHT）接入

pub mod lpd;
pub mod mqtt;
pub mod peer_cache;
pub mod traits;

pub use traits::Discovery;
