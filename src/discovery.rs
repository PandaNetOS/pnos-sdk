//! 服务发现缓存
//!
//! 缓存 [`pnos::registry::AppDiscoverResponse`]，避免每次应用间调用
//! 都向 runtime 发 discover 请求。缓存 TTL 由 [`SdkConfig::discovery_cache_ttl`] 控制。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pnos::registry::AppDiscoverResponse;
use tokio::sync::RwLock;

use crate::config::SdkConfig;
use crate::error::Result;
use crate::registry::RuntimeClient;

/// 缓存条目
struct CacheEntry {
    response: AppDiscoverResponse,
    cached_at: Instant,
}

/// 服务发现缓存
#[derive(Clone)]
pub struct DiscoveryCache {
    runtime: RuntimeClient,
    config: Arc<SdkConfig>,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
}

impl DiscoveryCache {
    pub fn new(runtime: RuntimeClient, config: Arc<SdkConfig>) -> Self {
        Self {
            runtime,
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 发现应用地址（带缓存）
    pub async fn discover(&self, app_id: &str) -> Result<AppDiscoverResponse> {
        // 1. 查缓存
        if let Some(entry) = self.cache.read().await.get(app_id) {
            if entry.cached_at.elapsed() < self.config.discovery_cache_ttl {
                return Ok(entry.response.clone());
            }
        }

        // 2. 缓存未命中或过期，调用 runtime
        let response = self.runtime.discover(app_id).await?;

        // 3. 写入缓存
        self.cache.write().await.insert(
            app_id.to_string(),
            CacheEntry {
                response: response.clone(),
                cached_at: Instant::now(),
            },
        );

        Ok(response)
    }

    /// 使指定应用的缓存失效（收到服务变更事件时调用）
    pub async fn invalidate(&self, app_id: &str) {
        self.cache.write().await.remove(app_id);
    }

    /// 清除全部缓存
    pub async fn clear(&self) {
        self.cache.write().await.clear();
    }

    /// 缓存 TTL
    pub fn ttl(&self) -> Duration {
        self.config.discovery_cache_ttl
    }
}
