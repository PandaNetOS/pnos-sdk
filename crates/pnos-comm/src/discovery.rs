//! 服务发现缓存
//!
//! 缓存 [`pnos::discovery::ComponentDiscoverResponse`]，避免每次组件间调用
//! 都向 runtime 发 discover 请求。
//!
//! v1.1 增强：
//! - **stale-while-revalidate**：缓存过期后先返回旧数据，后台异步刷新
//! - **stale-if-error**：runtime 不可用时返回旧缓存兜底
//!
//! 缓存 TTL 由 [`SdkConfig::discovery_cache_ttl`] 控制。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pnos::discovery::ComponentDiscoverResponse;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::config::SdkConfig;
use crate::error::Result;
use crate::registry::RuntimeClient;

/// 缓存条目
struct CacheEntry {
    response: ComponentDiscoverResponse,
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

    /// 发现组件地址（带缓存 + stale-while-revalidate + stale-if-error）
    ///
    /// 策略：
    /// 1. 缓存命中且未过期 → 直接返回
    /// 2. 缓存命中但已过期 → 先返回旧缓存（stale-while-revalidate），后台异步刷新
    /// 3. 缓存未命中 → 同步请求 runtime
    ///    - 成功 → 更新缓存，返回
    ///    - 失败 → 有旧缓存？返回旧缓存（stale-if-error）+ 后台重试
    ///    - 无旧缓存？返回错误
    pub async fn discover(&self, component_id: &str) -> Result<ComponentDiscoverResponse> {
        let ttl = self.config.discovery_cache_ttl;

        // 1. 查缓存
        let cached = {
            let cache = self.cache.read().await;
            cache
                .get(component_id)
                .map(|e| (e.response.clone(), e.cached_at))
        };

        if let Some((response, cached_at)) = &cached {
            if cached_at.elapsed() < ttl {
                // 缓存未过期，直接返回
                return Ok(response.clone());
            }

            // 缓存已过期：stale-while-revalidate，先返回旧数据，后台刷新
            debug!("缓存过期，stale-while-revalidate: {}", component_id);
            let runtime = self.runtime.clone();
            let cache = self.cache.clone();
            let cid = component_id.to_string();
            let cid_clone = cid.clone();
            tokio::spawn(async move {
                match runtime.discover(&cid_clone).await {
                    Ok(resp) => {
                        cache.write().await.insert(
                            cid_clone,
                            CacheEntry {
                                response: resp,
                                cached_at: Instant::now(),
                            },
                        );
                        debug!("后台刷新缓存成功: {}", cid);
                    }
                    Err(e) => {
                        warn!("后台刷新缓存失败: {} - {}", cid, e);
                    }
                }
            });
            return Ok(response.clone());
        }

        // 2. 缓存未命中，同步请求 runtime
        match self.runtime.discover(component_id).await {
            Ok(response) => {
                // 3. 写入缓存
                self.cache.write().await.insert(
                    component_id.to_string(),
                    CacheEntry {
                        response: response.clone(),
                        cached_at: Instant::now(),
                    },
                );
                Ok(response)
            }
            Err(e) => {
                // stale-if-error：runtime 不可用时，检查是否有旧缓存（理论上不会走到这里，因为上面已经处理了缓存）
                // 但如果是第一次请求且 runtime 不可用，直接返回错误
                warn!("服务发现失败（无缓存兜底）: {} - {}", component_id, e);
                Err(e)
            }
        }
    }

    /// 强制刷新缓存（忽略 TTL，同步请求 runtime）
    pub async fn refresh(&self, component_id: &str) -> Result<ComponentDiscoverResponse> {
        let response = self.runtime.discover(component_id).await?;
        self.cache.write().await.insert(
            component_id.to_string(),
            CacheEntry {
                response: response.clone(),
                cached_at: Instant::now(),
            },
        );
        Ok(response)
    }

    /// 使指定组件的缓存失效（收到服务变更事件时调用）
    pub async fn invalidate(&self, component_id: &str) {
        self.cache.write().await.remove(component_id);
        debug!("缓存失效: {}", component_id);
    }

    /// 清除全部缓存
    pub async fn clear(&self) {
        self.cache.write().await.clear();
    }

    /// 缓存 TTL
    pub fn ttl(&self) -> Duration {
        self.config.discovery_cache_ttl
    }

    /// 获取缓存的组件数量
    pub async fn len(&self) -> usize {
        self.cache.read().await.len()
    }

    /// 缓存是否为空
    pub async fn is_empty(&self) -> bool {
        self.cache.read().await.is_empty()
    }
}
