//! pnos-runtime API 客户端封装（统一组件协议）
//!
//! 封装 runtime 的全部 REST API：注册、注销、心跳、组件列表/详情/发现、
//! 系统信息/监控/健康检查。所有请求自动带 `X-Pnos-Token`。

use std::sync::Arc;
use std::time::Duration;

use pnos::component::{ComponentStatus, ComponentType};
use pnos::discovery::ComponentDiscoverResponse;
use pnos::registry::{
    ComponentInfo, ComponentRegisterRequest, ComponentRegisterResponse, HeartbeatRequest,
};
use pnos::response::ApiResponse;
use pnos::system::{SystemInfo, SystemStats};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::config::SdkConfig;
use crate::error::{Result, SdkError};

/// runtime API 客户端
#[derive(Clone)]
pub struct RuntimeClient {
    config: Arc<SdkConfig>,
    http: reqwest::Client,
    token: Arc<RwLock<Option<String>>>,
}

impl RuntimeClient {
    pub fn new(
        config: Arc<SdkConfig>,
        http: reqwest::Client,
        token: Arc<RwLock<Option<String>>>,
    ) -> Self {
        Self {
            config,
            http,
            token,
        }
    }

    /// 获取当前 token
    async fn token(&self) -> Option<String> {
        self.token.read().await.clone()
    }

    /// 设置 token（注册成功后调用）
    pub async fn set_token(&self, token: String) {
        *self.token.write().await = Some(token);
    }

    /// 注册组件
    pub async fn register(
        &self,
        req: &ComponentRegisterRequest,
    ) -> Result<ComponentRegisterResponse> {
        let url = format!("{}/components/register", self.config.api_base());
        debug!("注册组件: {} -> {}", req.id, url);

        let resp = self
            .http
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("注册请求失败: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(SdkError::new(
                pnos::error::ErrorCode::ComponentNotRegistered,
                format!("HTTP {status}: {text}"),
            ));
        }

        let body: ApiResponse<ComponentRegisterResponse> = resp.json().await?;

        if body.code != 0 {
            return Err(SdkError::new(
                pnos::error::ErrorCode::ComponentNotRegistered,
                body.message,
            ));
        }

        body.data
            .ok_or_else(|| SdkError::Other("注册响应 data 为空".to_string()))
    }

    /// 注册组件（带指数退避重试，runtime 未就绪时自动重试）
    ///
    /// 重试策略：1s, 2s, 4s, 8s, 16s，最多重试到 timeout（默认 30s）
    pub async fn register_with_retry(
        &self,
        req: &ComponentRegisterRequest,
        timeout: Duration,
    ) -> Result<ComponentRegisterResponse> {
        let start = std::time::Instant::now();
        let mut attempt = 0u32;

        loop {
            match self.register(req).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    attempt += 1;
                    let elapsed = start.elapsed();
                    if elapsed >= timeout {
                        warn!("注册超时（{:?}），最后错误: {}", timeout, e);
                        return Err(e);
                    }
                    // 指数退避：1s, 2s, 4s, 8s, 16s（上限 16s）
                    let backoff = Duration::from_secs(1u64 << attempt.min(4));
                    // 剩余时间不足时，只等剩余时间
                    let wait = backoff.min(timeout.saturating_sub(elapsed));
                    debug!("注册失败（第 {} 次），{:?} 后重试: {}", attempt, wait, e);
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    /// 注销组件
    pub async fn unregister(&self) -> Result<bool> {
        let url = format!("{}/components/unregister", self.config.api_base());
        let token = self.token().await;

        let mut req = self.http.post(&url).json(&serde_json::json!({
            "id": self.config.app_id
        }));
        if let Some(t) = &token {
            req = req.header("X-Pnos-Token", t);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("注销请求失败: {e}")))?;

        if !resp.status().is_success() {
            warn!("注销失败: HTTP {}", resp.status());
            return Ok(false);
        }

        let body: ApiResponse<bool> = resp.json().await?;

        Ok(body.data.unwrap_or(false))
    }

    /// 发送心跳（统一组件心跳，含状态+负载+任务统计）
    pub async fn heartbeat(
        &self,
        status: ComponentStatus,
        load: f32,
        active_tasks: u32,
        bytes_downloaded: u64,
    ) -> Result<bool> {
        let url = format!("{}/components/heartbeat", self.config.api_base());
        let token = self.token().await;

        let req_body = HeartbeatRequest {
            id: self.config.app_id.clone(),
            status,
            active_tasks,
            bytes_downloaded,
            speed_bps: 0,
            load,
            message: None,
        };

        let mut req = self.http.post(&url).json(&req_body);
        if let Some(t) = &token {
            req = req.header("X-Pnos-Token", t);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("心跳请求失败: {e}")))?;

        if !resp.status().is_success() {
            warn!("心跳失败: HTTP {}", resp.status());
            return Ok(false);
        }

        let body: ApiResponse<bool> = resp.json().await?;

        Ok(body.data.unwrap_or(false))
    }

    /// 列出所有已注册组件
    pub async fn list_components(&self) -> Result<Vec<ComponentInfo>> {
        let url = format!("{}/components", self.config.api_base());
        let token = self.token().await;

        let mut req = self.http.get(&url);
        if let Some(t) = &token {
            req = req.header("X-Pnos-Token", t);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("查询组件列表失败: {e}")))?;

        if !resp.status().is_success() {
            return Err(SdkError::new(
                pnos::error::ErrorCode::ComponentNotRegistered,
                format!("HTTP {}", resp.status()),
            ));
        }

        let body: ApiResponse<Vec<ComponentInfo>> = resp.json().await?;

        Ok(body.data.unwrap_or_default())
    }

    /// 获取单个组件详情
    pub async fn component_detail(&self, component_id: &str) -> Result<ComponentInfo> {
        let url = format!("{}/components/{}", self.config.api_base(), component_id);
        let token = self.token().await;

        let mut req = self.http.get(&url);
        if let Some(t) = &token {
            req = req.header("X-Pnos-Token", t);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("查询组件详情失败: {e}")))?;

        if !resp.status().is_success() {
            return Err(SdkError::ComponentNotFound(component_id.to_string()));
        }

        let body: ApiResponse<ComponentInfo> = resp.json().await?;

        body.data
            .ok_or_else(|| SdkError::ComponentNotFound(component_id.to_string()))
    }

    /// 发现组件地址
    pub async fn discover(&self, component_id: &str) -> Result<ComponentDiscoverResponse> {
        let url = format!(
            "{}/components/{}/discover",
            self.config.api_base(),
            component_id
        );
        let token = self.token().await;

        let mut req = self.http.get(&url);
        if let Some(t) = &token {
            req = req.header("X-Pnos-Token", t);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("服务发现失败: {e}")))?;

        if !resp.status().is_success() {
            return Err(SdkError::ComponentNotFound(component_id.to_string()));
        }

        let body: ApiResponse<ComponentDiscoverResponse> = resp.json().await?;

        body.data
            .ok_or_else(|| SdkError::ComponentNotFound(component_id.to_string()))
    }

    /// 按类型筛选组件
    pub async fn list_by_type(&self, component_type: ComponentType) -> Result<Vec<ComponentInfo>> {
        let all = self.list_components().await?;
        Ok(all
            .into_iter()
            .filter(|c| c.component_type == component_type)
            .collect())
    }

    /// 获取系统信息
    pub async fn system_info(&self) -> Result<SystemInfo> {
        let url = format!("{}/system/info", self.config.api_base());
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("获取系统信息失败: {e}")))?;

        let body: ApiResponse<SystemInfo> = resp.json().await?;

        body.data
            .ok_or_else(|| SdkError::Other("系统信息为空".to_string()))
    }

    /// 获取系统实时监控数据
    pub async fn system_stats(&self) -> Result<SystemStats> {
        let url = format!("{}/system/stats", self.config.api_base());
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("获取系统监控失败: {e}")))?;

        let body: ApiResponse<SystemStats> = resp.json().await?;

        body.data
            .ok_or_else(|| SdkError::Other("系统监控数据为空".to_string()))
    }
}
