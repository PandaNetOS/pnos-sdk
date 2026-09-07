//! pnos-runtime API 客户端封装
//!
//! 封装 runtime 的全部 REST API：注册、注销、心跳、应用列表/详情/发现、
//! 系统信息/监控/健康检查。所有请求自动带 `X-Pnos-Token`。

use std::sync::Arc;

use pnos::app::AppStatus;
use pnos::registry::{
    AppDiscoverResponse, AppInfo, AppRegisterRequest, AppRegisterResponse, HeartbeatRequest,
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

    /// 注册应用
    pub async fn register(&self, req: &AppRegisterRequest) -> Result<AppRegisterResponse> {
        let url = format!("{}/apps/register", self.config.api_base());
        debug!("注册应用: {} -> {}", req.id, url);

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
                pnos::error::ErrorCode::AppNotRegistered,
                format!("HTTP {status}: {text}"),
            ));
        }

        let body: ApiResponse<AppRegisterResponse> = resp.json().await?;

        if body.code != 0 {
            return Err(SdkError::new(
                pnos::error::ErrorCode::AppNotRegistered,
                body.message,
            ));
        }

        body.data
            .ok_or_else(|| SdkError::Other("注册响应 data 为空".to_string()))
    }

    /// 注销应用
    pub async fn unregister(&self) -> Result<bool> {
        let url = format!("{}/apps/unregister", self.config.api_base());
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

    /// 发送心跳
    pub async fn heartbeat(&self, status: AppStatus, message: Option<String>) -> Result<bool> {
        let url = format!("{}/apps/heartbeat", self.config.api_base());
        let token = self.token().await;

        let req_body = HeartbeatRequest {
            id: self.config.app_id.clone(),
            status,
            message,
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

    /// 列出所有已注册应用
    pub async fn list_apps(&self) -> Result<Vec<AppInfo>> {
        let url = format!("{}/apps", self.config.api_base());
        let token = self.token().await;

        let mut req = self.http.get(&url);
        if let Some(t) = &token {
            req = req.header("X-Pnos-Token", t);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("查询应用列表失败: {e}")))?;

        if !resp.status().is_success() {
            return Err(SdkError::new(
                pnos::error::ErrorCode::AppNotFound,
                format!("HTTP {}", resp.status()),
            ));
        }

        let body: ApiResponse<Vec<AppInfo>> = resp.json().await?;

        Ok(body.data.unwrap_or_default())
    }

    /// 获取单个应用详情
    pub async fn app_detail(&self, app_id: &str) -> Result<AppInfo> {
        let url = format!("{}/apps/{}", self.config.api_base(), app_id);
        let token = self.token().await;

        let mut req = self.http.get(&url);
        if let Some(t) = &token {
            req = req.header("X-Pnos-Token", t);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SdkError::Network(format!("查询应用详情失败: {e}")))?;

        if !resp.status().is_success() {
            return Err(SdkError::AppNotFound(app_id.to_string()));
        }

        let body: ApiResponse<AppInfo> = resp.json().await?;

        body.data
            .ok_or_else(|| SdkError::AppNotFound(app_id.to_string()))
    }

    /// 发现应用地址
    pub async fn discover(&self, app_id: &str) -> Result<AppDiscoverResponse> {
        let url = format!("{}/apps/{}/discover", self.config.api_base(), app_id);
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
            return Err(SdkError::AppNotFound(app_id.to_string()));
        }

        let body: ApiResponse<AppDiscoverResponse> = resp.json().await?;

        body.data
            .ok_or_else(|| SdkError::AppNotFound(app_id.to_string()))
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
