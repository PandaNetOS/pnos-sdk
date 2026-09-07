//! 组件间调用客户端
//!
//! 自动通过服务发现（带缓存）获取目标组件地址，自动注入认证 Token，
//! 失败自动重试。支持直连模式和 runtime 反向代理模式。

use std::sync::Arc;
use std::time::Duration;

use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::debug;

use crate::config::SdkConfig;
use crate::discovery::DiscoveryCache;
use crate::error::{Result, SdkError};
use pnos::response::ApiResponse;

/// 针对单个组件的调用客户端
#[derive(Clone)]
pub struct ComponentClient {
    discovery: DiscoveryCache,
    token: Arc<RwLock<Option<String>>>,
    http: reqwest::Client,
    config: Arc<SdkConfig>,
    target_component_id: String,
    /// 是否通过 runtime 反向代理调用（默认 false，直连）
    use_proxy: bool,
}

// 向后兼容别名
#[allow(dead_code)]
pub type AppClient = ComponentClient;

impl ComponentClient {
    pub(crate) fn new(
        discovery: DiscoveryCache,
        token: Arc<RwLock<Option<String>>>,
        http: reqwest::Client,
        config: Arc<SdkConfig>,
        target_component_id: &str,
    ) -> Self {
        Self {
            discovery,
            token,
            http,
            config,
            target_component_id: target_component_id.to_string(),
            use_proxy: false,
        }
    }

    /// 切换为通过 runtime 反向代理调用（/component/{id}/*）
    pub fn via_proxy(mut self) -> Self {
        self.use_proxy = true;
        self
    }

    /// GET 请求
    pub fn get(&self, path: &str) -> RequestBuilder {
        self.request(Method::GET, path)
    }

    /// POST 请求
    pub fn post<T: Serialize>(&self, path: &str, body: &T) -> RequestBuilder {
        let mut req = self.request(Method::POST, path);
        req.body = Some(serde_json::to_value(body).unwrap_or(serde_json::Value::Null));
        req
    }

    /// PUT 请求
    pub fn put<T: Serialize>(&self, path: &str, body: &T) -> RequestBuilder {
        let mut req = self.request(Method::PUT, path);
        req.body = Some(serde_json::to_value(body).unwrap_or(serde_json::Value::Null));
        req
    }

    /// DELETE 请求
    pub fn delete(&self, path: &str) -> RequestBuilder {
        self.request(Method::DELETE, path)
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        RequestBuilder {
            client: self.clone(),
            method,
            path: path.to_string(),
            body: None,
            headers: Vec::new(),
        }
    }
}

/// 请求构建器
pub struct RequestBuilder {
    client: ComponentClient,
    method: Method,
    path: String,
    body: Option<serde_json::Value>,
    headers: Vec<(String, String)>,
}

impl RequestBuilder {
    /// 添加请求头
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// 发送请求并解析响应 data
    pub async fn send<T: DeserializeOwned>(self) -> Result<T> {
        let resp = self.send_with_retry().await?;

        let api_resp: ApiResponse<T> = resp
            .json()
            .await
            .map_err(|e| SdkError::Other(format!("解析响应失败: {e}")))?;

        if api_resp.code != 0 {
            return Err(SdkError::new(
                pnos::error::ErrorCode::Unknown,
                format!("code={}, message={}", api_resp.code, api_resp.message),
            ));
        }

        api_resp
            .data
            .ok_or_else(|| SdkError::Other("响应 data 为空".to_string()))
    }

    /// 发送请求，只检查成功，不解析 data
    pub async fn send_empty(self) -> Result<()> {
        let resp = self.send_with_retry().await?;
        let api_resp: ApiResponse<serde_json::Value> = resp
            .json()
            .await
            .map_err(|e| SdkError::Other(format!("解析响应失败: {e}")))?;

        if api_resp.code != 0 {
            return Err(SdkError::new(
                pnos::error::ErrorCode::Unknown,
                format!("code={}, message={}", api_resp.code, api_resp.message),
            ));
        }
        Ok(())
    }

    /// 带重试地发送请求
    async fn send_with_retry(self) -> Result<reqwest::Response> {
        let max_retries = self.client.config.call_retries;
        let mut last_error: Option<SdkError> = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                // 退避：100ms * 2^attempt
                let backoff = Duration::from_millis(100 * 2u64.pow(attempt.min(5)));
                tokio::time::sleep(backoff).await;
                debug!(
                    "重试调用 {} {}/{} (第 {} 次)",
                    self.client.target_component_id, self.method, self.path, attempt
                );
            }

            match self.send_once().await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    // 只有网络错误和 5xx 才重试
                    let retryable =
                        matches!(e, SdkError::Network(_) | SdkError::ComponentUnreachable(_));
                    if !retryable || attempt == max_retries {
                        return Err(e);
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| SdkError::Other("未知错误".to_string())))
    }

    /// 发送一次请求（无重试）
    async fn send_once(&self) -> Result<reqwest::Response> {
        let token = self.client.token.read().await.clone();

        // 1. 确定目标 URL
        let base_url = if self.client.use_proxy {
            // 代理模式：通过 runtime 的 /component/{id}/* 转发
            format!(
                "{}/component/{}",
                self.client.config.runtime_url.trim_end_matches('/'),
                self.client.target_component_id
            )
        } else {
            // 直连模式：通过服务发现获取地址
            let discovered = self
                .client
                .discovery
                .discover(&self.client.target_component_id)
                .await?;
            discovered.accessible_url().to_string()
        };

        let url = format!("{}{}", base_url.trim_end_matches('/'), self.path);
        debug!(
            "调用 {} {} -> {}",
            self.method, self.client.target_component_id, url
        );

        // 2. 构建请求
        let mut req = self.client.http.request(self.method.clone(), &url);

        if let Some(t) = &token {
            req = req.header("X-Pnos-Token", t);
        }

        for (k, v) in &self.headers {
            req = req.header(k, v);
        }

        if let Some(body) = &self.body {
            req = req.json(body);
        }

        // 3. 发送
        let resp = req
            .send()
            .await
            .map_err(|e| SdkError::ComponentUnreachable(format!("请求失败: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(SdkError::ComponentUnreachable(format!(
                "HTTP {status}: {text}"
            )));
        }

        Ok(resp)
    }
}
