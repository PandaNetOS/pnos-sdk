//! 应用间调用客户端
//!
//! 自动通过 pnos-runtime 发现应用地址，自动注入认证 Token。

use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{PnosSdkError, Result};
use crate::PnosApp;

/// 针对单个应用的调用客户端
#[derive(Clone)]
pub struct AppClient {
    app: PnosApp,
    target_app_id: String,
}

impl AppClient {
    pub(crate) fn new(app: PnosApp, target_app_id: &str) -> Self {
        Self {
            app,
            target_app_id: target_app_id.to_string(),
        }
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
            app: self.app.clone(),
            target_app_id: self.target_app_id.clone(),
            method,
            path: path.to_string(),
            body: None,
            headers: Vec::new(),
        }
    }
}

/// 请求构建器
pub struct RequestBuilder {
    app: PnosApp,
    target_app_id: String,
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
        let resp = self.send_raw().await?;

        let api_resp: pnos::response::ApiResponse<T> = resp
            .json()
            .await
            .map_err(|e| PnosSdkError::Other(format!("解析响应失败: {e}")))?;

        if api_resp.code != 0 {
            return Err(PnosSdkError::Api {
                code: api_resp.code,
                message: api_resp.message,
            });
        }

        api_resp
            .data
            .ok_or_else(|| PnosSdkError::Other("响应 data 为空".to_string()))
    }

    /// 发送请求，只检查 code，不解析 data
    pub async fn send_empty(self) -> Result<()> {
        let resp = self.send_raw().await?;
        let api_resp: pnos::response::ApiResponse<serde_json::Value> = resp
            .json()
            .await
            .map_err(|e| PnosSdkError::Other(format!("解析响应失败: {e}")))?;

        if api_resp.code != 0 {
            return Err(PnosSdkError::Api {
                code: api_resp.code,
                message: api_resp.message,
            });
        }
        Ok(())
    }

    async fn send_raw(self) -> Result<reqwest::Response> {
        // 1. 通过 pnos-runtime 发现目标应用地址
        let discover_url = format!(
            "{}/api/v1/apps/{}/discover",
            self.app.config.runtime_url.trim_end_matches('/'),
            self.target_app_id
        );

        let token = self.app.token().await;
        let mut discover_req = self.app.http.get(&discover_url);
        if let Some(t) = &token {
            discover_req = discover_req.header("X-Pnos-Token", t);
        }

        let discover_resp = discover_req
            .send()
            .await
            .map_err(|e| PnosSdkError::Network(format!("服务发现失败: {e}")))?;

        if !discover_resp.status().is_success() {
            return Err(PnosSdkError::AppNotFound(self.target_app_id.clone()));
        }

        let discover_body: pnos::response::ApiResponse<pnos::registry::AppDiscoverResponse> =
            discover_resp
                .json()
                .await
                .map_err(|e| PnosSdkError::Other(format!("解析服务发现响应失败: {e}")))?;

        let base_url = discover_body
            .data
            .map(|d| d.base_url)
            .ok_or_else(|| PnosSdkError::AppNotFound(self.target_app_id.clone()))?;

        // 2. 构建实际请求
        let url = format!("{}{}", base_url.trim_end_matches('/'), self.path);
        let mut req = self.app.http.request(self.method, &url);

        if let Some(t) = &token {
            req = req.header("X-Pnos-Token", t);
        }

        for (k, v) in &self.headers {
            req = req.header(k, v);
        }

        if let Some(body) = self.body {
            req = req.json(&body);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| PnosSdkError::AppUnreachable(format!("请求失败: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(PnosSdkError::Api {
                code: status.as_u16() as u32,
                message: format!("HTTP {status}: {text}"),
            });
        }

        Ok(resp)
    }
}
