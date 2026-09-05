//! PandaNetOS 应用开发 SDK
//!
//! 应用启动时自动注册到 pnos-runtime，定期心跳，
//! 调用其他应用时自动发现地址并注入认证 Token。
//!
//! # 快速开始
//!
//! ```rust
//! use pnos_sdk::PnosApp;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // 初始化：自动注册 + 心跳
//!     let app = PnosApp::builder("my-app")
//!         .version("0.1.0")
//!         .port(18090)
//!         .init()
//!         .await?;
//!
//!     // 调用其他应用（自动发现地址 + 自动带 Token）
//!     let info: serde_json::Value = app
//!         .call("pk")
//!         .get("/api/v1/system/info")
//!         .send()
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod config;
pub mod error;
pub mod health;

pub use client::AppClient;
pub use config::SdkConfig;
pub use error::{PnosSdkError, Result};
pub use health::HealthBuilder;

use std::sync::Arc;
use std::time::Duration;

use pnos::app::AppStatus;
use pnos::registry::{AppRegisterRequest, HeartbeatRequest};
use tracing::{error, info, warn};

/// Pnos 应用主入口
#[derive(Clone)]
pub struct PnosApp {
    /// 应用 ID
    pub app_id: String,
    /// 应用版本
    pub version: String,
    /// 配置
    pub config: Arc<SdkConfig>,
    /// 认证 Token（注册后获得）
    token: Arc<tokio::sync::RwLock<Option<String>>>,
    /// HTTP 客户端
    pub(crate) http: reqwest::Client,
}

impl PnosApp {
    /// 创建构建器
    pub fn builder(app_id: impl Into<String>) -> PnosAppBuilder {
        PnosAppBuilder::new(app_id)
    }

    /// 调用其他应用
    pub fn call(&self, app_id: &str) -> AppClient {
        AppClient::new(self.clone(), app_id)
    }

    /// 获取当前 Token
    pub async fn token(&self) -> Option<String> {
        self.token.read().await.clone()
    }

    /// 手动发送心跳
    pub async fn heartbeat(&self, status: AppStatus) -> Result<()> {
        let token = self
            .token
            .read()
            .await
            .clone()
            .ok_or_else(|| PnosSdkError::Auth("未注册".to_string()))?;

        let req = HeartbeatRequest {
            id: self.app_id.clone(),
            status,
            message: None,
        };

        let resp = self
            .http
            .post(format!(
                "{}/api/v1/apps/heartbeat",
                self.config.runtime_url
            ))
            .header("X-Pnos-Token", token)
            .json(&req)
            .send()
            .await
            .map_err(|e| PnosSdkError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            warn!("心跳失败: HTTP {}", resp.status());
        }
        Ok(())
    }
}

/// Pnos 应用构建器
pub struct PnosAppBuilder {
    app_id: String,
    version: String,
    port: u16,
    runtime_url: Option<String>,
    health_check_path: String,
    web_path: Option<String>,
    dependencies: Vec<String>,
    auto_heartbeat: bool,
}

impl PnosAppBuilder {
    fn new(app_id: impl Into<String>) -> Self {
        Self {
            app_id: app_id.into(),
            version: "0.1.0".to_string(),
            port: 18080,
            runtime_url: None,
            health_check_path: "/health".to_string(),
            web_path: Some("/".to_string()),
            dependencies: Vec::new(),
            auto_heartbeat: true,
        }
    }

    /// 设置版本
    pub fn version(mut self, v: impl Into<String>) -> Self {
        self.version = v.into();
        self
    }

    /// 设置监听端口
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// 设置 pnos-runtime 地址（默认从环境变量 PNOS_RUNTIME_URL 读取）
    pub fn runtime_url(mut self, url: impl Into<String>) -> Self {
        self.runtime_url = Some(url.into());
        self
    }

    /// 设置健康检查路径
    pub fn health_check_path(mut self, path: impl Into<String>) -> Self {
        self.health_check_path = path.into();
        self
    }

    /// 设置 Web UI 路径
    pub fn web_path(mut self, path: impl Into<String>) -> Self {
        self.web_path = Some(path.into());
        self
    }

    /// 添加依赖
    pub fn dependency(mut self, dep: impl Into<String>) -> Self {
        self.dependencies.push(dep.into());
        self
    }

    /// 禁用自动心跳
    pub fn no_auto_heartbeat(mut self) -> Self {
        self.auto_heartbeat = false;
        self
    }

    /// 初始化应用：读取配置 → 注册 → 启动心跳
    pub async fn init(self) -> Result<PnosApp> {
        let config = SdkConfig::load(self.runtime_url)?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| PnosSdkError::Network(e.to_string()))?;

        let app = PnosApp {
            app_id: self.app_id.clone(),
            version: self.version.clone(),
            config: Arc::new(config),
            token: Arc::new(tokio::sync::RwLock::new(None)),
            http,
        };

        // 注册
        let register_req = AppRegisterRequest {
            id: self.app_id.clone(),
            name: self.app_id.clone(),
            version: self.version.clone(),
            port: self.port,
            health_check_path: self.health_check_path,
            web_path: self.web_path,
            dependencies: self.dependencies,
        };

        let resp = app
            .http
            .post(format!(
                "{}/api/v1/apps/register",
                app.config.runtime_url
            ))
            .json(&register_req)
            .send()
            .await
            .map_err(|e| PnosSdkError::Network(format!("注册失败: {}", e)))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(PnosSdkError::Register(format!(
                "HTTP {}: {}",
                status, text
            )));
        }

        let body: pnos::response::ApiResponse<pnos::registry::AppRegisterResponse> = resp
            .json()
            .await
            .map_err(|e| PnosSdkError::Network(format!("解析注册响应失败: {}", e)))?;

        let token = body
            .data
            .map(|d| d.token)
            .ok_or_else(|| PnosSdkError::Register("注册响应无 token".to_string()))?;

        info!(
            "应用注册成功: {} (port={})",
            app.app_id, self.port
        );
        *app.token.write().await = Some(token);

        // 启动心跳
        if self.auto_heartbeat {
            let app_clone = app.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(15)).await;
                    if let Err(e) = app_clone.heartbeat(AppStatus::Running).await {
                        error!("心跳异常: {}", e);
                    }
                }
            });
        }

        Ok(app)
    }
}
