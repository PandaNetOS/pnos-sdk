//! PandaNetOS 统一通信 SDK
//!
//! 引入后零通信代码：自动注册、心跳、注销、服务发现、认证、事件订阅、健康检查。
//!
//! # 快速开始（零通信代码）
//!
//! ```rust,no_run
//! use pnos_sdk::PnosApp;
//! use axum::{routing::get, Json};
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     PnosApp::builder("my-app")
//!         .version("0.1.0")
//!         .port(18090)
//!         // 只写业务路由
//!         .route("/api/v1/hello", get(|| async { Json(json!({"msg": "hi"})) }))
//!         // 可选：事件回调
//!         .on_event("app.status_changed", |evt| async move {
//!             println!("状态变更: {:?}", evt.payload);
//!         })
//!         // 一键启动：注册+心跳+服务器+认证+健康检查+WS+注销 全自动
//!         .run()
//!         .await
//! }
//! ```
//!
//! # 高级用法（嵌入自己的服务器）
//!
//! ```rust,no_run
//! use pnos_sdk::PnosApp;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let app = PnosApp::builder("my-app")
//!     .version("0.1.0")
//!     .init()
//!     .await?;
//!
//! // 拿到 Router 自己组装
//! let router = app.into_router();
//! // ... 自行启动服务器
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod config;
pub mod discovery;
pub mod error;
pub mod events;
pub mod health;
pub mod lifecycle;
pub mod logging;
pub mod middleware;
pub mod registry;
pub mod server;
pub mod ws;

// ---- 最常用类型 re-export ----
pub use client::AppClient;
pub use config::SdkConfig;
pub use error::{Result, SdkError};
pub use events::EventDispatcher;
pub use health::HealthBuilder;
pub use lifecycle::LifecycleManager;
pub use registry::RuntimeClient;
pub use server::AppServer;
pub use ws::WsClient;

use std::sync::Arc;

use axum::routing::MethodRouter;
use pnos::app::AppStatus;
use pnos::registry::AppRegisterRequest;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::discovery::DiscoveryCache;
use crate::events::EventHandler;

/// Pnos 应用主入口（所有组件的组合体）
#[derive(Clone)]
pub struct PnosApp {
    /// 应用 ID
    pub app_id: String,
    /// 应用版本
    pub version: String,
    /// 配置
    pub config: Arc<SdkConfig>,
    /// runtime API 客户端
    pub runtime: RuntimeClient,
    /// 服务发现（带缓存）
    pub discovery: DiscoveryCache,
    /// WebSocket 事件客户端
    pub ws: WsClient,
    /// 事件分发器
    pub events: EventDispatcher,
    /// 生命周期管理
    pub lifecycle: LifecycleManager,
    /// 内嵌 Web 服务器
    pub server: AppServer,
    /// 认证 Token（注册后获得）
    token: Arc<RwLock<Option<String>>>,
    /// HTTP 客户端
    http: reqwest::Client,
}

impl PnosApp {
    /// 创建构建器
    pub fn builder(app_id: impl Into<String>) -> PnosAppBuilder {
        PnosAppBuilder::new(app_id)
    }

    /// 调用其他应用（自动发现地址 + 自动带 Token）
    pub fn call(&self, app_id: &str) -> AppClient {
        AppClient::new(
            self.discovery.clone(),
            self.token.clone(),
            self.http.clone(),
            self.config.clone(),
            app_id,
        )
    }

    /// 获取当前 Token
    pub async fn token(&self) -> Option<String> {
        self.token.read().await.clone()
    }

    /// 手动发送心跳
    pub async fn heartbeat(&self, status: AppStatus) -> crate::error::Result<()> {
        self.runtime.heartbeat(status, None).await?;
        Ok(())
    }

    /// 触发优雅关闭
    pub async fn shutdown(&self) {
        self.lifecycle.shutdown().await;
    }

    /// 获取内嵌服务器（高级用户可自行操作）
    pub fn server(&self) -> &AppServer {
        &self.server
    }

    /// 获取内部 Router（高级用户可自行组装后启动）
    pub fn into_router(self) -> axum::Router {
        self.server.into_router()
    }
}

// ---------------------------------------------------------------------------
// 构建器
// ---------------------------------------------------------------------------

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
    /// 收集的业务路由
    routes: Vec<(String, MethodRouter)>,
    /// 收集的事件回调
    event_handlers: Vec<(String, EventHandler)>,
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
            routes: Vec::new(),
            event_handlers: Vec::new(),
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

    /// 添加业务路由
    pub fn route(mut self, path: &str, method_router: MethodRouter) -> Self {
        self.routes.push((path.to_string(), method_router));
        self
    }

    /// 注册事件回调（支持前缀匹配，如 "app.*"）
    pub fn on_event<F, Fut>(mut self, pattern: impl Into<String>, handler: F) -> Self
    where
        F: Fn(pnos::events::WsMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let eh: EventHandler = Arc::new(move |msg| Box::pin(handler(msg)));
        self.event_handlers.push((pattern.into(), eh));
        self
    }

    /// 初始化应用（注册 + 心跳 + WS + 事件分发，不启动服务器）
    ///
    /// 高级用户用此方法获取 [`PnosApp`] 后自行组装服务器。
    pub async fn init(self) -> crate::error::Result<PnosApp> {
        // 1. 初始化日志
        crate::logging::init_logging();

        // 2. 加载配置
        let mut config = SdkConfig::from_env(&self.app_id)?;
        config.version = self.version.clone();
        config.port = self.port;
        config.health_check_path = self.health_check_path.clone();
        config.web_path = self.web_path.clone();
        config.dependencies = self.dependencies.clone();
        config.auto_heartbeat = self.auto_heartbeat;
        if let Some(url) = &self.runtime_url {
            config.runtime_url = url.clone();
        }
        let config = Arc::new(config);

        // 3. 创建 HTTP 客户端
        let http = reqwest::Client::builder()
            .timeout(config.http_timeout)
            .build()
            .map_err(|e| crate::error::SdkError::Network(format!("HTTP 客户端构建失败: {e}")))?;

        // 4. 创建 token 存储
        let token = Arc::new(RwLock::new(None));

        // 5. 创建 runtime 客户端
        let runtime = RuntimeClient::new(config.clone(), http.clone(), token.clone());

        // 6. 创建服务发现缓存
        let discovery = DiscoveryCache::new(runtime.clone(), config.clone());

        // 7. 创建 WebSocket 客户端（先创建，注册后再 start）
        let (ws, ws_rx) = WsClient::new(config.clone(), token.clone());

        // 8. 创建事件分发器
        let events = EventDispatcher::new();

        // 9. 创建生命周期管理
        let lifecycle = LifecycleManager::new();

        // 10. 创建内嵌服务器（添加业务路由）
        let mut server = AppServer::new(self.port, &self.version, token.clone());
        for (path, method_router) in self.routes {
            server = server.route(&path, method_router);
        }

        // 11. 注册到 runtime
        let register_req = AppRegisterRequest {
            id: self.app_id.clone(),
            name: self.app_id.clone(),
            version: self.version.clone(),
            port: self.port,
            health_check_path: self.health_check_path.clone(),
            web_path: self.web_path.clone(),
            dependencies: self.dependencies.clone(),
        };

        let register_resp = runtime.register(&register_req).await?;
        info!(
            "应用注册成功: {} (port={}, token={}...)",
            self.app_id,
            self.port,
            &register_resp.token[..register_resp.token.len().min(8)]
        );
        runtime.set_token(register_resp.token).await;

        // 12. 启动自动心跳
        if self.auto_heartbeat {
            let runtime_clone = runtime.clone();
            let interval = config.heartbeat_interval;
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    if let Err(e) = runtime_clone.heartbeat(AppStatus::Running, None).await {
                        warn!("心跳异常: {}", e);
                    }
                }
            });
        }

        // 13. 启动 WebSocket 客户端
        ws.start();

        // 14. 启动事件分发器（从 WS 接收消息）
        events.clone().start(ws_rx);

        // 15. 注册事件回调
        for (pattern, handler) in self.event_handlers {
            // 订阅事件
            ws.subscribe(pattern.clone()).await;
            // 注册回调
            let events_clone = events.clone();
            tokio::spawn(async move {
                events_clone.on_direct(pattern, handler).await;
            });
        }

        // 16. 注册关闭回调（注销 + 关闭 WS）
        let runtime_clone = runtime.clone();
        let ws_clone = ws.clone();
        lifecycle.on_shutdown(move || {
            let runtime = runtime_clone.clone();
            let ws = ws_clone.clone();
            async move {
                info!("正在注销应用...");
                ws.shutdown();
                if let Err(e) = runtime.unregister().await {
                    error!("注销失败: {}", e);
                } else {
                    info!("应用已注销");
                }
            }
        });

        Ok(PnosApp {
            app_id: self.app_id,
            version: self.version,
            config,
            runtime,
            discovery,
            ws,
            events,
            lifecycle,
            token,
            http,
            server,
        })
    }

    /// 一键启动（初始化 + 启动服务器 + 等待关闭）
    ///
    /// 这是最常用的入口：调用后自动完成注册、心跳、服务器启动、
    /// 健康检查、认证中间件、WebSocket 事件连接，
    /// 并在 Ctrl+C 时自动注销并关闭。
    pub async fn run(self) -> anyhow::Result<()> {
        let app = self.init().await?;
        let lifecycle = app.lifecycle.clone();
        let server = app.server.clone();

        // 启动服务器（后台）
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                error!("服务器错误: {}", e);
            }
        });

        // 等待关闭信号
        lifecycle.wait_for_shutdown().await;

        // 等待服务器退出
        let _ = server_handle.await;

        info!("应用已完全关闭");
        Ok(())
    }
}

// 为 EventDispatcher 添加一个直接注册 handler 的方法（避免 on() 的 spawn 开销）
impl EventDispatcher {
    /// 直接注册事件回调（内部用，不 spawn）
    pub(crate) async fn on_direct(&self, pattern: String, handler: EventHandler) {
        let is_prefix = pattern.ends_with('*');
        let listener = crate::events::Listener {
            pattern,
            is_prefix,
            handler,
        };
        self.listeners.write().await.push(listener);
    }
}
