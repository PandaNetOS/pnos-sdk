//! PandaNetOS 统一通信 SDK（v1.0）
//!
//! 引入后零通信代码：自动注册、心跳、注销、服务发现、认证、事件订阅、健康检查。
//! 应用与 Agent 共用同一套 API，通过 component_type 区分。
//!
//! # 快速开始（零通信代码）
//!
//! ```rust,no_run
//! use pnos_comm::PnosApp;
//! use axum::{routing::get, Json};
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     PnosApp::builder("my-app")
//!         .version("1.0.0")
//!         .port(18090)
//!         // 只写业务路由
//!         .route("/api/v1/hello", get(|| async { Json(json!({"msg": "hi"})) }))
//!         // 可选：事件回调
//!         .on_event("component.status_changed", |evt| async move {
//!             println!("状态变更: {:?}", evt.payload);
//!         })
//!         // 一键启动：注册+心跳+服务器+认证+健康检查+WS+注销 全自动
//!         .run()
//!         .await
//! }
//! ```
//!
//! # Agent 模式
//!
//! ```rust,no_run
//! use pnos_comm::PnosApp;
//! use pnos::component::ComponentType;
//!
//! # async fn example() -> anyhow::Result<()> {
//! PnosApp::builder("spde-001")
//!     .version("0.6.2")
//!     .port(9000)
//!     .component_type(ComponentType::Agent)
//!     .capability("download.http")
//!     .capability("download.bt")
//!     .run()
//!     .await
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
pub use client::ComponentClient;
pub use config::SdkConfig;
pub use error::{Result, SdkError};
pub use events::EventDispatcher;
pub use health::HealthBuilder;
pub use lifecycle::LifecycleManager;
pub use registry::RuntimeClient;
pub use server::AppServer;
pub use ws::WsClient;

// 向后兼容别名
#[allow(dead_code)]
pub type AppClient = ComponentClient;

use std::sync::Arc;

use axum::routing::MethodRouter;
use pnos::component::{ComponentStatus, ComponentType};
use pnos::registry::ComponentRegisterRequest;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::discovery::DiscoveryCache;
use crate::events::EventHandler;

/// Pnos 组件主入口（所有组件的组合体，应用与 Agent 共用）
#[derive(Clone)]
pub struct PnosApp {
    /// 组件 ID
    pub app_id: String,
    /// 组件版本
    pub version: String,
    /// 组件类型
    pub component_type: ComponentType,
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

    /// 调用其他组件（自动发现地址 + 自动带 Token）
    pub fn call(&self, component_id: &str) -> ComponentClient {
        ComponentClient::new(
            self.discovery.clone(),
            self.token.clone(),
            self.http.clone(),
            self.config.clone(),
            component_id,
        )
    }

    /// 获取当前 Token
    pub async fn token(&self) -> Option<String> {
        self.token.read().await.clone()
    }

    /// 手动发送心跳（应用场景）
    pub async fn heartbeat(&self, status: ComponentStatus) -> crate::error::Result<()> {
        self.runtime.heartbeat(status, 0.0, 0, 0).await?;
        Ok(())
    }

    /// 手动发送心跳（Agent 场景，含任务统计）
    pub async fn heartbeat_agent(
        &self,
        status: ComponentStatus,
        load: f32,
        active_tasks: u32,
        bytes_downloaded: u64,
    ) -> crate::error::Result<()> {
        self.runtime
            .heartbeat(status, load, active_tasks, bytes_downloaded)
            .await?;
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

/// Pnos 组件构建器
pub struct PnosAppBuilder {
    app_id: String,
    version: String,
    component_type: ComponentType,
    port: u16,
    runtime_url: Option<String>,
    health_check_path: String,
    web_path: Option<String>,
    dependencies: Vec<String>,
    capabilities: Vec<String>,
    region: Option<String>,
    hostname: Option<String>,
    platform: Option<String>,
    arch: Option<String>,
    max_concurrent: Option<u32>,
    max_bandwidth_bps: Option<u64>,
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
            version: "1.0.0".to_string(),
            component_type: ComponentType::App,
            port: 18080,
            runtime_url: None,
            health_check_path: "/health".to_string(),
            web_path: Some("/".to_string()),
            dependencies: Vec::new(),
            capabilities: Vec::new(),
            region: None,
            hostname: None,
            platform: None,
            arch: None,
            max_concurrent: None,
            max_bandwidth_bps: None,
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

    /// 设置组件类型（应用/Agent/运行时/主控）
    pub fn component_type(mut self, component_type: ComponentType) -> Self {
        self.component_type = component_type;
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

    /// 添加能力标签（Agent 用）
    pub fn capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.push(capability.into());
        self
    }

    /// 设置区域标识
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// 设置 Agent 主机信息
    pub fn host_info(
        mut self,
        hostname: impl Into<String>,
        platform: impl Into<String>,
        arch: impl Into<String>,
    ) -> Self {
        self.hostname = Some(hostname.into());
        self.platform = Some(platform.into());
        self.arch = Some(arch.into());
        self
    }

    /// 设置最大并发与带宽（Agent 用）
    pub fn limits(mut self, max_concurrent: u32, max_bandwidth_bps: u64) -> Self {
        self.max_concurrent = Some(max_concurrent);
        self.max_bandwidth_bps = Some(max_bandwidth_bps);
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

    /// 注册事件回调（支持前缀匹配，如 "task.*" / "component.*"）
    pub fn on_event<F, Fut>(mut self, pattern: impl Into<String>, handler: F) -> Self
    where
        F: Fn(pnos::events::WsMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let eh: EventHandler = Arc::new(move |msg| Box::pin(handler(msg)));
        self.event_handlers.push((pattern.into(), eh));
        self
    }

    /// 初始化组件（注册 + 心跳 + WS + 事件分发，不启动服务器）
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

        // 11. 注册到 runtime（统一组件注册请求）
        let register_req = ComponentRegisterRequest {
            id: self.app_id.clone(),
            name: self.app_id.clone(),
            version: self.version.clone(),
            component_type: self.component_type,
            port: self.port,
            serve_host: None,
            serve_port: None,
            capabilities: self.capabilities.clone(),
            region: self.region.clone(),
            hostname: self.hostname.clone(),
            platform: self.platform.clone(),
            arch: self.arch.clone(),
            labels: Vec::new(),
            max_concurrent: self.max_concurrent,
            max_bandwidth_bps: self.max_bandwidth_bps,
            health_check_path: self.health_check_path.clone(),
            web_path: self.web_path.clone(),
            dependencies: self.dependencies.clone(),
        };

        let register_resp = runtime.register(&register_req).await?;
        info!(
            "组件注册成功: {} (type={}, port={}, token={}...)",
            self.app_id,
            self.component_type,
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
                    if let Err(e) = runtime_clone
                        .heartbeat(ComponentStatus::Running, 0.0, 0, 0)
                        .await
                    {
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
                info!("正在注销组件...");
                ws.shutdown();
                if let Err(e) = runtime.unregister().await {
                    error!("注销失败: {}", e);
                } else {
                    info!("组件已注销");
                }
            }
        });

        Ok(PnosApp {
            app_id: self.app_id,
            version: self.version,
            component_type: self.component_type,
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

        info!("组件已完全关闭");
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
