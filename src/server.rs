//! 内嵌 axum 服务器
//!
//! 自动挂载健康检查端点、认证中间件，开发者只需添加业务路由。
//! 高级用户可通过 [`AppServer::into_router`] 获取 Router 自行组装。

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio::sync::RwLock;
use tracing::info;

use crate::health::HealthBuilder;
use crate::middleware::{auth_middleware, AuthState};

/// 服务器共享状态
#[derive(Clone)]
struct ServerState {
    health_builder: Arc<RwLock<HealthBuilder>>,
}

/// 内嵌 Web 服务器
#[derive(Clone)]
pub struct AppServer {
    router: Router,
    addr: SocketAddr,
    health_builder: Arc<RwLock<HealthBuilder>>,
}

impl AppServer {
    /// 创建服务器（自动挂载 /health + 认证中间件）
    pub fn new(port: u16, version: impl Into<String>, token: Arc<RwLock<Option<String>>>) -> Self {
        let health_builder = Arc::new(RwLock::new(HealthBuilder::new(version)));
        let state = ServerState {
            health_builder: health_builder.clone(),
        };

        let auth_state = AuthState::new(token);

        let router = Router::new()
            // 健康检查端点（在认证中间件之前，白名单放行）
            .route("/health", get(health_handler))
            .with_state(state)
            // 认证中间件（白名单路径自动放行）
            .layer(middleware::from_fn_with_state(auth_state, auth_middleware));

        let addr = SocketAddr::from(([0, 0, 0, 0], port));

        Self {
            router,
            addr,
            health_builder,
        }
    }

    /// 添加业务路由
    pub fn route(mut self, path: &str, method_router: axum::routing::MethodRouter) -> Self {
        self.router = self.router.route(path, method_router);
        self
    }

    /// 合并另一个 Router
    pub fn merge(mut self, other: Router) -> Self {
        self.router = self.router.merge(other);
        self
    }

    /// 获取健康检查构建器（可用于更新依赖状态）
    pub fn health_builder(&self) -> Arc<RwLock<HealthBuilder>> {
        self.health_builder.clone()
    }

    /// 获取内部 Router（高级用户可自行组装后启动）
    pub fn into_router(self) -> Router {
        self.router
    }

    /// 获取监听地址
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// 启动服务器（阻塞直到关闭）
    pub async fn serve(self) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        info!("应用服务器启动: http://{}", self.addr);
        axum::serve(listener, self.router).await
    }
}

/// 健康检查处理函数
async fn health_handler(State(state): State<ServerState>) -> impl IntoResponse {
    let builder = state.health_builder.read().await;
    let response = builder.build();
    (StatusCode::OK, axum::Json(response)).into_response()
}
