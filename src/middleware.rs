//! axum 中间件
//!
//! - [`auth_middleware`]：校验 `X-Pnos-Token`，白名单路径免认证
//! - 错误自动转 [`pnos::response::ApiResponse`]（由 [`crate::error::SdkError`] 的 IntoResponse 实现）

use std::sync::Arc;

use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::sync::RwLock;

use crate::error::SdkError;

/// 认证中间件状态
#[derive(Clone)]
pub struct AuthState {
    /// 本应用的 token（注册后获得）
    pub(crate) token: Arc<RwLock<Option<String>>>,
    /// 免认证路径前缀（如 /health, /api/v1/ws）
    pub(crate) whitelist: Vec<String>,
}

impl AuthState {
    pub fn new(token: Arc<RwLock<Option<String>>>) -> Self {
        Self {
            token,
            whitelist: vec![
                "/health".to_string(),
                "/api/v1/ws".to_string(),
                "/ws".to_string(),
            ],
        }
    }

    /// 添加免认证路径
    pub fn add_whitelist(mut self, path: impl Into<String>) -> Self {
        self.whitelist.push(path.into());
        self
    }

    /// 检查路径是否在白名单中
    fn is_whitelisted(&self, path: &str) -> bool {
        self.whitelist.iter().any(|p| path.starts_with(p))
    }
}

/// 认证中间件
///
/// 白名单路径直接放行；其他路径校验 `X-Pnos-Token` header。
/// token 为空或不匹配返回 401。
pub async fn auth_middleware(
    State(state): State<AuthState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();

    // 白名单路径直接放行
    if state.is_whitelisted(&path) {
        return next.run(req).await;
    }

    // 提取 token
    let provided_token = req
        .headers()
        .get("X-Pnos-Token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let expected_token = state.token.read().await.clone();

    match (provided_token, expected_token) {
        (Some(provided), Some(expected)) if provided == expected => {
            // token 匹配，放行
            next.run(req).await
        }
        (Some(_), Some(_)) => {
            // token 不匹配
            SdkError::new(pnos::error::ErrorCode::TokenInvalid, "无效的认证 Token").into_response()
        }
        (None, Some(_)) => {
            // 未提供 token
            SdkError::new(
                pnos::error::ErrorCode::Unauthorized,
                "缺少 X-Pnos-Token 请求头",
            )
            .into_response()
        }
        (_, None) => {
            // 本应用尚未注册（无 token），放行（开发模式）
            // 或者也可以拒绝，这里选择放行并打日志
            tracing::warn!("应用尚未注册，认证中间件放行所有请求: {}", path);
            next.run(req).await
        }
    }
}
