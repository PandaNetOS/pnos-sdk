//! SDK 统一错误类型
//!
//! 包装 [`pnos::error::PnosError`]，额外提供 SDK 层错误
//! （网络、组件未找到、WebSocket 等），并实现 [`axum::response::IntoResponse`]
//! 让应用端用 `?` 传播后自动序列化为标准 [`pnos::response::ApiResponse`]。

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use pnos::error::{ErrorCode, PnosError};
use pnos::response::ApiResponse;
use thiserror::Error;

/// SDK 统一错误
#[derive(Debug, Error)]
pub enum SdkError {
    /// 业务错误（对齐 pnos 错误码）
    #[error("{0}")]
    Business(#[from] PnosError),

    /// 网络错误
    #[error("网络错误: {0}")]
    Network(String),

    /// 组件未找到
    #[error("组件未找到: {0}")]
    ComponentNotFound(String),

    /// 组件不可达
    #[error("组件不可达: {0}")]
    ComponentUnreachable(String),

    /// 认证失败
    #[error("认证失败: {0}")]
    Auth(String),

    /// WebSocket 错误
    #[error("WebSocket 错误: {0}")]
    WebSocket(String),

    /// 序列化错误
    #[error("序列化错误: {0}")]
    Serde(#[from] serde_json::Error),

    /// IO 错误
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    /// 其他错误
    #[error("其他错误: {0}")]
    Other(String),
}

impl SdkError {
    /// 从错误码 + 消息构造
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        SdkError::Business(PnosError::new(code, message))
    }

    /// 获取错误码
    pub fn code(&self) -> ErrorCode {
        match self {
            SdkError::Business(e) => e.code(),
            SdkError::Network(_) => ErrorCode::NetworkError,
            SdkError::ComponentNotFound(_) => ErrorCode::ComponentNotRegistered,
            SdkError::ComponentUnreachable(_) => ErrorCode::ServiceUnavailable,
            SdkError::Auth(_) => ErrorCode::Unauthorized,
            SdkError::WebSocket(_) => ErrorCode::InternalError,
            SdkError::Serde(_) => ErrorCode::InternalError,
            SdkError::Io(_) => ErrorCode::InternalError,
            SdkError::Other(_) => ErrorCode::Unknown,
        }
    }

    /// 获取 HTTP 状态码
    pub fn http_status(&self) -> StatusCode {
        match self {
            SdkError::Business(e) => StatusCode::from_u16(e.code().http_status())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            SdkError::Network(_) => StatusCode::BAD_GATEWAY,
            SdkError::ComponentNotFound(_) => StatusCode::NOT_FOUND,
            SdkError::ComponentUnreachable(_) => StatusCode::BAD_GATEWAY,
            SdkError::Auth(_) => StatusCode::UNAUTHORIZED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<reqwest::Error> for SdkError {
    fn from(e: reqwest::Error) -> Self {
        SdkError::Network(e.to_string())
    }
}

impl From<url::ParseError> for SdkError {
    fn from(e: url::ParseError) -> Self {
        SdkError::Other(format!("URL 解析错误: {e}"))
    }
}

/// 自动转 axum 响应：错误码 + 消息包装成 ApiResponse
impl IntoResponse for SdkError {
    fn into_response(self) -> Response {
        let status = self.http_status();
        let code = self.code();
        let message = self.to_string();
        let body: ApiResponse<()> = ApiResponse {
            code: code.code(),
            message,
            data: None,
            request_id: None,
        };
        (status, axum::Json(body)).into_response()
    }
}

/// SDK Result 别名
pub type Result<T> = std::result::Result<T, SdkError>;

#[cfg(test)]
mod tests {
    use super::*;
    use pnos::error::ErrorCode;

    #[test]
    fn test_error_code_mapping() {
        let err = SdkError::new(ErrorCode::ComponentNotRegistered, "test");
        assert_eq!(err.code(), ErrorCode::ComponentNotRegistered);
        assert_eq!(err.http_status(), axum::http::StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_network_error_code() {
        let err = SdkError::Network("timeout".to_string());
        assert_eq!(err.code(), ErrorCode::NetworkError);
        assert_eq!(err.http_status(), axum::http::StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn test_auth_error_code() {
        let err = SdkError::Auth("no token".to_string());
        assert_eq!(err.code(), ErrorCode::Unauthorized);
        assert_eq!(err.http_status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_component_not_found() {
        let err = SdkError::ComponentNotFound("spde-001".to_string());
        assert_eq!(err.code(), ErrorCode::ComponentNotRegistered);
        assert_eq!(err.http_status(), axum::http::StatusCode::NOT_FOUND);
        assert!(err.to_string().contains("spde-001"));
    }

    #[test]
    fn test_from_reqwest_error() {
        let parse_err = url::Url::parse("not a url").unwrap_err();
        let err: SdkError = parse_err.into();
        assert!(matches!(err, SdkError::Other(_)));
    }

    #[test]
    fn test_into_response() {
        let err = SdkError::ComponentNotFound("not found".to_string());
        let resp = err.into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
    }
}
