//! SDK 错误类型

use thiserror::Error;

pub type Result<T> = std::result::Result<T, PnosSdkError>;

#[derive(Debug, Error)]
pub enum PnosSdkError {
    #[error("配置错误: {0}")]
    Config(String),

    #[error("注册失败: {0}")]
    Register(String),

    #[error("认证失败: {0}")]
    Auth(String),

    #[error("应用未找到: {0}")]
    AppNotFound(String),

    #[error("应用不可达: {0}")]
    AppUnreachable(String),

    #[error("网络错误: {0}")]
    Network(String),

    #[error("API 错误: code={code}, message={message}")]
    Api { code: u32, message: String },

    #[error("序列化错误: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("其他错误: {0}")]
    Other(String),
}
