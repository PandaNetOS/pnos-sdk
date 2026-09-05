//! SDK 配置

use std::path::PathBuf;

/// SDK 配置
#[derive(Debug, Clone)]
pub struct SdkConfig {
    /// pnos-runtime 地址
    pub runtime_url: String,
    /// 应用数据目录
    pub data_dir: PathBuf,
    /// 媒体目录
    pub media_dir: PathBuf,
}

impl SdkConfig {
    /// 从环境变量加载
    pub fn load(runtime_url_override: Option<String>) -> Result<Self, crate::error::PnosSdkError> {
        let runtime_url = runtime_url_override
            .or_else(|| std::env::var("PNOS_RUNTIME_URL").ok())
            .unwrap_or_else(|| "http://127.0.0.1:80".to_string());

        let data_dir = std::env::var("PNOS_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/pnos/data/apps"));

        let media_dir = std::env::var("PNOS_MEDIA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/pnos/media"));

        Ok(Self {
            runtime_url,
            data_dir,
            media_dir,
        })
    }
}
