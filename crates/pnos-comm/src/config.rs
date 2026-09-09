//! SDK 配置
//!
//! 包装 [`pnos::config::PnosConfig`]（标准全局配置），
//! 额外提供 SDK 运行时参数（runtime 地址、数据目录、心跳间隔等）。

use std::path::PathBuf;
use std::time::Duration;

use pnos::config::PnosConfig;
use tracing::debug;

/// SDK 完整配置
#[derive(Debug, Clone)]
pub struct SdkConfig {
    /// 标准全局配置（数据目录、媒体目录、端口、日志级别等）
    pub pnos: PnosConfig,

    /// pnos-runtime 地址（基础容器内默认 http://127.0.0.1:8080）
    pub runtime_url: String,

    /// 数据根目录（$PNOS_DATA_DIR，默认 /data）
    pub data_dir: PathBuf,

    /// component_id 持久化文件（$DATA_DIR/component_id）
    pub component_id_file: PathBuf,

    /// 检查点数据库路径（$DATA_DIR/checkpoints.db）
    pub checkpoint_db: PathBuf,

    /// 应用唯一标识
    pub app_id: String,
    /// 应用版本
    pub version: String,
    /// 应用监听端口
    pub port: u16,
    /// 健康检查路径
    pub health_check_path: String,
    /// Web UI 根路径（None 表示无 UI）
    pub web_path: Option<String>,
    /// 依赖的应用 id 列表
    pub dependencies: Vec<String>,

    /// 是否启用自动心跳
    pub auto_heartbeat: bool,
    /// 心跳间隔
    pub heartbeat_interval: Duration,
    /// 服务发现缓存 TTL
    pub discovery_cache_ttl: Duration,
    /// HTTP 请求超时
    pub http_timeout: Duration,
    /// 调用重试次数
    pub call_retries: u32,
    /// 优雅关闭超时（默认 10s，对应 docker stop 超时）
    pub graceful_shutdown_timeout: Duration,
    /// 注册重试最大时长（默认 30s）
    pub register_retry_timeout: Duration,
}

impl SdkConfig {
    /// 从环境变量加载标准配置，runtime 地址从 `PNOS_RUNTIME_URL` 读取
    pub fn from_env(app_id: impl Into<String>) -> crate::error::Result<Self> {
        let pnos = PnosConfig::load().map_err(crate::error::SdkError::Business)?;

        let runtime_url = std::env::var("PNOS_RUNTIME_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());

        let data_dir = std::env::var("PNOS_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/data"));

        let component_id_file = data_dir.join("component_id");
        let checkpoint_db = data_dir.join("checkpoints.db");

        Ok(Self {
            pnos,
            runtime_url,
            data_dir,
            component_id_file,
            checkpoint_db,
            app_id: app_id.into(),
            version: "0.1.0".to_string(),
            port: 18080,
            health_check_path: "/health".to_string(),
            web_path: Some("/".to_string()),
            dependencies: Vec::new(),
            auto_heartbeat: true,
            heartbeat_interval: Duration::from_secs(15),
            discovery_cache_ttl: Duration::from_secs(30),
            http_timeout: Duration::from_secs(30),
            call_retries: 2,
            graceful_shutdown_timeout: Duration::from_secs(10),
            register_retry_timeout: Duration::from_secs(30),
        })
    }

    /// runtime 的 API 基础 URL（如 http://127.0.0.1:8080/api/v1）
    pub fn api_base(&self) -> String {
        format!("{}/api/v1", self.runtime_url.trim_end_matches('/'))
    }

    /// runtime 的 WebSocket URL（如 ws://127.0.0.1:8080/api/v1/ws）
    pub fn ws_url(&self) -> String {
        let base = self.runtime_url.trim_end_matches('/');
        let ws_base = if base.starts_with("https://") {
            base.replacen("https://", "wss://", 1)
        } else {
            base.replacen("http://", "ws://", 1)
        };
        format!("{ws_base}/api/v1/ws")
    }

    /// 确保数据目录存在
    pub fn ensure_data_dir(&self) -> crate::error::Result<()> {
        if !self.data_dir.exists() {
            std::fs::create_dir_all(&self.data_dir)
                .map_err(|e| crate::error::SdkError::Other(format!("创建数据目录失败: {e}")))?;
            debug!("创建数据目录: {:?}", self.data_dir);
        }
        Ok(())
    }

    /// 加载持久化的 component_id（重启后用原 ID 重注册）
    pub fn load_component_id(&self) -> Option<String> {
        std::fs::read_to_string(&self.component_id_file)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// 保存 component_id 到持久化文件
    pub fn save_component_id(&self, component_id: &str) -> crate::error::Result<()> {
        self.ensure_data_dir()?;
        std::fs::write(&self.component_id_file, component_id)
            .map_err(|e| crate::error::SdkError::Other(format!("保存 component_id 失败: {e}")))?;
        Ok(())
    }
}
