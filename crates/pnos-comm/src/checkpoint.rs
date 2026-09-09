//! 检查点持久化存储
//!
//! 有状态组件（如下载任务、事件消费 offset）可以用检查点保存状态，
//! 进程崩溃重启后从最近检查点恢复，避免从头开始。
//!
//! 存储后端：SQLite（WAL 模式，崩溃安全），路径 `$PNOS_DATA_DIR/checkpoints.db`。
//!
//! # 示例
//!
//! ```rust,no_run
//! use pnos_comm::PnosApp;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Clone)]
//! struct DownloadState {
//!     tasks: Vec<String>,
//!     cursor: u64,
//! }
//!
//! # async fn example(app: &PnosApp) -> anyhow::Result<()> {
//! // 保存状态
//! let state = DownloadState { tasks: vec!["task1".into()], cursor: 42 };
//! app.save_checkpoint("download_state", &state).await?;
//!
//! // 恢复状态
//! if let Some(restored) = app.load_checkpoint::<DownloadState>("download_state").await? {
//!     println!("恢复状态: cursor={}", restored.cursor);
//! }
//! # Ok(())
//! # }
//! ```

use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::debug;

use crate::error::{Result, SdkError};

/// 检查点存储（SQLite 后端）
#[derive(Clone)]
pub struct CheckpointStore {
    db: Arc<Mutex<Connection>>,
}

impl CheckpointStore {
    /// 打开检查点数据库（不存在则创建）
    pub fn open(path: &Path) -> Result<Self> {
        // 确保父目录存在
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| SdkError::Other(format!("创建检查点目录失败: {e}")))?;
            }
        }

        let conn = Connection::open(path)
            .map_err(|e| SdkError::Other(format!("打开检查点数据库失败: {e}")))?;

        // WAL 模式（崩溃安全，读写并发更好）
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS checkpoints (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL,
                 updated_at INTEGER NOT NULL
             );",
        )
        .map_err(|e| SdkError::Other(format!("初始化检查点数据库失败: {e}")))?;

        debug!("检查点数据库已打开: {:?}", path);
        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
        })
    }

    /// 保存检查点（upsert，key 已存在则覆盖）
    pub async fn save<T: Serialize + Send + 'static>(&self, key: &str, value: &T) -> Result<()> {
        let key = key.to_string();
        let value_json = serde_json::to_string(value)
            .map_err(|e| SdkError::Other(format!("序列化检查点失败: {e}")))?;
        let now = chrono::Utc::now().timestamp();

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let conn = db.blocking_lock();
            conn.execute(
                "INSERT INTO checkpoints (key, value, updated_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                rusqlite::params![key, value_json, now],
            )
            .map_err(|e| SdkError::Other(format!("保存检查点失败: {e}")))?;
            Ok::<(), SdkError>(())
        })
        .await
        .map_err(|e| SdkError::Other(format!("检查点任务失败: {e}")))?
    }

    /// 加载检查点（不存在返回 None）
    pub async fn load<T: DeserializeOwned + Send + 'static>(&self, key: &str) -> Result<Option<T>> {
        let key = key.to_string();
        let db = self.db.clone();

        let result = tokio::task::spawn_blocking(move || {
            let conn = db.blocking_lock();
            let mut stmt = conn
                .prepare("SELECT value FROM checkpoints WHERE key = ?1")
                .map_err(|e| SdkError::Other(format!("查询检查点失败: {e}")))?;
            let value: Option<String> = stmt
                .query_row(rusqlite::params![key], |row| row.get(0))
                .ok();
            Ok::<Option<String>, SdkError>(value)
        })
        .await
        .map_err(|e| SdkError::Other(format!("检查点任务失败: {e}")))??;

        match result {
            Some(json) => {
                let value: T = serde_json::from_str(&json)
                    .map_err(|e| SdkError::Other(format!("反序列化检查点失败: {e}")))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// 删除检查点
    pub async fn delete(&self, key: &str) -> Result<()> {
        let key = key.to_string();
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let conn = db.blocking_lock();
            conn.execute(
                "DELETE FROM checkpoints WHERE key = ?1",
                rusqlite::params![key],
            )
            .map_err(|e| SdkError::Other(format!("删除检查点失败: {e}")))?;
            Ok::<(), SdkError>(())
        })
        .await
        .map_err(|e| SdkError::Other(format!("检查点任务失败: {e}")))?
    }

    /// 列出所有检查点 key
    pub async fn list_keys(&self) -> Result<Vec<String>> {
        let db = self.db.clone();

        let keys = tokio::task::spawn_blocking(move || {
            let conn = db.blocking_lock();
            let mut stmt = conn
                .prepare("SELECT key FROM checkpoints ORDER BY updated_at DESC")
                .map_err(|e| SdkError::Other(format!("查询检查点列表失败: {e}")))?;
            let keys: Vec<String> = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| SdkError::Other(format!("查询检查点列表失败: {e}")))?
                .filter_map(|r| r.ok())
                .collect();
            Ok::<Vec<String>, SdkError>(keys)
        })
        .await
        .map_err(|e| SdkError::Other(format!("检查点任务失败: {e}")))??;

        Ok(keys)
    }
}

/// 在 PnosApp 上暴露检查点方法（通过扩展 trait）
#[allow(async_fn_in_trait)]
pub trait CheckpointExt {
    /// 保存检查点
    async fn save_checkpoint<T: Serialize + Send + 'static>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<()>;
    /// 加载检查点
    async fn load_checkpoint<T: DeserializeOwned + Send + 'static>(
        &self,
        key: &str,
    ) -> Result<Option<T>>;
    /// 删除检查点
    async fn delete_checkpoint(&self, key: &str) -> Result<()>;
}

// 注意：PnosApp 的 checkpoint 字段在 lib.rs 中初始化，
// 这里只提供存储实现，PnosApp 的方法在 lib.rs 中实现。
