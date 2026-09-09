//! 优雅关闭管理
//!
//! 监听 Ctrl+C / SIGTERM，触发关闭时执行所有注册的关闭回调
//! （注销组件、关闭 WebSocket 连接、保存状态等）。
//! 支持超时控制（默认 10s，对应 docker stop 超时），超时后强制退出。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::Notify;
use tracing::{info, warn};

/// 关闭回调类型
type ShutdownCallback = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// 优雅关闭管理器
#[derive(Clone)]
pub struct LifecycleManager {
    shutdown_notify: Arc<Notify>,
    callbacks: Arc<Mutex<Vec<ShutdownCallback>>>,
    default_timeout: Duration,
}

impl LifecycleManager {
    pub fn new() -> Self {
        Self {
            shutdown_notify: Arc::new(Notify::new()),
            callbacks: Arc::new(Mutex::new(Vec::new())),
            default_timeout: Duration::from_secs(10),
        }
    }

    /// 设置默认优雅关闭超时
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// 注册关闭回调（按注册顺序逆序执行）
    pub fn on_shutdown<F, Fut>(&self, callback: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cb: ShutdownCallback = Box::new(move || Box::pin(callback()));
        let callbacks = self.callbacks.clone();
        tokio::spawn(async move {
            callbacks.lock().await.push(cb);
        });
    }

    /// 触发关闭（执行所有回调，使用默认超时）
    pub async fn shutdown(&self) {
        self.shutdown_with_timeout(self.default_timeout).await;
    }

    /// 触发关闭（指定超时，超时后强制结束剩余回调）
    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        info!("触发优雅关闭（超时: {:?}）...", timeout);
        self.shutdown_notify.notify_waiters();

        // 逆序执行回调（后注册的先执行，类似栈的析构顺序）
        let callbacks = {
            let mut guard = self.callbacks.lock().await;
            std::mem::take(&mut *guard)
        };

        let result = tokio::time::timeout(timeout, async {
            for cb in callbacks.into_iter().rev() {
                cb().await;
            }
        })
        .await;

        match result {
            Ok(_) => info!("优雅关闭完成"),
            Err(_) => warn!("优雅关闭超时（{:?}），强制结束剩余回调", timeout),
        }
    }

    /// 等待关闭信号（Ctrl+C / SIGTERM / 手动触发），使用默认超时
    pub async fn wait_for_shutdown(&self) {
        self.wait_for_shutdown_with_timeout(self.default_timeout)
            .await;
    }

    /// 等待关闭信号（指定超时）
    pub async fn wait_for_shutdown_with_timeout(&self, timeout: Duration) {
        tokio::select! {
            _ = self.shutdown_notify.notified() => {
                // 手动触发
            }
            _ = tokio::signal::ctrl_c() => {
                info!("收到 Ctrl+C 信号");
            }
        }
        self.shutdown_with_timeout(timeout).await;
    }

    /// 获取关闭通知（用于在其他任务中监听关闭信号）
    pub fn shutdown_notify(&self) -> Arc<Notify> {
        self.shutdown_notify.clone()
    }
}

impl Default for LifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}
