//! 事件分发器
//!
//! 从 WebSocket 客户端接收 [`pnos::events::WsMessage`]，
//! 按事件类型分发给注册的回调。支持前缀匹配（如 `app.*` 匹配所有 `app.` 开头的事件）。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use pnos::events::WsMessage;

/// 异步事件回调
pub type EventHandler =
    Arc<dyn Fn(WsMessage) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// 已注册的事件监听器
pub(crate) struct Listener {
    /// 事件类型匹配模式（如 "app.status_changed" 或 "app.*"）
    pub(crate) pattern: String,
    /// 是否为前缀匹配（以 "*" 结尾）
    pub(crate) is_prefix: bool,
    /// 回调
    pub(crate) handler: EventHandler,
}

/// 事件分发器
#[derive(Clone)]
pub struct EventDispatcher {
    pub(crate) listeners: Arc<RwLock<Vec<Listener>>>,
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self {
            listeners: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 注册事件回调
    ///
    /// - 精确匹配：`on("app.status_changed", handler)`
    /// - 前缀匹配：`on("app.*", handler)` 匹配所有 `app.` 开头的事件
    /// - 全部事件：`on("*", handler)`
    pub fn on<F, Fut>(&self, pattern: impl Into<String>, handler: F)
    where
        F: Fn(WsMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let pattern = pattern.into();
        let is_prefix = pattern.ends_with('*');
        let listener = Listener {
            pattern: pattern.clone(),
            is_prefix,
            handler: Arc::new(move |msg| Box::pin(handler(msg))),
        };

        // 因为是 async fn，需要在运行时写入
        let listeners = self.listeners.clone();
        tokio::spawn(async move {
            listeners.write().await.push(listener);
        });
        debug!("注册事件监听: {}", pattern);
    }

    /// 分发一条消息到所有匹配的监听器
    pub async fn dispatch(&self, msg: WsMessage) {
        let listeners = self.listeners.read().await;
        let event_type = &msg.event_type;

        for listener in listeners.iter() {
            let matched = if listener.is_prefix {
                if listener.pattern == "*" {
                    true
                } else {
                    // 去掉末尾的 "*"，比较前缀
                    let prefix = &listener.pattern[..listener.pattern.len() - 1];
                    event_type.starts_with(prefix)
                }
            } else {
                listener.pattern == *event_type
            };

            if matched {
                let handler = listener.handler.clone();
                let msg_clone = msg.clone();
                tokio::spawn(async move {
                    handler(msg_clone).await;
                });
            }
        }
    }

    /// 启动消费循环，从 receiver 读取消息并分发
    pub fn start(self, mut rx: mpsc::UnboundedReceiver<WsMessage>) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                self.dispatch(msg).await;
            }
            warn!("事件通道已关闭，事件分发器停止");
        });
    }
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pnos::events::WsMessage;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn make_msg(event_type: &str) -> WsMessage {
        WsMessage::new(event_type, serde_json::json!({}))
    }

    #[tokio::test]
    async fn test_exact_match() {
        let dispatcher = EventDispatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        dispatcher.on("app.status_changed", move |_msg| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        // 等待 spawn 完成注册
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        dispatcher.dispatch(make_msg("app.status_changed")).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // 不匹配的事件
        dispatcher.dispatch(make_msg("system.stats")).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_prefix_match() {
        let dispatcher = EventDispatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        dispatcher.on("app.*", move |_msg| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        dispatcher.dispatch(make_msg("app.status_changed")).await;
        dispatcher.dispatch(make_msg("app.install_progress")).await;
        dispatcher.dispatch(make_msg("system.stats")).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 只有 app.* 匹配的 2 个
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_wildcard_all() {
        let dispatcher = EventDispatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        dispatcher.on("*", move |_msg| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        dispatcher.dispatch(make_msg("app.status_changed")).await;
        dispatcher.dispatch(make_msg("system.stats")).await;
        dispatcher.dispatch(make_msg("task.progress")).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(count.load(Ordering::SeqCst), 3);
    }
}
