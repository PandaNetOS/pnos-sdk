//! WebSocket 事件客户端
//!
//! 连接 pnos-runtime 的 `/api/v1/ws` 端点，自动重连、Ping-Pong 保活，
//! 收到的 [`pnos::events::WsMessage`] 通过 mpsc channel 推送给事件分发器。

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::config::SdkConfig;
use crate::error::{Result, SdkError};
use pnos::events::{WsMessage, WsSubscribe};

/// WebSocket 客户端
#[derive(Clone)]
pub struct WsClient {
    config: Arc<SdkConfig>,
    token: Arc<RwLock<Option<String>>>,
    /// 事件输出通道（事件分发器从这里收消息）
    event_tx: mpsc::UnboundedSender<WsMessage>,
    /// 订阅管理（需要重连后重新订阅）
    subscriptions: Arc<RwLock<Vec<String>>>,
    /// 关闭信号
    shutdown: Arc<tokio::sync::Notify>,
}

impl WsClient {
    pub fn new(
        config: Arc<SdkConfig>,
        token: Arc<RwLock<Option<String>>>,
    ) -> (Self, mpsc::UnboundedReceiver<WsMessage>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let client = Self {
            config,
            token,
            event_tx,
            subscriptions: Arc::new(RwLock::new(Vec::new())),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        };
        (client, event_rx)
    }

    /// 启动 WebSocket 后台任务（连接 + 重连 + 保活）
    pub fn start(&self) {
        let client = self.clone();
        tokio::spawn(async move {
            client.run().await;
        });
    }

    /// 订阅事件类型（支持前缀匹配，如 "app.*"）
    pub async fn subscribe(&self, event_type: impl Into<String>) {
        let event_type = event_type.into();
        let mut subs = self.subscriptions.write().await;
        if !subs.contains(&event_type) {
            subs.push(event_type.clone());
        }
        // 发送订阅消息（如果连接已建立）
        // 注意：实际发送在 run() 循环里处理，这里只记录订阅
        debug!("订阅事件: {}", event_type);
    }

    /// 取消订阅
    pub async fn unsubscribe(&self, event_type: &str) {
        let mut subs = self.subscriptions.write().await;
        subs.retain(|s| s != event_type);
        debug!("取消订阅事件: {}", event_type);
    }

    /// 关闭连接
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    /// 主循环：连接 → 读写 → 断线重连
    async fn run(&self) {
        let mut reconnect_delay = Duration::from_secs(1);

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    info!("WebSocket 客户端关闭");
                    return;
                }
                result = self.connect_and_run() => {
                    match result {
                        Ok(()) => {
                            // 正常关闭
                            return;
                        }
                        Err(e) => {
                            warn!("WebSocket 断开: {}，{}ms 后重连", e, reconnect_delay.as_millis());
                            tokio::time::sleep(reconnect_delay).await;
                            // 指数退避，最大 30s
                            reconnect_delay = std::cmp::min(reconnect_delay * 2, Duration::from_secs(30));
                        }
                    }
                }
            }
        }
    }

    /// 建立连接并运行读写循环
    async fn connect_and_run(&self) -> Result<()> {
        let token = self
            .token
            .read()
            .await
            .clone()
            .ok_or_else(|| SdkError::Auth("未注册，无法建立 WebSocket 连接".to_string()))?;

        let url = format!("{}?token={}", self.config.ws_url(), token);
        debug!("连接 WebSocket: {}", url);

        let (ws_stream, _response) = tokio_tungstenite::connect_async(&url)
            .await
            .map_err(|e| SdkError::WebSocket(format!("连接失败: {e}")))?;

        info!("WebSocket 已连接: {}", self.config.ws_url());

        let (mut write, mut read) = ws_stream.split();

        // 重连成功后重置退避延迟（通过外部变量，这里用一个小技巧）
        // 发送所有已订阅的事件
        let subs = self.subscriptions.read().await.clone();
        for event_type in &subs {
            let sub_msg = WsSubscribe {
                action: "subscribe".to_string(),
                event_type: event_type.clone(),
            };
            let json = serde_json::to_string(&sub_msg).map_err(SdkError::Serde)?;
            write
                .send(Message::Text(json))
                .await
                .map_err(|e| SdkError::WebSocket(format!("发送订阅失败: {e}")))?;
        }
        drop(subs);

        // 保活定时器
        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    let _ = write.send(Message::Close(None)).await;
                    return Ok(());
                }
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        return Err(SdkError::WebSocket(format!("发送 Ping 失败: {e}")));
                    }
                    debug!("WebSocket Ping");
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            self.handle_text_message(&text).await;
                        }
                        Some(Ok(Message::Binary(data))) => {
                            if let Ok(text) = String::from_utf8(data) {
                                self.handle_text_message(&text).await;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {
                            debug!("WebSocket Pong");
                        }
                        Some(Ok(Message::Ping(_))) => {
                            // 自动回 Pong（tungstenite 会自动处理，但保险起见）
                        }
                        Some(Ok(Message::Frame(_))) => {
                            // 原始帧，忽略
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("WebSocket 被服务端关闭");
                            return Err(SdkError::WebSocket("服务端关闭连接".to_string()));
                        }
                        Some(Err(e)) => {
                            return Err(SdkError::WebSocket(format!("读取错误: {e}")));
                        }
                        None => {
                            return Err(SdkError::WebSocket("连接已关闭".to_string()));
                        }
                    }
                }
            }
        }
    }

    /// 处理文本消息：解析为 WsMessage 并推送到事件通道
    async fn handle_text_message(&self, text: &str) {
        match serde_json::from_str::<WsMessage>(text) {
            Ok(msg) => {
                debug!("收到事件: {} (source={})", msg.event_type, msg.source);
                if let Err(e) = self.event_tx.send(msg) {
                    error!("事件通道发送失败: {}", e);
                }
            }
            Err(e) => {
                warn!("解析 WebSocket 消息失败: {}，原始内容: {}", e, text);
            }
        }
    }
}
