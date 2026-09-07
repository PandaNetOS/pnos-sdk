//! pnos-sdk 完整使用示例
//!
//! 运行：cargo run --example quickstart
//!
//! 演示：
//! 1. 一键启动（自动注册+心跳+服务器+认证+健康检查+WS）
//! 2. 业务路由
//! 3. 事件订阅
//! 4. 调用其他应用
//! 5. 优雅关闭

use axum::{routing::get, Json};
use pnos_sdk::PnosApp;
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化日志（SDK 内部也会初始化，这里显式调用确保早于 SDK）
    pnos_sdk::logging::init_logging();

    PnosApp::builder("demo-app")
        .version("0.1.0")
        .port(18090)
        // 声明依赖（注册时上报给 runtime）
        .dependency("pk")
        // 业务路由（SDK 自动挂载 /health + 认证中间件）
        .route(
            "/api/v1/hello",
            get(|| async { Json(json!({ "msg": "hello from demo-app" })) }),
        )
        .route(
            "/api/v1/echo/:msg",
            get(
                |axum::extract::Path(msg): axum::extract::Path<String>| async move {
                    Json(json!({ "echo": msg }))
                },
            ),
        )
        // 事件订阅（SDK 自动连接 WS + 自动重连）
        .on_event("app.status_changed", |evt| async move {
            tracing::info!("收到应用状态变更: {} -> {:?}", evt.event_type, evt.payload);
        })
        .on_event("system.stats", |evt| async move {
            tracing::debug!("收到系统监控数据: {:?}", evt.payload);
        })
        // 一键启动
        .run()
        .await
}

// ─── 高级用法：init() 后自行组装服务器 ───
//
// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     let app = PnosApp::builder("demo-app")
//         .version("0.1.0")
//         .init()
//         .await?;
//
//     // 调用其他应用（自动发现+自动带token+自动重试）
//     let info: serde_json::Value = app
//         .call("pk")
//         .get("/api/v1/system/info")
//         .send()
//         .await?;
//     println!("pk info: {:?}", info);
//
//     // 拿到 Router 自行组装
//     let router = app
//         .into_router()
//         .route("/custom", get(|| async { "custom route" }));
//
//     // 自行启动服务器...
//     Ok(())
// }
