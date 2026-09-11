# pnos-comm

PandaNetOS 应用开发 SDK。应用启动自动注册到 pnos-runtime，定期心跳，调用其他应用自动发现地址并注入认证 Token。

## 核心能力

| 能力 | 说明 |
|------|------|
| 自动注册 | `PnosApp::init()` 启动时自动向 pnos-runtime 注册 |
| 自动心跳 | 每 15 秒自动发送心跳，超时 runtime 自动标记离线 |
| 服务发现 | 调用其他应用时自动通过 runtime 查询地址 |
| 统一认证 | 自动注入 `X-Pnos-Token`，应用间调用零配置 |
| 统一响应 | 自动解析 `ApiResponse<T>`，错误码直接转 `Result` |
| 健康检查 | `HealthBuilder` 快速构建标准健康检查响应 |

## 快速开始

```rust
use pnos_comm::PnosApp;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化：自动注册 + 心跳
    let app = PnosApp::builder("my-app")
        .version("0.1.0")
        .port(18090)
        .init()
        .await?;

    // 调用其他应用（自动发现地址 + 自动带 Token）
    let info: serde_json::Value = app
        .call("pk")
        .get("/api/v1/system/info")
        .send()
        .await?;

    Ok(())
}
```

## 应用间调用

```rust
// GET，自动解析 data
let data: MyType = app.call("pk").get("/api/v1/tasks").send().await?;

// POST
let result: TaskId = app.call("pk")
    .post("/api/v1/tasks", &TaskRequest { ... })
    .send()
    .await?;

// 只检查成功，不解析 data
app.call("spde").post("/api/v1/start", &()).send_empty().await?;
```

## 健康检查

```rust
use pnos_comm::HealthBuilder;

let health = HealthBuilder::new("0.1.0")
    .dependency_ok("pk")
    .dependency_degraded("spde", "下载队列积压")
    .build();
// 返回符合 pnos 标准的 HealthResponse
```

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `PNOS_RUNTIME_URL` | pnos-runtime 地址 | `http://127.0.0.1:8080` |
| `PNOS_DATA_DIR` | 应用数据目录 | `/pnos/data/apps` |
| `PNOS_MEDIA_DIR` | 媒体目录 | `/pnos/media` |

## 依赖

- `pnos` — 统一标准库（响应格式、错误码、注册协议、健康检查协议）
