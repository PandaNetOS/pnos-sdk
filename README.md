# pnos-sdk

PandaNetOS 统一 SDK 仓库，cargo workspace 管理。

## Crates

| crate | 说明 |
|---|---|
| [`pnos-comm`](crates/pnos-comm) | 统一通信 SDK：自动注册、心跳、服务发现、组件调用、事件订阅、认证、健康检查。应用与 Agent 共用 |

## 新增 SDK

在 `crates/` 下创建目录，加入 `Cargo.toml` 和 `src/`，workspace 自动识别。

```toml
# crates/pnos-xxx/Cargo.toml
[package]
name = "pnos-xxx"
version = "0.1.0"
edition.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
pnos.workspace = true
```
