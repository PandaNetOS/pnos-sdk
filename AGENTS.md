# pnos-sdk AGENTS.md

> 本文件是 AI 代理进入 pnos-sdk 仓库时的首读指南。
> 生态级全局约束请参考 [根目录 AGENTS.md](../AGENTS.md)。

## 仓库定位

pnos-sdk 是 PandaNetOS 生态的**统一 SDK 仓库**，Cargo workspace，内含多个子 crate。所有 Agent 按需依赖子 crate。

## 架构概览

```
pnos-sdk/
└── crates/
    ├── pnos-comm/     # 统一通信 SDK（自动注册、心跳、服务发现、组件调用、事件订阅、认证、健康检查）
    ├── pnos-net/      # 网络传输层（TCP 直连/打洞/中继、连接管理、NAT 检测，预留 Iroh 扩展）
    └── pnos-bench/    # 通用压测框架（多场景插件、恒定 QPS、延迟分布统计）
```

## 目录结构

```
pnos-sdk/
├── crates/           # 子 crate
│   ├── pnos-comm/    # 通信 SDK
│   ├── pnos-net/     # 网络传输
│   └── pnos-bench/   # 压测框架
├── docs/             # 文档
└── Cargo.toml        # workspace 配置
```

## 构建与测试

| 命令 | 说明 |
|---|---|
| `cargo build --release` | 构建所有子 crate |
| `cargo test --all` | 运行所有测试 |
| `cargo build -p pnos-comm` | 只构建通信 SDK |
| `cargo build -p pnos-net` | 只构建网络传输 |

## 依赖关系

- **依赖**：`pnos`（git，pnos-spec）— workspace 统一依赖
- **被依赖**：pdc（pnos-net）、pk（pnos-comm）、pnos-runtime（pnos-comm）、所有 Agent

## 子 crate 说明

| crate | 用途 | 主要使用方 |
|---|---|---|
| pnos-comm | 通信、注册、心跳、服务发现、事件订阅 | pk、pnos-runtime |
| pnos-net | TCP 打洞、中继、连接管理、NAT 检测 | pdc、spde |
| pnos-bench | 压测框架 | 测试工程 |

## 注意事项

1. pnos-sdk 是 workspace，新增子 crate 需在 Cargo.toml 的 members 中注册
2. 所有子 crate 统一依赖 pnos-spec，不允许直接依赖旧 pandanetos
3. pnos-net 当前有 18 个 warnings（预存问题），修改时注意不要引入新 warning

## 变更历史

| 日期 | 版本 | 变更内容 |
|---|---|---|
| 2026-09-16 | v1.0 | 初始版本 |
