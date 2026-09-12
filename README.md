# pnos-sdk

PandaNetOS 统一 SDK 仓库，cargo workspace 管理。

## Crates

| crate | 说明 |
|---|---|
| [`pnos-comm`](crates/pnos-comm) | 统一通信 SDK：自动注册、心跳、服务发现、组件调用、事件订阅、认证、健康检查。应用与 Agent 共用 |
| [`pnos-net`](crates/pnos-net) | 网络传输层：TCP 直连/打洞/中继、连接管理、NAT 检测。支持 Transport trait 抽象，可扩展 Iroh 传输 |
| [`pnos-bench`](crates/pnos-bench) | 通用压测框架：多场景插件、恒定 QPS、并发控制、延迟分布统计、文本/JSON 报告 |

## pnos-bench 使用

通用压测工具，支持 UDP Tracker（BEP15）、HTTP 等场景，可扩展自定义场景。

### 编译

```bash
cargo build --release -p pnos-bench
```

### 列出场景

```bash
./target/release/pnos-bench list
```

### 压测 PDC 超级 Tracker

```bash
# announce 压测，5万 QPS，持续60秒
./target/release/pnos-bench run udp_tracker \
  --target 127.0.0.1:6880 \
  --qps 50000 \
  --concurrency 200 \
  --duration 60s

# scrape 压测
./target/release/pnos-bench run udp_tracker \
  --target 127.0.0.1:6880 \
  --qps 10000 \
  --duration 30s \
  --arg mode=scrape

# 混合模式（announce:scrape = 8:2）
./target/release/pnos-bench run udp_tracker \
  --target 127.0.0.1:6880 \
  --qps 20000 \
  --duration 30s \
  --arg mode=mixed

# JSON 报告输出
./target/release/pnos-bench run udp_tracker \
  --target 127.0.0.1:6880 \
  --qps 10000 \
  --duration 30s \
  --report json
```

### 压测 HTTP 接口

```bash
./target/release/pnos-bench run http \
  --target http://127.0.0.1:8080 \
  --qps 5000 \
  --duration 30s \
  --arg url=http://127.0.0.1:8080/api/test
```

### UDP Echo Server（测试压测工具上限）

```bash
# 启动 echo server
./target/release/udp-echo-server --port 6890

# 压测 echo server
./target/release/pnos-bench run udp_tracker --target 127.0.0.1:6890 --qps 100000 --duration 20s
```

### 压测报告示例

```
═══════════════════════════════════════════════
  pnos-bench 压测报告
  场景: udp_tracker
  目标: 127.0.0.1:6880
  时长: 30.10s
═══════════════════════════════════════════════

【吞吐量】
  总请求:           299964
  实际 QPS:           9967 /s
  目标 QPS:          10000 /s
  达成率:            99.7%

【成功率】
  成功:             299964 (100.00%)
  失败:                  0 (0.00%)
  超时:                  0

【延迟分布】
  P50:              2020 μs (2.02 ms)
  P90:              5359 μs (5.36 ms)
  P99:             26991 μs (26.99 ms)
  P999:           150015 μs (150.01 ms)
  Max:            355583 μs (355.58 ms)
  Avg:              3491 μs (3.49 ms)
═══════════════════════════════════════════════
```

### 扩展自定义压测场景

实现 `Scenario` trait：

```rust
use async_trait::async_trait;
use pnos_bench::{BenchContext, Scenario};

pub struct MyScenario;

#[async_trait]
impl Scenario for MyScenario {
    fn name(&self) -> &str { "my_scenario" }
    fn description(&self) -> &str { "我的压测场景" }

    async fn setup(&self, _ctx: &BenchContext) -> anyhow::Result<()> {
        // 初始化：预生成数据、建立连接池
        Ok(())
    }

    async fn request(&self, ctx: &BenchContext) -> anyhow::Result<()> {
        // 单次请求逻辑
        Ok(())
    }
}
```

## pnos-net 传输层

支持 TCP 直连、UDP 打洞、TCP 中继。已抽象 `Transport` trait，为 Iroh 接入预留扩展点。

| 模式 | 说明 |
|---|---|
| TcpOnly | 仅 TCP（当前默认，行为与原有一致） |
| IrohOnly | 仅 Iroh（阶段2实现） |
| Auto | TCP + Iroh 并行，取最快（阶段2实现） |

详细设计见 [`docs/iroh-transport-design.md`](docs/iroh-transport-design.md)。

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
