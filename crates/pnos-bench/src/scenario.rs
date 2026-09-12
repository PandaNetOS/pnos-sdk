//! 压测场景抽象
//!
//! 每个项目实现自己的 Scenario，压测引擎反复调用 request() 方法。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use crate::metrics::MetricsCollector;

/// 压测上下文：引擎传给场景的运行时信息
pub struct BenchContext {
    /// 当前已发送请求数
    pub request_count: AtomicU64,
    /// 目标 QPS
    pub target_qps: u32,
    /// 并发 worker 数
    pub concurrency: u32,
    /// 压测开始时间
    pub start_time: std::time::Instant,
    /// 用户自定义参数（从 CLI --args 传入）
    pub args: std::collections::HashMap<String, String>,
    /// 指标收集器（场景可直接更新，用于异步指标模式）
    pub metrics: Arc<MetricsCollector>,
}

impl BenchContext {
    pub fn new(
        target_qps: u32,
        concurrency: u32,
        args: std::collections::HashMap<String, String>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            request_count: AtomicU64::new(0),
            target_qps,
            concurrency,
            start_time: std::time::Instant::now(),
            args,
            metrics,
        }
    }

    pub fn next_request_id(&self) -> u64 {
        self.request_count.fetch_add(1, Ordering::Relaxed)
    }
}

/// 压测场景 trait
///
/// 每个项目实现自己的场景，压测引擎反复调用 request()。
#[async_trait]
pub trait Scenario: Send + Sync {
    /// 场景唯一标识（用于 CLI 选择）
    fn name(&self) -> &str;

    /// 场景描述
    fn description(&self) -> &str;

    /// 是否自己管理指标（发送/接收分离模式）
    ///
    /// 返回 true 时，引擎不会在 request() 返回后自动记录成功/失败/延迟，
    /// 由场景通过 ctx.metrics 自行记录（通常在后台接收任务中）。
    fn manages_own_metrics(&self) -> bool {
        false
    }

    /// 初始化（预生成数据、建立连接池等），在压测开始前调用一次
    async fn setup(&self, _ctx: &BenchContext) -> anyhow::Result<()> {
        Ok(())
    }

    /// 单次请求，压测引擎反复调用
    ///
    /// 同步模式（默认）：返回 Ok(()) 表示成功，Err 表示失败，引擎统计延迟。
    /// 异步模式（manages_own_metrics=true）：只发送不等待，由后台任务统计。
    async fn request(&self, ctx: &BenchContext) -> anyhow::Result<()>;

    /// 清理，压测结束后调用一次
    async fn teardown(&self, _ctx: &BenchContext) -> anyhow::Result<()> {
        Ok(())
    }
}

/// 场景注册表：所有可用场景
pub struct ScenarioRegistry {
    scenarios: Vec<Arc<dyn Scenario>>,
}

impl ScenarioRegistry {
    pub fn new() -> Self {
        Self { scenarios: Vec::new() }
    }

    pub fn register(&mut self, scenario: Arc<dyn Scenario>) {
        self.scenarios.push(scenario);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Scenario>> {
        self.scenarios.iter().find(|s| s.name() == name).cloned()
    }

    pub fn list(&self) -> &[Arc<dyn Scenario>] {
        &self.scenarios
    }
}

impl Default for ScenarioRegistry {
    fn default() -> Self {
        Self::new()
    }
}
