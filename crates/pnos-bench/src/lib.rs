//! pnos-bench - PandaNetOS 通用压测框架
//!
//! 支持恒定 QPS、并发控制、延迟分布统计、多场景插件。

pub mod engine;
pub mod metrics;
pub mod report;
pub mod scenario;
pub mod scenarios;

pub use engine::{BenchConfig, BenchEngine};
pub use metrics::{BenchResult, MetricsCollector};
pub use report::{print_report, ReportFormat};
pub use scenario::{BenchContext, Scenario, ScenarioRegistry};
