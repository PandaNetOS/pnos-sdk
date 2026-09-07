//! 统一日志初始化
//!
//! 基于 [`tracing_subscriber`]，输出格式：时间 级别 目标 - 消息。
//! 日志级别从环境变量 `PNOS_LOG_LEVEL` 或 `RUST_LOG` 读取，默认 `info`。

use tracing_subscriber::EnvFilter;

/// 初始化全局日志订阅者
///
/// 幂等：重复调用不会报错（第二次会被 tracing 忽略）。
pub fn init_logging() {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .try_init();
}

/// 初始化全局日志订阅者（指定默认级别）
pub fn init_logging_with_level(default_level: &str) {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .try_init();
}
