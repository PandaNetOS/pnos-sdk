//! 压测引擎
//!
//! 支持恒定 QPS、并发数控制、持续时间控制。

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::metrics::MetricsCollector;
use crate::scenario::{BenchContext, Scenario};

/// 压测配置
#[derive(Debug, Clone)]
pub struct BenchConfig {
    /// 目标地址
    pub target: String,
    /// 目标 QPS（0 表示不限速，按并发数跑满）
    pub qps: u32,
    /// 并发 worker 数
    pub concurrency: u32,
    /// 持续时间
    pub duration: Duration,
    /// 请求超时
    pub request_timeout: Duration,
    /// 进度报告间隔
    pub progress_interval: Duration,
    /// 自定义参数
    pub args: std::collections::HashMap<String, String>,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            target: "127.0.0.1:6880".to_string(),
            qps: 1000,
            concurrency: 50,
            duration: Duration::from_secs(60),
            request_timeout: Duration::from_secs(5),
            progress_interval: Duration::from_secs(5),
            args: std::collections::HashMap::new(),
        }
    }
}

/// 压测引擎
pub struct BenchEngine {
    scenario: Arc<dyn Scenario>,
    config: BenchConfig,
    metrics: Arc<MetricsCollector>,
}

impl BenchEngine {
    pub fn new(scenario: Arc<dyn Scenario>, config: BenchConfig) -> Self {
        Self {
            scenario,
            config,
            metrics: Arc::new(MetricsCollector::new()),
        }
    }

    /// 执行压测
    pub async fn run(&self) -> anyhow::Result<crate::metrics::BenchResult> {
        info!(
            "=== 开始压测 ==="
        );
        info!("场景: {}", self.scenario.name());
        info!("目标: {}", self.config.target);
        info!("目标 QPS: {}", self.config.qps);
        info!("并发数: {}", self.config.concurrency);
        info!("持续时间: {:?}", self.config.duration);

        let ctx = Arc::new(BenchContext::new(
            self.config.qps,
            self.config.concurrency,
            self.config.args.clone(),
            self.metrics.clone(),
        ));

        // Setup
        debug!("执行 setup...");
        self.scenario.setup(&ctx).await?;

        let start = Instant::now();
        use std::sync::atomic::{AtomicBool, Ordering};
        let stop_flag = Arc::new(AtomicBool::new(false));

        // 进度报告任务
        let metrics_clone = self.metrics.clone();
        let progress_interval = self.config.progress_interval;
        let duration = self.config.duration;
        let stop_clone = stop_flag.clone();
        tokio::spawn(async move {
            let mut last_total = 0u64;
            let mut last_time = Instant::now();
            loop {
                tokio::time::sleep(progress_interval).await;
                let (total, success, failure, timeout) = metrics_clone.snapshot();
                let elapsed = start.elapsed();
                let delta = total - last_total;
                let delta_time = last_time.elapsed().as_secs_f64();
                let current_qps = if delta_time > 0.0 { delta as f64 / delta_time } else { 0.0 };
                let success_rate = if total > 0 { success as f64 / total as f64 * 100.0 } else { 100.0 };

                info!(
                    "[进度] 已运行 {:>6.1}s | 总请求 {:>10} | 当前QPS {:>8.0} | 成功率 {:>5.1}% | 失败 {} 超时 {}",
                    elapsed.as_secs_f64(),
                    total,
                    current_qps,
                    success_rate,
                    failure,
                    timeout
                );

                last_total = total;
                last_time = Instant::now();

                if elapsed >= duration {
                    stop_clone.store(true, Ordering::SeqCst);
                    break;
                }
            }
        });

        // Worker 任务
        let mut handles = Vec::new();
        for worker_id in 0..self.config.concurrency {
            let scenario = self.scenario.clone();
            let ctx = ctx.clone();
            let metrics = self.metrics.clone();
            let config = self.config.clone();
            let stop = stop_flag.clone();

            handles.push(tokio::spawn(async move {
                Self::worker_loop(worker_id, scenario, ctx, metrics, config, stop).await;
            }));
        }

        // 等待停止信号
        while !stop_flag.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        debug!("收到停止信号，等待 worker 退出...");

        // 等待所有 worker 退出
        for handle in handles {
            let _ = handle.await;
        }

        let elapsed = start.elapsed();

        // Teardown
        debug!("执行 teardown...");
        self.scenario.teardown(&ctx).await?;

        // 从 metrics 提取结果
        let (total, success, failure, timeout) = self.metrics.snapshot();
        let histogram = self.metrics.histogram_snapshot();

        let actual_qps = if elapsed.as_secs_f64() > 0.0 {
            total as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let result = crate::metrics::BenchResult {
            scenario_name: self.scenario.name().to_string(),
            target: self.config.target.clone(),
            duration: elapsed,
            total_requests: total,
            success_count: success,
            failure_count: failure,
            timeout_count: timeout,
            actual_qps,
            target_qps: self.config.qps,
            latency_histogram: histogram,
        };

        info!("=== 压测结束 ===");
        Ok(result)
    }

    /// Worker 主循环
    async fn worker_loop(
        worker_id: u32,
        scenario: Arc<dyn Scenario>,
        ctx: Arc<BenchContext>,
        metrics: Arc<MetricsCollector>,
        config: BenchConfig,
        stop_flag: Arc<std::sync::atomic::AtomicBool>,
    ) {
        use std::sync::atomic::Ordering;
        debug!("Worker {} 启动", worker_id);

        // QPS 控制：每个 worker 的发送间隔
        let per_worker_qps = if config.qps > 0 && config.concurrency > 0 {
            config.qps as f64 / config.concurrency as f64
        } else {
            0.0 // 不限速
        };
        let interval = if per_worker_qps > 0.0 {
            Duration::from_micros((1_000_000.0 / per_worker_qps) as u64)
        } else {
            Duration::from_micros(0)
        };

        let mut next_send = Instant::now();

        loop {
            // 检查停止信号
            if stop_flag.load(Ordering::SeqCst) {
                break;
            }

            // QPS 限速
            if interval.as_micros() > 0 {
                let now = Instant::now();
                if next_send > now {
                    tokio::time::sleep(next_send - now).await;
                }
                next_send += interval;
            }

            // 执行请求
            let start = Instant::now();
            let result = tokio::time::timeout(config.request_timeout, scenario.request(&ctx)).await;

            // 异步指标模式：场景自己管理成功/失败/延迟，引擎只记录发送
            if scenario.manages_own_metrics() {
                match result {
                    Ok(Ok(())) => { /* 发送成功，由接收任务记录真实成功 */ }
                    Ok(Err(e)) => {
                        debug!("Worker {} 发送失败: {}", worker_id, e);
                        metrics.record_failure();
                    }
                    Err(_) => {
                        warn!("Worker {} 发送超时", worker_id);
                        metrics.record_timeout();
                    }
                }
            } else {
                // 同步模式：引擎记录完整指标
                match result {
                    Ok(Ok(())) => {
                        metrics.record_success(start.elapsed());
                    }
                    Ok(Err(e)) => {
                        debug!("Worker {} 请求失败: {}", worker_id, e);
                        metrics.record_failure();
                    }
                    Err(_) => {
                        warn!("Worker {} 请求超时", worker_id);
                        metrics.record_timeout();
                    }
                }
            }
        }

        debug!("Worker {} 退出", worker_id);
    }
}
