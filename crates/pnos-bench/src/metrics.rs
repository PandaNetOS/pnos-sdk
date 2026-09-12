//! 压测指标收集
//!
//! 使用 hdrhistogram 记录延迟分布，统计成功率和吞吐量。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use hdrhistogram::Histogram;

/// 压测结果
#[derive(Debug, Clone)]
pub struct BenchResult {
    /// 场景名称
    pub scenario_name: String,
    /// 目标地址
    pub target: String,
    /// 压测时长
    pub duration: Duration,
    /// 总请求数
    pub total_requests: u64,
    /// 成功数
    pub success_count: u64,
    /// 失败数
    pub failure_count: u64,
    /// 超时数
    pub timeout_count: u64,
    /// 实际 QPS
    pub actual_qps: f64,
    /// 目标 QPS
    pub target_qps: u32,
    /// 延迟直方图（微秒）
    pub latency_histogram: Histogram<u64>,
}

impl BenchResult {
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.success_count as f64 / self.total_requests as f64 * 100.0
        }
    }

    pub fn failure_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.failure_count as f64 / self.total_requests as f64 * 100.0
        }
    }

    pub fn latency_p50(&self) -> u64 {
        self.latency_histogram.value_at_percentile(50.0)
    }

    pub fn latency_p90(&self) -> u64 {
        self.latency_histogram.value_at_percentile(90.0)
    }

    pub fn latency_p99(&self) -> u64 {
        self.latency_histogram.value_at_percentile(99.0)
    }

    pub fn latency_p999(&self) -> u64 {
        self.latency_histogram.value_at_percentile(99.9)
    }

    pub fn latency_max(&self) -> u64 {
        self.latency_histogram.max()
    }

    pub fn latency_avg(&self) -> f64 {
        self.latency_histogram.mean()
    }
}

/// 线程安全的指标收集器
pub struct MetricsCollector {
    total_requests: AtomicU64,
    success_count: AtomicU64,
    failure_count: AtomicU64,
    timeout_count: AtomicU64,
    histogram: Mutex<Histogram<u64>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            timeout_count: AtomicU64::new(0),
            // 延迟范围：1微秒 ~ 10秒，3位有效数字
            histogram: Mutex::new(Histogram::new_with_bounds(1, 10_000_000, 3).unwrap()),
        }
    }

    pub fn record_success(&self, latency: Duration) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.success_count.fetch_add(1, Ordering::Relaxed);
        let micros = latency.as_micros() as u64;
        if let Ok(mut h) = self.histogram.lock() {
            let _ = h.record(micros.max(1));
        }
    }

    pub fn record_failure(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.failure_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_timeout(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.total_requests.load(Ordering::Relaxed),
            self.success_count.load(Ordering::Relaxed),
            self.failure_count.load(Ordering::Relaxed),
            self.timeout_count.load(Ordering::Relaxed),
        )
    }

    /// 克隆 histogram 数据
    pub fn histogram_snapshot(&self) -> Histogram<u64> {
        self.histogram.lock().map(|h| h.clone()).unwrap_or_else(|_| {
            Histogram::new_with_bounds(1, 10_000_000, 3).unwrap()
        })
    }

    pub fn into_result(
        self,
        scenario_name: String,
        target: String,
        duration: Duration,
        target_qps: u32,
    ) -> BenchResult {
        let total = self.total_requests.load(Ordering::Relaxed);
        let actual_qps = if duration.as_secs_f64() > 0.0 {
            total as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        BenchResult {
            scenario_name,
            target,
            duration,
            total_requests: total,
            success_count: self.success_count.load(Ordering::Relaxed),
            failure_count: self.failure_count.load(Ordering::Relaxed),
            timeout_count: self.timeout_count.load(Ordering::Relaxed),
            actual_qps,
            target_qps,
            latency_histogram: self.histogram.into_inner().unwrap_or_else(|_| {
                Histogram::new_with_bounds(1, 10_000_000, 3).unwrap()
            }),
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// 进度报告（压测过程中定期输出）
pub struct ProgressReport {
    pub elapsed: Duration,
    pub total_requests: u64,
    pub current_qps: f64,
    pub success_rate: f64,
    pub latency_p50: u64,
    pub latency_p99: u64,
}
