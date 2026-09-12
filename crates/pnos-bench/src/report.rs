//! 压测报告输出
//!
//! 支持控制台文本报告和 JSON 报告。

use crate::metrics::BenchResult;

/// 报告格式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportFormat {
    /// 控制台文本
    Text,
    /// JSON
    Json,
}

/// 输出压测报告
pub fn print_report(result: &BenchResult, format: ReportFormat) {
    match format {
        ReportFormat::Text => print_text_report(result),
        ReportFormat::Json => print_json_report(result),
    }
}

/// 文本报告
fn print_text_report(r: &BenchResult) {
    let qps_achievement = if r.target_qps > 0 {
        r.actual_qps / r.target_qps as f64 * 100.0
    } else {
        100.0
    };

    println!();
    println!("═══════════════════════════════════════════════");
    println!("  pnos-bench 压测报告");
    println!("  场景: {}", r.scenario_name);
    println!("  目标: {}", r.target);
    println!("  时长: {:.2}s", r.duration.as_secs_f64());
    println!("═══════════════════════════════════════════════");
    println!();
    println!("【吞吐量】");
    println!("  总请求:     {:>12}", r.total_requests);
    println!("  实际 QPS:   {:>12.0} /s", r.actual_qps);
    println!("  目标 QPS:   {:>12} /s", r.target_qps);
    println!("  达成率:     {:>11.1}%", qps_achievement);
    println!();
    println!("【成功率】");
    println!("  成功:       {:>12} ({:.2}%)", r.success_count, r.success_rate());
    println!("  失败:       {:>12} ({:.2}%)", r.failure_count, r.failure_rate());
    println!("  超时:       {:>12}", r.timeout_count);
    println!();
    println!("【延迟分布】");
    println!("  P50:        {:>10} μs ({:.2} ms)", r.latency_p50(), r.latency_p50() as f64 / 1000.0);
    println!("  P90:        {:>10} μs ({:.2} ms)", r.latency_p90(), r.latency_p90() as f64 / 1000.0);
    println!("  P99:        {:>10} μs ({:.2} ms)", r.latency_p99(), r.latency_p99() as f64 / 1000.0);
    println!("  P999:       {:>10} μs ({:.2} ms)", r.latency_p999(), r.latency_p999() as f64 / 1000.0);
    println!("  Max:        {:>10} μs ({:.2} ms)", r.latency_max(), r.latency_max() as f64 / 1000.0);
    println!("  Avg:        {:>10.0} μs ({:.2} ms)", r.latency_avg(), r.latency_avg() / 1000.0);
    println!();
    println!("【结论】");
    if r.latency_p99() < 100 {
        println!("  ✅ 延迟达标 (P99 < 100μs)");
    } else if r.latency_p99() < 1000 {
        println!("  ⚠️  延迟一般 (P99 = {}μs)", r.latency_p99());
    } else {
        println!("  ❌ 延迟超标 (P99 = {}μs)", r.latency_p99());
    }

    if r.target_qps > 0 {
        if qps_achievement >= 95.0 {
            println!("  ✅ QPS 达标 ({:.0}/{})", r.actual_qps, r.target_qps);
        } else if qps_achievement >= 80.0 {
            println!("  ⚠️  QPS 接近达标 ({:.0}/{}, {:.1}%)", r.actual_qps, r.target_qps, qps_achievement);
        } else {
            println!("  ❌ QPS 未达标 ({:.0}/{}, {:.1}%)", r.actual_qps, r.target_qps, qps_achievement);
        }
    }

    if r.success_rate() >= 99.9 {
        println!("  ✅ 成功率优秀 ({:.2}%)", r.success_rate());
    } else if r.success_rate() >= 99.0 {
        println!("  ⚠️  成功率良好 ({:.2}%)", r.success_rate());
    } else {
        println!("  ❌ 成功率偏低 ({:.2}%)", r.success_rate());
    }
    println!("═══════════════════════════════════════════════");
    println!();
}

/// JSON 报告
fn print_json_report(r: &BenchResult) {
    let json = serde_json::json!({
        "scenario": r.scenario_name,
        "target": r.target,
        "duration_secs": r.duration.as_secs_f64(),
        "throughput": {
            "total_requests": r.total_requests,
            "actual_qps": r.actual_qps,
            "target_qps": r.target_qps,
            "achievement_pct": if r.target_qps > 0 {
                r.actual_qps / r.target_qps as f64 * 100.0
            } else {
                100.0
            },
        },
        "success": {
            "success_count": r.success_count,
            "failure_count": r.failure_count,
            "timeout_count": r.timeout_count,
            "success_rate_pct": r.success_rate(),
            "failure_rate_pct": r.failure_rate(),
        },
        "latency_us": {
            "p50": r.latency_p50(),
            "p90": r.latency_p90(),
            "p99": r.latency_p99(),
            "p999": r.latency_p999(),
            "max": r.latency_max(),
            "avg": r.latency_avg(),
        },
    });
    println!("{}", serde_json::to_string_pretty(&json).unwrap());
}
