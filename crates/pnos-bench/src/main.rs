//! pnos-bench 命令行入口

use std::collections::HashMap;
use std::time::Duration;

use clap::{Parser, Subcommand};
use pnos_bench::{
    engine::{BenchConfig, BenchEngine},
    report::{print_report, ReportFormat},
    scenario::ScenarioRegistry,
    scenarios,
};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "pnos-bench", version, about = "PandaNetOS 通用压测框架")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// 日志级别
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    /// 列出所有可用压测场景
    List,

    /// 运行压测
    Run {
        /// 场景名称（用 list 查看可用场景）
        scenario: String,

        /// 目标地址
        #[arg(short, long)]
        target: String,

        /// 目标 QPS（0 表示不限速）
        #[arg(short, long, default_value_t = 1000)]
        qps: u32,

        /// 并发 worker 数
        #[arg(short, long, default_value_t = 50)]
        concurrency: u32,

        /// 持续时间（如 60s, 5m, 1h）
        #[arg(short, long, default_value = "60s")]
        duration: String,

        /// 请求超时（如 5s）
        #[arg(long, default_value = "5s")]
        timeout: String,

        /// 自定义参数（key=value，可多次指定）
        #[arg(short = 'a', long = "arg")]
        args: Vec<String>,

        /// 报告格式（text/json）
        #[arg(long, default_value = "text")]
        report: String,
    },
}

fn parse_duration(s: &str) -> Duration {
    let s = s.trim();
    if let Some(secs) = s.strip_suffix('s') {
        Duration::from_secs(secs.parse().unwrap_or(60))
    } else if let Some(mins) = s.strip_suffix('m') {
        Duration::from_secs(mins.parse::<u64>().unwrap_or(1) * 60)
    } else if let Some(hours) = s.strip_suffix('h') {
        Duration::from_secs(hours.parse::<u64>().unwrap_or(1) * 3600)
    } else {
        Duration::from_secs(s.parse().unwrap_or(60))
    }
}

fn parse_args(args: &[String]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for arg in args {
        if let Some((k, v)) = arg.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // 初始化日志
    let level = match cli.log_level.as_str() {
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("设置日志失败");

    // 注册场景
    let mut registry = ScenarioRegistry::new();
    scenarios::register_all(&mut registry);

    match cli.command {
        Commands::List => {
            println!();
            println!("可用压测场景:");
            println!("{:-<60}", "");
            for s in registry.list() {
                println!("  {:<20} {}", s.name(), s.description());
            }
            println!();
        }
        Commands::Run {
            scenario,
            target,
            qps,
            concurrency,
            duration,
            timeout,
            args,
            report,
        } => {
            let scenario = match registry.get(&scenario) {
                Some(s) => s,
                None => {
                    eprintln!("未知场景: {}", scenario);
                    eprintln!("用 'pnos-bench list' 查看可用场景");
                    std::process::exit(1);
                }
            };

            let mut extra_args = parse_args(&args);
            extra_args.insert("target".to_string(), target.clone());

            let config = BenchConfig {
                target: target.clone(),
                qps,
                concurrency,
                duration: parse_duration(&duration),
                request_timeout: parse_duration(&timeout),
                progress_interval: Duration::from_secs(5),
                args: extra_args,
            };

            let engine = BenchEngine::new(scenario, config);
            let result = engine.run().await?;

            let format = match report.as_str() {
                "json" => ReportFormat::Json,
                _ => ReportFormat::Text,
            };
            print_report(&result, format);
        }
    }

    Ok(())
}
