//! DNS 解析池（pnos-net 通用能力）
//!
//! 并行向多个公共 DNS 服务器发起查询，取最快响应，避免单一 DNS 故障或污染。
//! 内置 5 个 DNS：114、阿里、腾讯、谷歌、Cloudflare。
//! hickory-resolver 自带缓存，无需额外实现。
//!
//! # 为什么不用系统 DNS
//!
//! 宿主机的系统 DNS 配置是「环境依赖」而非「可控依赖」：现场出现过唯一的
//! 解析器整台不响应（网卡 DNS 指向一台不答 DNS 查询的主机），导致连内网
//! peer 都解析不了。本模块统一走自带的 [`DEFAULT_DNS_SERVERS`]，默认
//! **完全不读**宿主系统 DNS 配置；确有需要时可通过
//! [`DnsConfig::allow_system_fallback`] 显式打开回退。
//!
//! # 与 iroh 的关系
//!
//! iroh 端点默认 `DnsResolver::new()` 会 `with_system_defaults()` 读宿主配置，
//! 其 pkarr 发布/解析（HTTPS 到 `dns.iroh.link`）、`DnsAddressLookup`（TXT
//! 查询）与 DERP 中继主机名解析全都共用同一个解析器。用
//! [`DnsPool::to_iroh_resolver`] 构造的解析器注入 `Endpoint::builder(...)
//! .dns_resolver(...)` 即可让这四路同时脱离系统 DNS。

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use anyhow::Result;
use hickory_resolver::config::{
    LookupIpStrategy, NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use hickory_resolver::TokioAsyncResolver;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// 内置公共 DNS 服务器（生态唯一真源）
///
/// 格式为 `ip` 或 `ip:port`，省略端口按 53 处理。
pub const DEFAULT_DNS_SERVERS: &[&str] = &[
    "114.114.114.114:53", // 114 DNS
    "223.5.5.5:53",       // 阿里 DNS
    "119.29.29.29:53",    // 腾讯 DNS
    "8.8.8.8:53",         // 谷歌 DNS
    "1.1.1.1:53",         // Cloudflare DNS
];

/// 默认单次查询超时（秒）
const DEFAULT_DNS_QUERY_TIMEOUT_SECS: u64 = 3;
/// 默认单次查询重试次数
const DEFAULT_DNS_ATTEMPTS: usize = 2;

fn default_dns_servers() -> Vec<String> {
    DEFAULT_DNS_SERVERS.iter().map(|s| s.to_string()).collect()
}

fn default_dns_query_timeout_secs() -> u64 {
    DEFAULT_DNS_QUERY_TIMEOUT_SECS
}

fn default_dns_attempts() -> usize {
    DEFAULT_DNS_ATTEMPTS
}

/// DNS 解析配置
///
/// 所有字段带 serde 默认值，未配置时行为与内置默认完全一致，保证向后兼容。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsConfig {
    /// DNS 服务器列表（`ip` 或 `ip:port`，省略端口按 53）
    #[serde(default = "default_dns_servers")]
    pub servers: Vec<String>,
    /// 是否允许回退到宿主系统 DNS 配置
    ///
    /// 默认 `false`：只走 `servers`，绝不读取系统 DNS。
    #[serde(default)]
    pub allow_system_fallback: bool,
    /// 单次查询超时（秒）
    #[serde(default = "default_dns_query_timeout_secs")]
    pub query_timeout_secs: u64,
    /// 单次查询重试次数
    #[serde(default = "default_dns_attempts")]
    pub attempts: usize,
}

impl Default for DnsConfig {
    fn default() -> Self {
        Self {
            servers: default_dns_servers(),
            allow_system_fallback: false,
            query_timeout_secs: DEFAULT_DNS_QUERY_TIMEOUT_SECS,
            attempts: DEFAULT_DNS_ATTEMPTS,
        }
    }
}

impl DnsConfig {
    /// 把 `servers` 解析为 `SocketAddr` 列表
    ///
    /// 非法项跳过并告警；全部非法（或列表为空）时回退 [`DEFAULT_DNS_SERVERS`]，
    /// 保证永远不会退化成「没有解析器可用」。
    pub fn parse_servers(&self) -> Vec<SocketAddr> {
        let mut out: Vec<SocketAddr> = Vec::with_capacity(self.servers.len());
        for raw in &self.servers {
            match parse_server(raw) {
                Some(addr) => {
                    if !out.contains(&addr) {
                        out.push(addr);
                    }
                }
                None => warn!("[dns] 忽略非法的 DNS 服务器配置项: {:?}", raw),
            }
        }
        if out.is_empty() {
            warn!("[dns] DNS 服务器列表为空或全部非法，回退内置公共 DNS");
            out.extend(DEFAULT_DNS_SERVERS.iter().filter_map(|s| parse_server(s)));
        }
        out
    }

    /// 从配置直接构造 iroh 端点的 DNS 解析器
    ///
    /// 与 [`DnsPool::to_iroh_resolver`] 等价，但不构造 `DnsPool` —— 适用于
    /// 「只需要一个 iroh 解析器、不想为此建池」的异步路径（例如端点身份
    /// 初始化），避免创建用不到的资源。
    ///
    /// 只有 `allow_system_fallback = true` 时才会带上 `with_system_defaults()`；
    /// 默认走 `disable_fallback()`，彻底切断系统 DNS 参与。
    pub fn to_iroh_resolver(&self) -> iroh::dns::DnsResolver {
        use iroh::dns::{DnsResolver, NameserverConfig};

        let servers = self.parse_servers();
        let mut builder = DnsResolver::builder();
        for addr in &servers {
            builder = builder
                .add_nameserver_config(NameserverConfig::udp(addr.ip()).with_port(addr.port()));
        }

        if self.allow_system_fallback {
            builder = builder.with_system_defaults();
        } else {
            builder = builder.disable_fallback();
        }

        builder.build()
    }
}

/// 解析单个服务器配置项；支持 `ip` 与 `ip:port` 两种写法
fn parse_server(raw: &str) -> Option<SocketAddr> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Some(addr);
    }
    raw.parse::<IpAddr>().ok().map(|ip| SocketAddr::new(ip, 53))
}

/// 归一化主机名：去掉 IPv6 字面量的方括号与首尾空白
///
/// 调用方常从 `host:port` 字符串里切出 `[::1]` 这种形式，直接丢给解析器
/// 会被当成域名去查 DNS。
fn normalize_host(host: &str) -> String {
    host.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string()
}

/// 拆分 `host:port` 形式的端点
///
/// 支持 `host:port`、`ip:port`、`[v6]:port` 与裸 `host` / 裸 `ip`；
/// 缺端口时回退 `default_port`。返回 `None` 表示无法识别主机部分
/// （例如端口位置放了非数字），调用方应保留原值。
fn split_host_port(endpoint: &str, default_port: u16) -> Option<(String, u16)> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return None;
    }

    // 裸 IP（含无括号的 IPv6，如 `::1`）：没有端口信息
    if endpoint.parse::<IpAddr>().is_ok() {
        return Some((endpoint.to_string(), default_port));
    }

    // [v6]:port
    if let Some(rest) = endpoint.strip_prefix('[') {
        let (host, tail) = rest.split_once(']')?;
        let port = tail
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(default_port);
        return Some((host.to_string(), port));
    }

    match endpoint.rsplit_once(':') {
        Some((host, p)) => p.parse::<u16>().ok().map(|port| (host.to_string(), port)),
        // 裸主机名：没有端口信息
        None => Some((endpoint.to_string(), default_port)),
    }
}

/// DNS 解析池
///
/// 只持有 hickory 的 `TokioAsyncResolver`：**内部不含任何 tokio `Runtime`**。
///
/// 这一点是硬约束，不是风格偏好 —— hickory 的同步 `Resolver` 会在结构体里
/// 持有一个 `Mutex<Runtime>`（`hickory_resolver::resolver::Resolver.runtime`），
/// 于是「在 tokio 异步上下文里析构 `DnsPool`」会触发
/// `Cannot drop a runtime in a context where blocking is not allowed.` 的 panic；
/// 一个 `async fn` 里的局部 `DnsPool`（如 `IrohIdentity::from_pnos_node_id`）
/// 或挂在 `Arc<DnsPool>` 上的组件在异步任务里被丢弃时都会踩到。
/// 因此本类型一律保持「纯异步、零 runtime」，阻塞型调用由调用方自行
/// 用 `spawn_blocking` 承接（见 `resolve_endpoints` 的文档）。
pub struct DnsPool {
    /// 异步解析器（供 tokio 路径使用）
    resolver: TokioAsyncResolver,
    /// 实际生效的 DNS 服务器列表
    servers: Vec<SocketAddr>,
    /// 是否允许回退系统 DNS
    allow_system_fallback: bool,
}

impl DnsPool {
    /// 创建 DNS 池，使用内置默认配置（并行查询 5 个公共 DNS）
    pub fn new() -> Result<Self> {
        Self::from_config(&DnsConfig::default())
    }

    /// 按配置创建 DNS 池
    ///
    /// 未调用 `allow_system_fallback` 时**不会**读取宿主系统 DNS 配置。
    pub fn from_config(cfg: &DnsConfig) -> Result<Self> {
        let servers = cfg.parse_servers();

        let mut config = ResolverConfig::new();
        for addr in &servers {
            config.add_name_server(NameServerConfig {
                socket_addr: *addr,
                protocol: Protocol::Udp,
                tls_dns_name: None,
                trust_negative_responses: false,
                bind_addr: None,
            });
        }

        let mut opts = ResolverOpts::default();
        opts.timeout = Duration::from_secs(cfg.query_timeout_secs.max(1));
        opts.attempts = cfg.attempts.max(1);
        // A 与 AAAA 并发查询（而非"先 v4 再 v6"串行两轮）：
        // 现场出现过只拿到 AAAA、而对端网络没有 v6 路由导致连接全废的情况。
        opts.ip_strategy = LookupIpStrategy::Ipv4AndIpv6;
        // 并行查询所有 nameserver，取最快响应
        opts.num_concurrent_reqs = servers.len().max(1);

        let resolver = TokioAsyncResolver::tokio(config, opts);

        debug!(
            "[dns_pool] 已初始化 {} 个 DNS 服务器（系统 DNS 回退={}）",
            servers.len(),
            cfg.allow_system_fallback
        );

        Ok(Self {
            resolver,
            servers,
            allow_system_fallback: cfg.allow_system_fallback,
        })
    }

    /// 实际生效的 DNS 服务器列表
    pub fn servers(&self) -> &[SocketAddr] {
        &self.servers
    }

    /// 是否允许回退系统 DNS
    pub fn allow_system_fallback(&self) -> bool {
        self.allow_system_fallback
    }

    /// 解析主机名+端口为 SocketAddr 列表（异步）
    ///
    /// 并行向配置的 DNS 发查询，取第一个成功返回的结果。
    /// 解析结果由 hickory-resolver 自动缓存（默认 TTL）。
    /// 字面 IP 直接直通，不发查询。
    pub async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>> {
        if let Ok(ip) = normalize_host(host).parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        let host = normalize_host(host);

        match self.resolver.lookup_ip(&host).await {
            Ok(response) => Self::to_addrs(response.iter(), &host, port),
            Err(e) => {
                if !self.allow_system_fallback {
                    return Err(anyhow::anyhow!(
                        "DNS 解析 {} 失败（未启用系统 DNS 回退）: {}",
                        host,
                        e
                    ));
                }
                warn!("[dns_pool] {} 查询失败（{}），回退系统 DNS", host, e);
                let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
                    .await
                    .map_err(|e2| anyhow::anyhow!("系统 DNS 解析 {} 失败: {}", host, e2))?
                    .collect();
                Self::ensure_non_empty(addrs, &host)
            }
        }
    }

    /// 批量预解析 `host:port` 形式的服务列表（异步）
    ///
    /// 用于「下游只能吃 `ip:port` 字面量」的场景。典型是 STUN：其同步 UDP
    /// Binding 内部走 `to_socket_addrs()`（即系统 DNS），宿主解析器不可用时
    /// 会导致所有服务器看似都不可达。先在这里换成 `ip:port` 即可绕开。
    ///
    /// 返回值的构成由 [`DnsConfig::allow_system_fallback`] 决定：
    ///
    /// - `false`（默认）：**返回的每一项都保证是 `ip:port` 字面量**。已是字面量
    ///   的项原样返回；无法识别主机/端口或解析失败的项**直接丢弃**（WARN 明示
    ///   原因）。这条不变量是「本进程绝不读系统 DNS」的闭环保证 —— 一旦把域名
    ///   原样透传给下游，下游的 `to_socket_addrs()` 就会绕回系统解析器，等于把
    ///   刚堵上的洞重新打开。
    /// - `true`：解析失败的项保留原值，交由下游按原有方式（含系统 DNS）处理，
    ///   与开启回退前的语义一致。
    ///
    /// 缺端口的项使用 `default_port`。
    ///
    /// 本方法只做异步解析；调用方若处在同步上下文（例如 STUN 的同步 Binding），
    /// 应在 `tokio::task::spawn_blocking` 之前先 `await` 本方法拿到结果再传入。
    pub async fn resolve_endpoints(&self, endpoints: &[String], default_port: u16) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            if endpoint.trim().parse::<SocketAddr>().is_ok() {
                out.push(endpoint.clone());
                continue;
            }
            let (host, port) = match split_host_port(endpoint, default_port) {
                Some(pair) => pair,
                None => {
                    self.handle_unresolved(&mut out, endpoint, "无法识别主机/端口");
                    continue;
                }
            };
            match self.resolve(&host, port).await {
                Ok(addrs) if !addrs.is_empty() => {
                    debug!(
                        "[dns_pool] 预解析 {} -> {}（不读系统 DNS）",
                        endpoint, addrs[0]
                    );
                    out.push(addrs[0].to_string());
                }
                Ok(_) => self.handle_unresolved(&mut out, endpoint, "无解析结果"),
                Err(e) => self.handle_unresolved(&mut out, endpoint, &e.to_string()),
            }
        }
        out
    }

    /// 处置解析不出字面量的条目
    ///
    /// 严格模式（默认）丢弃，以维持「返回值只含 `ip:port` 字面量」的不变量；
    /// 只有显式允许系统 DNS 回退时才保留原值，让下游沿用旧行为。
    fn handle_unresolved(&self, out: &mut Vec<String>, endpoint: &str, reason: &str) {
        if self.allow_system_fallback {
            warn!(
                "[dns_pool] 预解析 {} 失败（{}），保留原值（已允许系统 DNS 回退）",
                endpoint, reason
            );
            out.push(endpoint.to_string());
        } else {
            warn!(
                "[dns_pool] 预解析 {} 失败（{}），已丢弃（未启用系统 DNS 回退，避免下游绕回系统解析器）",
                endpoint, reason
            );
        }
    }

    /// 构造 iroh 端点的 DNS 解析器
    ///
    /// 把它注入 `Endpoint::builder(presets::N0).dns_resolver(...)` 后，
    /// pkarr 发布/解析（HTTPS）、`DnsAddressLookup`（TXT 查询）与 DERP
    /// 中继主机名解析会全部改用本池，不再读宿主系统 DNS。
    ///
    /// 只有 `allow_system_fallback = true` 时才会带上 `with_system_defaults()`；
    /// 默认走 `disable_fallback()`，彻底切断系统 DNS 参与。
    pub fn to_iroh_resolver(&self) -> iroh::dns::DnsResolver {
        // 已经是「解析后的 SocketAddr 列表」，包成 DnsConfig 复用同一份构造逻辑，
        // 避免两处 builder 代码漂移。
        let cfg = DnsConfig {
            servers: self.servers.iter().map(|a| a.to_string()).collect(),
            allow_system_fallback: self.allow_system_fallback,
            ..DnsConfig::default()
        };
        cfg.to_iroh_resolver()
    }

    /// 把 IP 迭代器转成 SocketAddr 列表并做非空校验
    fn to_addrs(
        ips: impl Iterator<Item = IpAddr>,
        host: &str,
        port: u16,
    ) -> Result<Vec<SocketAddr>> {
        let addrs: Vec<SocketAddr> = ips.map(|ip| SocketAddr::new(ip, port)).collect();
        let addrs = Self::ensure_non_empty(addrs, host)?;
        debug!("[dns_pool] {} -> {} 个地址", host, addrs.len());
        Ok(addrs)
    }

    /// 结果非空校验
    fn ensure_non_empty(addrs: Vec<SocketAddr>, host: &str) -> Result<Vec<SocketAddr>> {
        if addrs.is_empty() {
            anyhow::bail!("DNS 解析 {} 未返回任何记录", host);
        }
        Ok(addrs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 默认配置必须使用内置 5 个公共 DNS，且默认不启用系统回退。
    #[test]
    fn test_dns_config_defaults() {
        let cfg = DnsConfig::default();
        assert_eq!(cfg.servers.len(), DEFAULT_DNS_SERVERS.len());
        assert!(!cfg.allow_system_fallback, "默认必须拒绝系统 DNS 回退");
        assert_eq!(cfg.query_timeout_secs, DEFAULT_DNS_QUERY_TIMEOUT_SECS);
        assert_eq!(cfg.attempts, DEFAULT_DNS_ATTEMPTS);

        let addrs = cfg.parse_servers();
        assert_eq!(addrs.len(), 5);
        assert!(addrs.iter().all(|a| a.port() == 53));
    }

    /// 非法项跳过、重复项去重、全非法回退内置列表。
    #[test]
    fn test_parse_servers_sanitizes() {
        let cfg = DnsConfig {
            servers: vec![
                "9.9.9.9".to_string(),      // 省略端口 → 53
                "1.2.3.4:5353".to_string(), // 自定义端口
                "9.9.9.9:53".to_string(),   // 与第一条重复
                "not-an-ip".to_string(),    // 非法
                "  ".to_string(),           // 空
            ],
            ..Default::default()
        };
        let addrs = cfg.parse_servers();
        assert_eq!(
            addrs,
            vec![
                "9.9.9.9:53".parse::<SocketAddr>().unwrap(),
                "1.2.3.4:5353".parse::<SocketAddr>().unwrap(),
            ]
        );

        let empty = DnsConfig {
            servers: vec!["bad".to_string()],
            ..Default::default()
        };
        assert_eq!(empty.parse_servers().len(), DEFAULT_DNS_SERVERS.len());
    }

    /// 字面 IP 直通，不做任何 DNS 查询（即使池指向不可达服务器）。
    ///
    /// 顺带回归一件事：池在 `#[tokio::test]` 的异步上下文里**析构**不得 panic。
    /// 历史坑：池内若持有 hickory 同步 `Resolver`（内含 `Mutex<Runtime>`），
    /// 异步上下文析构会报 `Cannot drop a runtime in a context where blocking
    /// is not allowed.`；`IrohIdentity::from_pnos_node_id` 这类 `async fn` 里的
    /// 局部池会直接把联邦初始化打崩。
    #[tokio::test]
    async fn test_resolve_literal_ip_passthrough() {
        {
            let pool = DnsPool::new().expect("DnsPool 初始化失败");
            let addrs = pool.resolve("192.168.30.51", 6885).await.unwrap();
            assert_eq!(addrs, vec!["192.168.30.51:6885".parse().unwrap()]);

            let addrs = pool.resolve("10.0.0.7", 6880).await.unwrap();
            assert_eq!(addrs, vec!["10.0.0.7:6880".parse().unwrap()]);
        } // ← 异步上下文内析构池，不得 panic
    }

    /// 池暴露的服务器列表与配置一致，且默认不允许系统回退。
    #[test]
    fn test_pool_exposes_effective_servers() {
        let cfg = DnsConfig {
            servers: vec!["223.5.5.5".to_string()],
            allow_system_fallback: false,
            ..Default::default()
        };
        let pool = DnsPool::from_config(&cfg).expect("DnsPool 初始化失败");
        assert_eq!(pool.servers().len(), 1);
        assert_eq!(pool.servers()[0].ip().to_string(), "223.5.5.5");
        assert!(!pool.allow_system_fallback());
    }

    /// IPv6 字面量（含方括号形式）必须直通，不能被当成域名去查 DNS。
    #[tokio::test]
    async fn test_resolve_ipv6_literal_passthrough() {
        let pool = DnsPool::new().expect("DnsPool 初始化失败");
        for host in ["::1", "[::1]", " [::1] ", "fe80::1"] {
            let addrs = pool.resolve(host, 6885).await.unwrap();
            assert_eq!(addrs.len(), 1, "host={host} 应直通");
            assert!(addrs[0].is_ipv6(), "host={host} 应保持 IPv6");
        }
        let addrs = pool.resolve("[2001:db8::1]", 6880).await.unwrap();
        assert_eq!(addrs, vec!["[2001:db8::1]:6880".parse().unwrap()]);
    }

    /// `host:port` 拆分的各种形态
    #[test]
    fn test_split_host_port_forms() {
        assert_eq!(
            split_host_port("example.com:6885", 0),
            Some(("example.com".to_string(), 6885))
        );
        assert_eq!(
            split_host_port(" 1.2.3.4:3478 ", 0),
            Some(("1.2.3.4".to_string(), 3478))
        );
        assert_eq!(
            split_host_port("1.2.3.4", 0),
            Some(("1.2.3.4".to_string(), 0))
        );
        assert_eq!(
            split_host_port("[2001:db8::1]:3478", 0),
            Some(("2001:db8::1".to_string(), 3478))
        );
        // 裸 IPv6：不得被误认为是 `host:port`
        assert_eq!(
            split_host_port("::1", 3478),
            Some(("::1".to_string(), 3478))
        );
        // 裸主机名：用默认端口
        assert_eq!(
            split_host_port("stun.example.com", 3478),
            Some(("stun.example.com".to_string(), 3478))
        );
        // 非法端口：无法识别
        assert_eq!(split_host_port("host:not-a-port", 0), None);
        assert_eq!(split_host_port("   ", 0), None);
    }

    /// 已是 ip:port 的项直通；严格模式下无法识别/解析失败的项**被丢弃**，
    /// 从而保证下游拿到的每一项都是字面量（不会绕回系统 DNS）。
    #[tokio::test]
    async fn test_resolve_endpoints_strict_mode_drops_unresolvable() {
        let pool = DnsPool::new().expect("DnsPool 初始化失败");
        assert!(!pool.allow_system_fallback(), "默认必须是严格模式");

        // 字面 ip:port 原样返回且不发查询
        let endpoints = vec!["192.168.30.51:6885".to_string(), "1.2.3.4:3478".to_string()];
        let out = pool.resolve_endpoints(&endpoints, 0).await;
        assert_eq!(out, endpoints, "字面 ip:port 必须原样返回且不发查询");
        // 不变量：返回值全部可解析为 SocketAddr（即不存在域名残留）
        assert!(out.iter().all(|s| s.parse::<SocketAddr>().is_ok()));

        // 无法识别端口的项被丢弃 —— 留着它下游 to_socket_addrs() 会落到系统 DNS
        let bad = vec!["host:not-a-port".to_string()];
        assert!(pool.resolve_endpoints(&bad, 0).await.is_empty());

        // 空列表不 panic
        assert!(pool.resolve_endpoints(&[], 0).await.is_empty());
    }

    /// 宽松模式（`allow_system_fallback = true`）保留解析失败的项，兼容旧行为。
    #[tokio::test]
    async fn test_resolve_endpoints_permissive_mode_keeps_unresolvable() {
        let cfg = DnsConfig {
            allow_system_fallback: true,
            ..Default::default()
        };
        let pool = DnsPool::from_config(&cfg).expect("DnsPool 初始化失败");
        let bad = vec!["host:not-a-port".to_string()];
        assert_eq!(pool.resolve_endpoints(&bad, 0).await, bad);
    }

    /// 缺端口的域名项按 `default_port` 补全（字面 IP 分支不发查询，
    /// 保证用例不依赖外网）。
    #[tokio::test]
    async fn test_resolve_endpoints_applies_default_port() {
        let pool = DnsPool::new().expect("DnsPool 初始化失败");
        let endpoints = vec!["10.1.2.3".to_string()];
        assert_eq!(
            pool.resolve_endpoints(&endpoints, 3478).await,
            vec!["10.1.2.3:3478".to_string()]
        );
    }
}
