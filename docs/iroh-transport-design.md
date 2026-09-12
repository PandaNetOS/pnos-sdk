# pnos-net Iroh 传输接入设计文档

> 版本：v1.0
> 日期：2026-09-12
> 状态：待评审

## 1. 设计目标

### 1.1 背景

pnos-net 当前传输层为自研 TCP + UDP 打洞 + 中继，存在以下问题：



* **NAT 穿透成功率低**：当前 PDC 联邦 `reachability=OutboundOnly`，公网节点无法主动连入

* **单地址无择优**：节点只有一个 SocketAddr，内外网不能同时尝试

* **明文传输**：TCP 连接无加密

* **维护成本高**：NAT / 打洞 / 中继全自研，边界情况多

### 1.2 目标

在 pnos-net 中接入 **Iroh 1.0** 作为可选传输后端，实现：



1. **NAT 穿透升级**：用 Iroh 的 QNT（QUIC-NAT-Traversal）+ DERP 中继替代自研打洞

2. **零配置发现**：Dial by Ed25519 公钥，不需要知道对端 IP

3. **多路径择优**：QUIC multipath，内外网同时连自动选优

4. **传输加密**：QUIC + Noise 加密

5. **渐进式迁移**：Auto 模式并行尝试 Iroh 和 TCP，先成功先用，不影响现有功能

6. **上层零改动**：PDC/SPDE/PK 继续调用 `NetAgent.connect_to()`，传输切换在 SDK 内部

### 1.3 非目标



* 不替换 PDC 的 BT DHT 爬虫（Iroh 无 DHT，BT DHT 是 PDC 核心业务）

* 不替换超级 Tracker（业务层，与传输无关）

* 不在第一阶段实现 Iroh gossip 替换联邦 gossip（先做传输层）



***

## 2. 架构总览

### 2.1 当前架构



```
┌─────────────────────────────────────────────────┐

│              应用层 (PDC/SPDE/PK)                │

│         NetAgent.connect\\\_to(node\\\_id, addrs)      │

└──────────────────────┬──────────────────────────┘

\&#x20;                      │

┌──────────────────────▼──────────────────────────┐

│              ConnectStrategy                     │

│  直连 → UDP打洞 → 中继                           │

└──────────────────────┬──────────────────────────┘

\&#x20;                      │

┌──────────────────────▼──────────────────────────┐

│              TcpConnection                       │

│         tokio::net::TcpStream (明文)             │

└─────────────────────────────────────────────────┘
```

### 2.2 目标架构



```
┌─────────────────────────────────────────────────┐

│              应用层 (PDC/SPDE/PK)                │

│         NetAgent.connect\\\_to(node\\\_id, addrs)      │

│                  （零改动）                       │

└──────────────────────┬──────────────────────────┘

\&#x20;                      │

┌──────────────────────▼──────────────────────────┐

│           TransportRouter (新增)                 │

│  根据配置选择：Tcp / Iroh / Auto                  │

│  Auto: 并行尝试 Iroh/TCP，先成功先用              │

└──────┬───────────────────────────┬──────────────┘

\&#x20;      │                           │

┌──────▼──────────┐     ┌──────────▼──────────────┐

│  TcpTransport   │     │  IrohTransport (新增)    │

│  (现有，保留)    │     │  QUIC + QNT + DERP       │

│  TCP + 打洞     │     │  Dial by NodeId          │

│                 │     │  加密 + multipath        │

└─────────────────┘     └─────────────────────────┘
```

### 2.3 核心设计原则



1. **Transport trait 抽象**：统一 TCP 和 Iroh 的连接接口

2. **配置驱动切换**：`transport_mode` 字段控制，默认 Auto

3. **NodeId 双映射**：pnos-net 的 20 字节 NodeId 与 Iroh 的 Ed25519 公钥互相映射

4. **发现层不变**：LPD/MQTT/DHT 发现继续用现有机制，Iroh 节点发现作为补充

5. **失败可观测**：每次连接记录使用的传输方式和失败原因



***

## 3. 核心抽象：Transport trait

### 3.1 定义位置

`crates/pnos-net/src/transport/mod.rs`（新增目录）

### 3.2 trait 定义



```
//! 传输层抽象

//!

//! 统一 TCP 和 Iroh 两种传输后端的连接接口。

//! NetAgent 通过 Transport trait 建立连接，不关心底层实现。

use std::net::SocketAddr;

use std::time::Duration;

use async\\\_trait::async\\\_trait;

use crate::types::{NodeId, Reachability};

/// 连接方式（用于日志和指标）

\\#\\\[derive(Debug, Clone, Copy, PartialEq, Eq)]

pub enum TransportKind {

\&#x20;   /// TCP 直连

\&#x20;   Tcp,

\&#x20;   /// UDP 打洞后升级 TCP

\&#x20;   UdpHolePunch,

\&#x20;   /// TCP 中继

\&#x20;   TcpRelay,

\&#x20;   /// Iroh QUIC 直连

\&#x20;   IrohDirect,

\&#x20;   /// Iroh QUIC 打洞

\&#x20;   IrohHolePunch,

\&#x20;   /// Iroh DERP 中继

\&#x20;   IrohRelay,

}

impl std::fmt::Display for TransportKind {

\&#x20;   fn fmt(\\\&self, f: \\\&mut std::fmt::Formatter<'\\\_>) -> std::fmt::Result {

\&#x20;       match self {

\&#x20;           TransportKind::Tcp => write!(f, "TCP直连"),

\&#x20;           TransportKind::UdpHolePunch => write!(f, "UDP打洞"),

\&#x20;           TransportKind::TcpRelay => write!(f, "TCP中继"),

\&#x20;           TransportKind::IrohDirect => write!(f, "Iroh直连"),

\&#x20;           TransportKind::IrohHolePunch => write!(f, "Iroh打洞"),

\&#x20;           TransportKind::IrohRelay => write!(f, "Iroh中继"),

\&#x20;       }

\&#x20;   }

}

/// 传输连接抽象

///

/// 封装底层连接（TcpStream 或 Iroh bi-stream），

/// 提供统一的读写接口。

pub trait TransportStream:

\&#x20;   tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static

{

\&#x20;   /// 获取对端地址（如有）

\&#x20;   fn peer\\\_addr(\\\&self) -> Option\\\<SocketAddr>;

\&#x20;   /// 获取本地地址（如有）

\&#x20;   fn local\\\_addr(\\\&self) -> Option\\\<SocketAddr>;

\&#x20;   /// 传输类型

\&#x20;   fn kind(\\\&self) -> TransportKind;

}

/// 连接结果

pub struct TransportConnectResult {

\&#x20;   /// 已建立的连接流

\&#x20;   pub stream: Box\\\<dyn TransportStream>,

\&#x20;   /// 实际连接成功的地址

\&#x20;   pub connected\\\_addr: Option\\\<SocketAddr>,

\&#x20;   /// 连接耗时

\&#x20;   pub latency: Duration,

\&#x20;   /// 使用的传输方式

\&#x20;   pub kind: TransportKind,

}

/// 传输后端 trait

\\#\\\[async\\\_trait]

pub trait Transport: Send + Sync {

\&#x20;   /// 传输类型

\&#x20;   fn kind(\\\&self) -> TransportKind;

\&#x20;   /// 建立到对端的连接

\&#x20;   ///

\&#x20;   /// - \\\`node\\\_id\\\`: 对端节点 ID（Iroh 模式下用于 dial-by-key）

\&#x20;   /// - \\\`addrs\\\`: 对端已知地址列表（TCP 模式下使用）

\&#x20;   /// - \\\`reachability\\\`: 对端可达性

\&#x20;   /// - \\\`nat\\\_type\\\`: 对端 NAT 类型（可选）

\&#x20;   async fn connect(

\&#x20;       \\\&self,

\&#x20;       node\\\_id: NodeId,

\&#x20;       addrs: &\\\[SocketAddr],

\&#x20;       reachability: Reachability,

\&#x20;       nat\\\_type: Option\\\<String>,

\&#x20;   ) -> anyhow::Result\\\<TransportConnectResult>;

\&#x20;   /// 启动传输后端（监听端口、初始化 Iroh endpoint 等）

\&#x20;   async fn start(\\\&self) -> anyhow::Result<()>;

\&#x20;   /// 停止传输后端

\&#x20;   async fn stop(\\\&self);

}
```



***

## 4. Iroh 传输实现

### 4.1 文件结构



```
crates/pnos-net/src/transport/

├── mod.rs              # Transport trait 定义（见上）

├── tcp.rs              # TcpTransport 实现（从现有 connector/strategy 迁移）

├── iroh.rs             # IrohTransport 实现（新增）

└── router.rs           # TransportRouter（新增，Auto 模式调度）
```

### 4.2 IrohTransport 实现



```
//! Iroh 传输后端

//!

//! 基于 Iroh 1.0 的 QUIC 传输，支持：

//! - Dial by NodeId（不需要知道 IP）

//! - QNT NAT 穿透

//! - DERP 中继兜底

//! - QUIC multipath（内外网同时连）

//! - Noise 加密

use std::net::SocketAddr;

use std::sync::Arc;

use std::time::{Duration, Instant};

use async\\\_trait::async\\\_trait;

use parking\\\_lot::RwLock;

use tracing::{debug, info, warn};

use super::{Transport, TransportConnectResult, TransportKind, TransportStream};

use crate::types::{NodeId, Reachability};

/// Iroh 传输配置

\\#\\\[derive(Debug, Clone)]

pub struct IrohTransportConfig {

\&#x20;   /// 本机节点 ID（20字节，用于派生 Iroh NodeId）

\&#x20;   pub node\\\_id: \\\[u8; 20],

\&#x20;   /// 监听端口（UDP，QUIC 基于 UDP）

\&#x20;   pub listen\\\_port: u16,

\&#x20;   /// 数据目录（Iroh 持久化节点身份和 peer 缓存）

\&#x20;   pub data\\\_dir: std::path::PathBuf,

\&#x20;   /// 是否启用 DERP 中继（默认 true）

\&#x20;   pub derp\\\_enabled: bool,

\&#x20;   /// 自定义 DERP 服务器列表（空则用官方）

\&#x20;   pub derp\\\_urls: Vec\\\<String>,

\&#x20;   /// 连接超时（默认 10 秒）

\&#x20;   pub connect\\\_timeout: Duration,

\&#x20;   /// ALPN 协议标识（用于区分 pnos 联邦和其他 iroh 应用）

\&#x20;   pub alpn: Vec\\\<u8>,

}

impl Default for IrohTransportConfig {

\&#x20;   fn default() -> Self {

\&#x20;       Self {

\&#x20;           node\\\_id: \\\[0u8; 20],

\&#x20;           listen\\\_port: 6885,

\&#x20;           data\\\_dir: std::path::PathBuf::from("./data"),

\&#x20;           derp\\\_enabled: true,

\&#x20;           derp\\\_urls: vec!\\\[],

\&#x20;           connect\\\_timeout: Duration::from\\\_secs(10),

\&#x20;           alpn: b"pnos/federation/1".to\\\_vec(),

\&#x20;       }

\&#x20;   }

}

/// Iroh 节点身份映射

///

/// pnos-net 的 NodeId 是 20 字节（与 BT DHT 兼容），

/// Iroh 的 NodeId 是 Ed25519 公钥（32字节）。

///

/// 映射策略：从 pnos NodeId 派生确定性的 Ed25519 密钥对。

/// 这样同一个 pnos NodeId 永远对应同一个 Iroh NodeId，

/// 对端可以通过 pnos NodeId 直接 dial。

pub struct IrohIdentity {

\&#x20;   /// pnos 20字节 NodeId

\&#x20;   pub pnos\\\_node\\\_id: \\\[u8; 20],

\&#x20;   /// Iroh 端点（包含密钥对）

\&#x20;   pub endpoint: iroh::Endpoint,

\&#x20;   /// 派生的 Iroh NodeId（32字节公钥）

\&#x20;   pub iroh\\\_node\\\_id: iroh::NodeId,

}

impl IrohIdentity {

\&#x20;   /// 从 pnos NodeId 派生 Iroh 身份

\&#x20;   ///

\&#x20;   /// 使用 HKDF 从 20 字节 NodeId 派生 Ed25519 种子，

\&#x20;   /// 确保确定性：相同 NodeId 永远生成相同密钥对。

\&#x20;   pub fn from\\\_pnos\\\_node\\\_id(node\\\_id: \\\[u8; 20], data\\\_dir: \\\&std::path::Path) -> anyhow::Result\\\<Self> {

\&#x20;       use iroh::NodeId;

\&#x20;       // 尝试从磁盘加载已有的密钥对

\&#x20;       let key\\\_path = data\\\_dir.join("iroh\\\_identity");

\&#x20;       let secret\\\_key = if key\\\_path.exists() {

\&#x20;           // 加载已有密钥

\&#x20;           let bytes = std::fs::read(\\\&key\\\_path)?;

\&#x20;           iroh::SecretKey::from\\\_bytes(bytes.try\\\_into().map\\\_err(|\\\_| {

\&#x20;               anyhow::anyhow!("Iroh 密钥文件格式错误")

\&#x20;           })?)

\&#x20;       } else {

\&#x20;           // 从 pnos NodeId 派生新密钥

\&#x20;           // 使用 HKDF-SHA256 从 20 字节派生 32 字节种子

\&#x20;           let seed = Self::derive\\\_seed(\\\&node\\\_id);

\&#x20;           let sk = iroh::SecretKey::from\\\_bytes(seed);

\&#x20;           // 持久化

\&#x20;           std::fs::create\\\_dir\\\_all(data\\\_dir)?;

\&#x20;           std::fs::write(\\\&key\\\_path, sk.to\\\_bytes())?;

\&#x20;           sk

\&#x20;       };

\&#x20;       let iroh\\\_node\\\_id = secret\\\_key.public();

\&#x20;       Ok(Self {

\&#x20;           pnos\\\_node\\\_id: node\\\_id,

\&#x20;           endpoint: iroh::Endpoint::builder()

\&#x20;               .secret\\\_key(secret\\\_key)

\&#x20;               .bind()

\&#x20;               .map\\\_err(|e| anyhow::anyhow!("Iroh Endpoint 绑定失败: {}", e))?,

\&#x20;           iroh\\\_node\\\_id,

\&#x20;       })

\&#x20;   }

\&#x20;   /// HKDF 派生 32 字节种子

\&#x20;   fn derive\\\_seed(node\\\_id: &\\\[u8; 20]) -> \\\[u8; 32] {

\&#x20;       use hkdf::Hkdf;

\&#x20;       use sha2::Sha256;

\&#x20;       let hk = Hkdf::\\\<Sha256>::new(Some(b"pnos-iroh-identity-v1"), node\\\_id);

\&#x20;       let mut seed = \\\[0u8; 32];

\&#x20;       hk.expand(b"ed25519-seed", \\\&mut seed)

\&#x20;           .expect("HKDF expand 失败");

\&#x20;       seed

\&#x20;   }

\&#x20;   /// pnos NodeId → Iroh NodeId 转换（对端拨号用）

\&#x20;   pub fn pnos\\\_to\\\_iroh\\\_node\\\_id(node\\\_id: \\\&NodeId) -> anyhow::Result\\\<iroh::NodeId> {

\&#x20;       // 对端的 Iroh NodeId 也需要从 pnos NodeId 派生

\&#x20;       // 但对端的私钥我们不知道，只能派生公钥

\&#x20;       // 这要求双方使用相同的派生算法

\&#x20;       let seed = Self::derive\\\_seed(\\\&node\\\_id.0);

\&#x20;       let secret = iroh::SecretKey::from\\\_bytes(seed);

\&#x20;       Ok(secret.public())

\&#x20;   }

}

/// Iroh 连接流（封装 bi-stream）

pub struct IrohStream {

\&#x20;   inner: iroh::endpoint::BidiStream,

\&#x20;   kind: TransportKind,

}

impl tokio::io::AsyncRead for IrohStream {

\&#x20;   fn poll\\\_read(

\&#x20;       mut self: std::pin::Pin<\\\&mut Self>,

\&#x20;       cx: \\\&mut std::task::Context<'\\\_>,

\&#x20;       buf: \\\&mut tokio::io::ReadBuf<'\\\_>,

\&#x20;   ) -> std::task::Poll\\\<std::io::Result<()>> {

\&#x20;       std::pin::Pin::new(\\\&mut self.inner.0).poll\\\_read(cx, buf)

\&#x20;   }

}

impl tokio::io::AsyncWrite for IrohStream {

\&#x20;   fn poll\\\_write(

\&#x20;       mut self: std::pin::Pin<\\\&mut Self>,

\&#x20;       cx: \\\&mut std::task::Context<'\\\_>,

\&#x20;       buf: &\\\[u8],

\&#x20;   ) -> std::task::Poll\\\<std::io::Result\\\<usize>> {

\&#x20;       std::pin::Pin::new(\\\&mut self.inner.1).poll\\\_write(cx, buf)

\&#x20;   }

\&#x20;   fn poll\\\_flush(

\&#x20;       mut self: std::pin::Pin<\\\&mut Self>,

\&#x20;       cx: \\\&mut std::task::Context<'\\\_>,

\&#x20;   ) -> std::task::Poll\\\<std::io::Result<()>> {

\&#x20;       std::pin::Pin::new(\\\&mut self.inner.1).poll\\\_flush(cx)

\&#x20;   }

\&#x20;   fn poll\\\_shutdown(

\&#x20;       mut self: std::pin::Pin<\\\&mut Self>,

\&#x20;       cx: \\\&mut std::task::Context<'\\\_>,

\&#x20;   ) -> std::task::Poll\\\<std::io::Result<()>> {

\&#x20;       std::pin::Pin::new(\\\&mut self.inner.1).poll\\\_shutdown(cx)

\&#x20;   }

}

impl TransportStream for IrohStream {

\&#x20;   fn peer\\\_addr(\\\&self) -> Option\\\<SocketAddr> {

\&#x20;       // Iroh 连接可能是 multipath，对端地址不唯一

\&#x20;       // 返回 None 表示不适用

\&#x20;       None

\&#x20;   }

\&#x20;   fn local\\\_addr(\\\&self) -> Option\\\<SocketAddr> {

\&#x20;       None

\&#x20;   }

\&#x20;   fn kind(\\\&self) -> TransportKind {

\&#x20;       self.kind

\&#x20;   }

}

/// Iroh 传输后端

pub struct IrohTransport {

\&#x20;   config: IrohTransportConfig,

\&#x20;   identity: RwLock\\\<Option\\\<Arc\\\<IrohIdentity>>>,

}

impl IrohTransport {

\&#x20;   pub fn new(config: IrohTransportConfig) -> Self {

\&#x20;       Self {

\&#x20;           config,

\&#x20;           identity: RwLock::new(None),

\&#x20;       }

\&#x20;   }

\&#x20;   /// 获取 Iroh 端点（需先 start）

\&#x20;   fn endpoint(\\\&self) -> anyhow::Result\\\<Arc\\\<IrohIdentity>> {

\&#x20;       self.identity

\&#x20;           .read()

\&#x20;           .clone()

\&#x20;           .ok\\\_or\\\_else(|| anyhow::anyhow!("IrohTransport 未启动"))

\&#x20;   }

}

\\#\\\[async\\\_trait]

impl Transport for IrohTransport {

\&#x20;   fn kind(\\\&self) -> TransportKind {

\&#x20;       TransportKind::IrohDirect

\&#x20;   }

\&#x20;   async fn start(\\\&self) -> anyhow::Result<()> {

\&#x20;       let identity = IrohIdentity::from\\\_pnos\\\_node\\\_id(

\&#x20;           self.config.node\\\_id,

\&#x20;           \\\&self.config.data\\\_dir,

\&#x20;       )?;

\&#x20;       info!(

\&#x20;           "\\\[iroh-transport] Iroh 端点已启动, pnos\\\_node\\\_id={}, iroh\\\_node\\\_id={}",

\&#x20;           hex::encode(self.config.node\\\_id),

\&#x20;           identity.iroh\\\_node\\\_id,

\&#x20;       );

\&#x20;       \\\*self.identity.write() = Some(Arc::new(identity));

\&#x20;       Ok(())

\&#x20;   }

\&#x20;   async fn stop(\\\&self) {

\&#x20;       if let Some(identity) = self.identity.write().take() {

\&#x20;           info!("\\\[iroh-transport] Iroh 端点关闭");

\&#x20;           // identity drop 时自动关闭 endpoint

\&#x20;       }

\&#x20;   }

\&#x20;   async fn connect(

\&#x20;       \\\&self,

\&#x20;       node\\\_id: NodeId,

\&#x20;       addrs: &\\\[SocketAddr],

\&#x20;       \\\_reachability: Reachability,

\&#x20;       \\\_nat\\\_type: Option\\\<String>,

\&#x20;   ) -> anyhow::Result\\\<TransportConnectResult> {

\&#x20;       let identity = self.endpoint()?;

\&#x20;       let start = Instant::now();

\&#x20;       // 1. 从 pnos NodeId 派生 Iroh NodeId

\&#x20;       let iroh\\\_node\\\_id = IrohIdentity::pnos\\\_to\\\_iroh\\\_node\\\_id(\\\&node\\\_id)?;

\&#x20;       // 2. 如果有已知地址，先尝试直接连接（更快）

\&#x20;       //    Iroh 内部会同时尝试直连和 DERP 中继

\&#x20;       let mut connection = None;

\&#x20;       // 尝试用已知地址直连

\&#x20;       for addr in addrs {

\&#x20;           match tokio::time::timeout(

\&#x20;               self.config.connect\\\_timeout,

\&#x20;               identity.endpoint.connect(iroh\\\_node\\\_id, self.config.alpn.clone()),

\&#x20;           )

\&#x20;           .await

\&#x20;           {

\&#x20;               Ok(Ok(conn)) => {

\&#x20;                   debug!("\\\[iroh-transport] 连接成功: node\\\_id={}", node\\\_id);

\&#x20;                   connection = Some(conn);

\&#x20;                   break;

\&#x20;               }

\&#x20;               Ok(Err(e)) => {

\&#x20;                   debug!("\\\[iroh-transport] 连接失败: {}, 尝试下一个地址", e);

\&#x20;               }

\&#x20;               Err(\\\_) => {

\&#x20;                   debug!("\\\[iroh-transport] 连接超时: {}", addr);

\&#x20;               }

\&#x20;           }

\&#x20;       }

\&#x20;       // 3. 如果已知地址都失败，用 Iroh 的节点发现（DERP 中继）

\&#x20;       let connection = match connection {

\&#x20;           Some(c) => c,

\&#x20;           None => {

\&#x20;               debug!("\\\[iroh-transport] 已知地址均失败，尝试 Iroh 节点发现");

\&#x20;               tokio::time::timeout(

\&#x20;                   self.config.connect\\\_timeout,

\&#x20;                   identity.endpoint.connect(iroh\\\_node\\\_id, self.config.alpn.clone()),

\&#x20;               )

\&#x20;               .await

\&#x20;               .map\\\_err(|\\\_| anyhow::anyhow!("Iroh 连接超时"))?

\&#x20;               .map\\\_err(|e| anyhow::anyhow!("Iroh 连接失败: {}", e))?

\&#x20;           }

\&#x20;       };

\&#x20;       // 4. 打开 bi-stream

\&#x20;       let (send, recv) = connection

\&#x20;           .open\\\_bi()

\&#x20;           .await

\&#x20;           .map\\\_err(|e| anyhow::anyhow!("Iroh 打开流失败: {}", e))?;

\&#x20;       let latency = start.elapsed();

\&#x20;       // 5. 判断连接方式（直连/打洞/中继）

\&#x20;       // Iroh 内部会自动选择，这里通过连接信息判断

\&#x20;       let kind = if connection.remote\\\_addresses().next().is\\\_some() {

\&#x20;           TransportKind::IrohDirect

\&#x20;       } else {

\&#x20;           TransportKind::IrohRelay

\&#x20;       };

\&#x20;       Ok(TransportConnectResult {

\&#x20;           stream: Box::new(IrohStream {

\&#x20;               inner: (recv, send),

\&#x20;               kind,

\&#x20;           }),

\&#x20;           connected\\\_addr: addrs.first().copied(),

\&#x20;           latency,

\&#x20;           kind,

\&#x20;       })

\&#x20;   }

}
```



***

## 5. TransportRouter：Auto 模式调度

### 5.1 设计

Auto 模式下，并行尝试 Iroh 和 TCP，哪个先成功用哪个。记录每次连接的传输方式和耗时，用于后续优化。

### 5.2 实现



```
//! 传输路由器

//!

//! 根据配置选择传输后端，Auto 模式下并行尝试 Iroh 和 TCP，先成功先用。

use std::net::SocketAddr;

use std::sync::Arc;

use std::time::Duration;

use async\\\_trait::async\\\_trait;

use parking\\\_lot::RwLock;

use tracing::{debug, info, warn};

use super::{

\&#x20;   iroh::{IrohTransport, IrohTransportConfig},

\&#x20;   tcp::TcpTransport,

\&#x20;   Transport, TransportConnectResult, TransportKind,

};

use crate::types::{NodeId, Reachability};

/// 传输模式

\\#\\\[derive(Debug, Clone, Copy, PartialEq, Eq)]

pub enum TransportMode {

\&#x20;   /// 仅 TCP（现有行为）

\&#x20;   TcpOnly,

\&#x20;   /// 仅 Iroh

\&#x20;   IrohOnly,

\&#x20;   /// Auto：并行尝试 Iroh/TCP，先成功先用（默认）

\&#x20;   Auto,

}

impl Default for TransportMode {

\&#x20;   fn default() -> Self {

\&#x20;       TransportMode::Auto

\&#x20;   }

}

/// 传输统计

\\#\\\[derive(Debug, Clone, Default)]

pub struct TransportStats {

\&#x20;   pub tcp\\\_attempts: u64,

\&#x20;   pub tcp\\\_success: u64,

\&#x20;   pub iroh\\\_attempts: u64,

\&#x20;   pub iroh\\\_success: u64,

\&#x20;   pub auto\\\_fallback\\\_count: u64,

\&#x20;   pub total\\\_latency\\\_tcp\\\_ms: u64,

\&#x20;   pub total\\\_latency\\\_iroh\\\_ms: u64,

}

/// 传输路由器

pub struct TransportRouter {

\&#x20;   mode: TransportMode,

\&#x20;   tcp: Option\\\<Arc\\\<TcpTransport>>,

\&#x20;   iroh: Option\\\<Arc\\\<IrohTransport>>,

\&#x20;   stats: RwLock\\\<TransportStats>,

}

impl TransportRouter {

\&#x20;   pub fn new(mode: TransportMode) -> Self {

\&#x20;       Self {

\&#x20;           mode,

\&#x20;           tcp: None,

\&#x20;           iroh: None,

\&#x20;           stats: RwLock::new(TransportStats::default()),

\&#x20;       }

\&#x20;   }

\&#x20;   pub fn with\\\_tcp(mut self, tcp: TcpTransport) -> Self {

\&#x20;       self.tcp = Some(Arc::new(tcp));

\&#x20;       self

\&#x20;   }

\&#x20;   pub fn with\\\_iroh(mut self, iroh: IrohTransport) -> Self {

\&#x20;       self.iroh = Some(Arc::new(iroh));

\&#x20;       self

\&#x20;   }

\&#x20;   pub fn stats(\\\&self) -> TransportStats {

\&#x20;       self.stats.read().clone()

\&#x20;   }

\&#x20;   async fn connect\\\_with(

\&#x20;       \\\&self,

\&#x20;       transport: \\\&dyn Transport,

\&#x20;       node\\\_id: NodeId,

\&#x20;       addrs: &\\\[SocketAddr],

\&#x20;       reachability: Reachability,

\&#x20;       nat\\\_type: Option\\\<String>,

\&#x20;       is\\\_iroh: bool,

\&#x20;   ) -> anyhow::Result\\\<TransportConnectResult> {

\&#x20;       let result = transport

\&#x20;           .connect(node\\\_id, addrs, reachability, nat\\\_type.clone())

\&#x20;           .await;

\&#x20;       let mut stats = self.stats.write();

\&#x20;       if is\\\_iroh {

\&#x20;           stats.iroh\\\_attempts += 1;

\&#x20;           if result.is\\\_ok() {

\&#x20;               stats.iroh\\\_success += 1;

\&#x20;               stats.total\\\_latency\\\_iroh\\\_ms +=

\&#x20;                   result.as\\\_ref().unwrap().latency.as\\\_millis() as u64;

\&#x20;           }

\&#x20;       } else {

\&#x20;           stats.tcp\\\_attempts += 1;

\&#x20;           if result.is\\\_ok() {

\&#x20;               stats.tcp\\\_success += 1;

\&#x20;               stats.total\\\_latency\\\_tcp\\\_ms +=

\&#x20;                   result.as\\\_ref().unwrap().latency.as\\\_millis() as u64;

\&#x20;           }

\&#x20;       }

\&#x20;       result

\&#x20;   }

}

\\#\\\[async\\\_trait]

impl Transport for TransportRouter {

\&#x20;   fn kind(\\\&self) -> TransportKind {

\&#x20;       // Router 本身不代表具体传输方式

\&#x20;       TransportKind::Tcp

\&#x20;   }

\&#x20;   async fn start(\\\&self) -> anyhow::Result<()> {

\&#x20;       if let Some(ref tcp) = self.tcp {

\&#x20;           tcp.start().await?;

\&#x20;       }

\&#x20;       if let Some(ref iroh) = self.iroh {

\&#x20;           if let Err(e) = iroh.start().await {

\&#x20;               warn!("\\\[transport-router] Iroh 启动失败，将仅使用 TCP: {}", e);

\&#x20;           }

\&#x20;       }

\&#x20;       Ok(())

\&#x20;   }

\&#x20;   async fn stop(\\\&self) {

\&#x20;       if let Some(ref iroh) = self.iroh {

\&#x20;           iroh.stop().await;

\&#x20;       }

\&#x20;       if let Some(ref tcp) = self.tcp {

\&#x20;           tcp.stop().await;

\&#x20;       }

\&#x20;   }

\&#x20;   async fn connect(

\&#x20;       \\\&self,

\&#x20;       node\\\_id: NodeId,

\&#x20;       addrs: &\\\[SocketAddr],

\&#x20;       reachability: Reachability,

\&#x20;       nat\\\_type: Option\\\<String>,

\&#x20;   ) -> anyhow::Result\\\<TransportConnectResult> {

\&#x20;       match self.mode {

\&#x20;           TransportMode::TcpOnly => {

\&#x20;               let tcp = self

\&#x20;                   .tcp

\&#x20;                   .as\\\_ref()

\&#x20;                   .ok\\\_or\\\_else(|| anyhow::anyhow!("TcpOnly 模式但未配置 TcpTransport"))?;

\&#x20;               self.connect\\\_with(tcp.as\\\_ref(), node\\\_id, addrs, reachability, nat\\\_type, false)

\&#x20;                   .await

\&#x20;           }

\&#x20;           TransportMode::IrohOnly => {

\&#x20;               let iroh = self

\&#x20;                   .iroh

\&#x20;                   .as\\\_ref()

\&#x20;                   .ok\\\_or\\\_else(|| anyhow::anyhow!("IrohOnly 模式但未配置 IrohTransport"))?;

\&#x20;               self.connect\\\_with(iroh.as\\\_ref(), node\\\_id, addrs, reachability, nat\\\_type, true)

\&#x20;                   .await

\&#x20;           }

\&#x20;           TransportMode::Auto => {

&#x20;               // 并行尝试 Iroh 和 TCP，哪个先成功用哪个

&#x20;               let iroh\_fut = async {

&#x20;                   if let Some(ref iroh) = self.iroh {

&#x20;                       self.connect\_with(

&#x20;                           iroh.as\_ref(),

&#x20;                           node\_id,

&#x20;                           addrs,

&#x20;                           reachability,

&#x20;                           nat\_type.clone(),

&#x20;                           true,

&#x20;                       )

&#x20;                       .await

&#x20;                   } else {

&#x20;                       Err(anyhow::anyhow!("IrohTransport 未配置"))

&#x20;                   }

&#x20;               };

&#x20;               let tcp\_fut = async {

&#x20;                   if let Some(ref tcp) = self.tcp {

&#x20;                       self.connect\_with(

&#x20;                           tcp.as\_ref(),

&#x20;                           node\_id,

&#x20;                           addrs,

&#x20;                           reachability,

&#x20;                           nat\_type.clone(),

&#x20;                           false,

&#x20;                       )

&#x20;                       .await

&#x20;                   } else {

&#x20;                       Err(anyhow::anyhow!("TcpTransport 未配置"))

&#x20;                   }

&#x20;               };

&#x20;               tokio::select! {

&#x20;                   result = iroh\_fut => {

&#x20;                       match result {

&#x20;                           Ok(r) => {

&#x20;                               debug!("\[transport-router] Iroh 连接先成功");

&#x20;                               Ok(r)

&#x20;                           }

&#x20;                           Err(e) => {

&#x20;                               debug!("\[transport-router] Iroh 失败({})，等待 TCP", e);

&#x20;                               tcp\_fut.await

&#x20;                           }

&#x20;                       }

&#x20;                   }

&#x20;                   result = tcp\_fut => {

&#x20;                       match result {

&#x20;                           Ok(r) => {

&#x20;                               debug!("\[transport-router] TCP 连接先成功");

&#x20;                               Ok(r)

&#x20;                           }

&#x20;                           Err(e) => {

&#x20;                               debug!("\[transport-router] TCP 失败({})，等待 Iroh", e);

&#x20;                               iroh\_fut.await

&#x20;                           }

&#x20;                       }

&#x20;                   }

&#x20;               }

&#x20;           }

\&#x20;       }

\&#x20;   }

}
```



***

## 6. NetAgent 集成

### 6.1 配置变更

在 `NetAgentConfig` 中增加传输模式配置：



```
/// NetAgent 配置

pub struct NetAgentConfig {

\&#x20;   // ... 现有字段 ...

\&#x20;   /// 传输模式（默认 Auto）

\&#x20;   #\\\[serde(default)]

\&#x20;   pub transport\\\_mode: TransportMode,

\&#x20;   /// Iroh 传输配置（transport\\\_mode != TcpOnly 时使用）

\&#x20;   #\\\[serde(default)]

\&#x20;   pub iroh\\\_config: Option\\\<IrohTransportConfig>,

}
```

### 6.2 NetAgent 内部变更



```
pub struct NetAgent {

\&#x20;   // ... 现有字段 ...

\&#x20;   /// 传输路由器（替代直接调用 strategy）

\&#x20;   transport: Arc\\\<TransportRouter>,

}
```

`connect_to()` 方法从调用 `strategy.connect()` 改为调用 `transport.connect()`：



```
pub async fn connect\\\_to(

\&#x20;   \\\&self,

\&#x20;   node\\\_id: NodeId,

\&#x20;   addrs: &\\\[SocketAddr],

\&#x20;   peer\\\_reachability: Reachability,

\&#x20;   peer\\\_nat\\\_type: Option\\\<String>,

) -> anyhow::Result\\\<ConnectResult> {

\&#x20;   // ... 参数校验 ...

\&#x20;   match self

\&#x20;       .transport

\&#x20;       .connect(node\\\_id, addrs, peer\\\_reachability, peer\\\_nat\\\_type)

\&#x20;       .await

\&#x20;   {

\&#x20;       Ok(result) => {

\&#x20;           // 转换为现有 ConnectResult 格式

\&#x20;           let connect\\\_result = ConnectResult {

\&#x20;               connection: TcpConnection {

\&#x20;                   // 需要适配：TransportStream → TcpConnection

\&#x20;                   // 或者将 ConnectResult.connection 改为 Box\\\<dyn TransportStream>

\&#x20;                   stream: todo!(),

\&#x20;                   connected\\\_addr: result.connected\\\_addr.unwrap\\\_or(addrs\\\[0]),

\&#x20;                   latency: result.latency,

\&#x20;               },

\&#x20;               method: match result.kind {

\&#x20;                   TransportKind::Tcp => ConnectMethod::TcpDirect,

\&#x20;                   TransportKind::UdpHolePunch => ConnectMethod::UdpHolePunch,

\&#x20;                   TransportKind::TcpRelay => ConnectMethod::Relay,

\&#x20;                   \\\_ => ConnectMethod::TcpDirect, // Iroh 的方式需要扩展 ConnectMethod

\&#x20;               },

\&#x20;               total\\\_latency: result.latency,

\&#x20;           };

\&#x20;           Ok(connect\\\_result)

\&#x20;       }

\&#x20;       Err(e) => Err(e),

\&#x20;   }

}
```

### 6.3 ConnectResult 适配

为了最小化上层改动，`ConnectResult.connection` 从 `TcpConnection` 改为 `Box<dyn TransportStream>`：



```
pub struct ConnectResult {

\&#x20;   /// 已建立的连接流（统一抽象）

\&#x20;   pub connection: Box\\\<dyn TransportStream>,

\&#x20;   /// 实际使用的连接方式

\&#x20;   pub method: ConnectMethod,

\&#x20;   /// 总耗时

\&#x20;   pub total\\\_latency: Duration,

}
```

> **注意**
> ：这是一个 breaking change，PDC 联邦层的
> `Connection::new()`
> 需要适配。但改动量很小，只是把
> `TcpStream`
> 换成
> `Box<dyn TransportStream>`
> 。



***

## 7. NodeId 映射方案

### 7.1 问题



* pnos-net NodeId：20 字节（与 BT DHT 兼容）

* Iroh NodeId：Ed25519 公钥，32 字节

### 7.2 方案：确定性派生

使用 HKDF-SHA256 从 20 字节 pnos NodeId 派生 32 字节 Ed25519 种子：



```
seed = HKDF-SHA256(salt="pnos-iroh-identity-v1", ikm=pnos\\\_node\\\_id, info="ed25519-seed")

ed25519\\\_keypair = Ed25519(seed)

iroh\\\_node\\\_id = ed25519\\\_keypair.public\\\_key
```

**优点**：



* 确定性：相同 pnos NodeId 永远生成相同 Iroh NodeId

* 对端可以通过 pnos NodeId 直接计算出 Iroh NodeId，不需要额外的映射交换

* 私钥持久化到磁盘，重启不丢失

**缺点**：



* pnos NodeId 不是高熵随机数（可能是 DHT node\_id），派生的密钥安全性依赖 HKDF

* 如果 pnos NodeId 被猜到，Iroh 私钥也会被猜到（但 pnos NodeId 本身就是公开的）

### 7.3 备选方案：映射表交换

在联邦握手时交换 `pnos_node_id → iroh_node_id` 映射表。

**优点**：Iroh 密钥对独立生成，安全性更高

**缺点**：需要额外的握手逻辑，冷启动时不知道对端 Iroh NodeId

### 7.4 决策

第一阶段用**确定性派生**，简单且零配置。后续如果有安全需求再切换到映射表交换。



***

## 8. 节点发现整合

### 8.1 现有发现机制（保留）



* **LPD**（局域网多播）：继续使用，发现局域网节点

* **MQTT Rendezvous**：公网节点发现，继续使用

* **DHT 发现**（PDC 联邦）：BT DHT 魔法 infohash，继续使用

### 8.2 Iroh 节点发现（补充）

Iroh 内置节点发现（通过 DERP 中继的 node lookup），作为现有发现机制的补充：



* 当已知对端 pnos NodeId 但不知道地址时，Iroh 可以通过 DERP 网络找到对端

* 这是 Iroh 的 "dial by key" 能力，不需要额外配置

### 8.3 发现事件统一

所有发现机制（LPD/MQTT/DHT/Iroh）都输出 `DiscoveredNode` 事件，NetAgent 统一处理：



```
pub struct DiscoveredNode {

\&#x20;   pub node\\\_id: NodeId,

\&#x20;   pub addresses: Vec\\\<SocketAddr>,

\&#x20;   pub reachability: Reachability,

\&#x20;   pub nat\\\_type: Option\\\<String>,

\&#x20;   /// 发现来源（用于调试）

\&#x20;   pub source: &'static str, // "lpd" / "mqtt" / "dht" / "iroh"

}
```



***

## 9. NAT 穿透策略

### 9.1 Iroh 模式下

Iroh 内置 QNT + DERP 中继，**不需要** pnos-net 的自研 NAT 模块：



* `nat_enabled: false`（关闭自研 NAT）

* `hole_punch_enabled: false`（关闭自研打洞）

* Iroh 内部自动处理 STUN / 打洞 / 中继

### 9.2 TCP 模式下

继续使用现有自研 NAT 模块（STUN + NAT-PMP + PCP + UDP 打洞）。

### 9.3 Auto 模式下



* Iroh 连接时用 Iroh 的 NAT 穿透

* TCP 回退时用自研 NAT 穿透

* 两者独立运行，互不干扰



***

## 10. 配置设计

### 10.1 NetAgentConfig 新增字段



```
pub struct NetAgentConfig {

\&#x20;   // ... 现有字段 ...

\&#x20;   /// 传输模式

\&#x20;   #\\\[serde(default = "default\\\_transport\\\_mode")]

\&#x20;   pub transport\\\_mode: TransportMode,

\&#x20;   /// Iroh 配置（transport\\\_mode != TcpOnly 时使用）

\&#x20;   #\\\[serde(default)]

\&#x20;   pub iroh: Option\\\<IrohConfig>,

}

fn default\\\_transport\\\_mode() -> TransportMode {

\&#x20;   TransportMode::Auto

}

/// Iroh 传输配置

\\#\\\[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]

pub struct IrohConfig {

\&#x20;   /// 监听端口（UDP，默认与 listen\\\_port 相同）

\&#x20;   #\\\[serde(default = "default\\\_iroh\\\_port")]

\&#x20;   pub listen\\\_port: u16,

\&#x20;   /// 是否启用 DERP 中继

\&#x20;   #\\\[serde(default = "default\\\_true")]

\&#x20;   pub derp\\\_enabled: bool,

\&#x20;   /// 自定义 DERP 服务器列表

\&#x20;   #\\\[serde(default)]

\&#x20;   pub derp\\\_urls: Vec\\\<String>,

\&#x20;   /// 连接超时（秒）

\&#x20;   #\\\[serde(default = "default\\\_iroh\\\_timeout")]

\&#x20;   pub connect\\\_timeout\\\_secs: u64,

\&#x20;   /// ALPN 协议标识

\&#x20;   #\\\[serde(default = "default\\\_alpn")]

\&#x20;   pub alpn: String,

}
```

### 10.2 PDC 配置示例



```
federation:

\&#x20; enabled: true

\&#x20; listen\\\_port: 6885

\&#x20; # 传输模式：auto / tcp\\\_only / iroh\\\_only

\&#x20; transport\\\_mode: auto

\&#x20; iroh:

\&#x20;   derp\\\_enabled: true

\&#x20;   connect\\\_timeout\\\_secs: 10
```



***

## 11. 分阶段迁移计划

### 阶段 1：基础设施（1-2 天）



* [ ] 新增 `transport/` 模块，定义 `Transport` trait

* [ ] 将现有 TCP 连接逻辑迁移到 `TcpTransport`

* [ ] `NetAgent` 接入 `TransportRouter`，默认 TcpOnly（行为不变）

* [ ] 单元测试：TcpTransport 与现有行为一致

**验收标准**：所有现有测试通过，PDC 联邦功能正常，行为与迁移前一致。

### 阶段 2：Iroh 接入（2-3 天）



* [ ] 新增 `IrohTransport` 实现

* [ ] 实现 `IrohIdentity`（NodeId 派生 + 持久化）

* [ ] `TransportRouter` 支持 Auto 模式

* [ ] `NetAgentConfig` 增加 `transport_mode` 和 `iroh` 配置

* [ ] PDC 配置增加 `transport_mode: auto`

**验收标准**：



* Auto 模式下并行尝试 Iroh 和 TCP，先成功先用

* 两个 NAT 后的节点能通过 Iroh 互连（QNT 打洞或 DERP 中继）

* 连接统计可观测（`/api/v1/net-agent/stats`）

### 阶段 3：联邦层适配（1 天）



* [ ] `ConnectResult.connection` 改为 `Box<dyn TransportStream>`

* [ ] PDC 联邦 `Connection::new()` 适配新接口

* [ ] 联邦握手协议增加传输方式协商

**验收标准**：PDC 联邦在 Iroh 模式下正常同步，gossip/merkle/sync 全部正常。

### 阶段 4：优化与验证（1-2 天）



* [ ] 性能对比：TCP vs Iroh 的连接延迟、吞吐量

* [ ] NAT 穿透成功率对比

* [ ] 压力测试：100+ 节点联邦

* [ ] 文档更新

**验收标准**：Iroh 模式下 NAT 穿透成功率 > 90%，连接延迟 < 500ms。



***

## 12. 风险与回退

### 12.1 风险



| 风险                 | 影响     | 缓解措施                         |
| ------------------ | ------ | ---------------------------- |
| Iroh 1.0 API 不稳定   | 编译失败   | 锁定 iroh 版本，Auto 模式下 TCP 并行兜底 |
| NodeId 派生安全性问题     | 身份伪造   | 第一阶段只用内网测试，后续可切换映射表          |
| Iroh 依赖增加编译时间      | 开发效率下降 | sccache 缓存，feature flag 可选编译 |
| QUIC 被防火墙拦截        | 连接失败   | Auto 模式下 TCP 并行兜底            |
| Iroh DERP 官方服务器不可用 | 中继失败   | 支持自定义 DERP 服务器，自建中继          |

### 12.2 回退方案

任何阶段出现问题，将配置改为 `transport_mode: tcp_only` 即可完全回退到现有行为，无需代码回滚。



***

## 13. 依赖变更

### Cargo.toml



```
\\\[dependencies]

\\# 新增

iroh = { version = "1.0", default-features = false, features = \\\["std", "relay"] }

hkdf = "0.12"

sha2 = "0.10"

hex = "0.4"

\\# 可选 feature

\\\[features]

default = \\\["tcp"]

tcp = \\\[]

iroh-transport = \\\["dep:iroh", "dep:hkdf", "dep:sha2"]
```

通过 feature flag 控制是否编译 Iroh，嵌入式场景可以只编 TCP。



***

## 14. 已决策问题

### 14.1 节点发现策略：全并行，无优先级 ✅ 已决策

**决策**：LPD / MQTT Rendezvous / DHT / Iroh 四种发现机制**同时运行**，无优先级，谁先发现就输出 `DiscoveredNode` 事件。

**设计细节**：



* 所有发现机制独立运行，互不阻塞

* 发现的节点地址合并到 `DiscoveredNode.addresses` 列表

* 同一节点被多种机制发现时，地址取并集

* NetAgent 收到发现事件后立即尝试连接（不等所有发现完成）

* Iroh 的 dial-by-key 作为兜底：当其他发现都没拿到地址时，直接用 NodeId 拨号

**理由**：最大化发现能力，缩短冷启动时间。局域网内 LPD 秒级发现，公网用 MQTT/DHT，极端情况 Iroh DERP 兜底。

### 14.2 传输协商：不需要握手协商，并行连接 ✅ 已决策

**决策**：Auto 模式下**并行尝试 Iroh 和 TCP**，哪个先成功用哪个，不需要在握手时协商能力。

**设计细节**：



* `TransportRouter.connect()` 同时发起 Iroh 连接和 TCP 连接

* 使用 `tokio::select!` 等待第一个成功

* 成功后取消另一个连接（drop 即可）

* 两者都失败时返回合并的错误信息

* 对端不支持 Iroh 时，Iroh 连接会快速失败（不是超时），TCP 照常成功

**理由**：



* 避免握手协商的复杂度和额外 RTT

* 避免串行回退的超时延迟（Iroh 失败等 10 秒才回退 TCP）

* 对端是否支持 Iroh 由连接成败自然判断，不需要显式协商

### 14.3 多路径连接的统计 ✅ 已决策

QUIC multipath 下，如何统计每条路径的质量？

**决策**：需要 Iroh 暴露路径级别的统计 API。在 Iroh 提供该 API 之前，pnos-net 只统计连接级别的指标（总延迟、总吞吐、传输方式），不做路径级细分。

**设计细节**：
- `TransportStats` 增加 `iroh_path_count`（当前活跃路径数）
- 路径级统计（每条路径的 RTT、丢包率、带宽）待 Iroh 暴露 API 后补充
- 监控面板先展示连接级指标，路径级作为后续优化项

### 14.4 SPDE 下载是否复用 Iroh ✅ 已决策

SPDE 的 P2P 下载是否也走 Iroh，还是继续用 BT 协议？

**决策**：**SPDE 继续用 BT 协议，Iroh 只用于 Agent 间联邦通信。**

**理由**：
- BT 协议有成熟的 peer 发现（DHT/PEX/Tracker）、分片下载、做种上传生态
- Iroh 定位是"设备间直连"，更适合小消息/联邦同步，不是大文件下载协议
- SPDE 的核心能力是 BT 下载，替换为 Iroh 等于重写下载引擎
- 两者职责分离：Iroh 负责 Agent 间控制面通信，BT 负责数据面下载



***

## 附录：文件变更清单



| 文件                                        | 操作 | 说明                                      |
| ----------------------------------------- | -- | --------------------------------------- |
| `crates/pnos-net/src/transport/mod.rs`    | 新增 | Transport trait 定义                      |
| `crates/pnos-net/src/transport/tcp.rs`    | 新增 | TcpTransport（从 connector/strategy 迁移）   |
| `crates/pnos-net/src/transport/iroh.rs`   | 新增 | IrohTransport 实现                        |
| `crates/pnos-net/src/transport/router.rs` | 新增 | TransportRouter（Auto 调度）                |
| `crates/pnos-net/src/net_agent.rs`        | 修改 | 接入 TransportRouter，配置增加 transport\_mode |
| `crates/pnos-net/src/types.rs`            | 修改 | ConnectMethod 增加 Iroh 变体                |
| `crates/pnos-net/src/connector.rs`        | 保留 | 内部使用，不对外暴露                              |
| `crates/pnos-net/src/strategy.rs`         | 保留 | TcpTransport 内部使用                       |
| `crates/pnos-net/Cargo.toml`              | 修改 | 增加 iroh/hkdf/sha2 依赖                    |
| `crates/pnos-net/src/lib.rs`              | 修改 | 导出 transport 模块                         |