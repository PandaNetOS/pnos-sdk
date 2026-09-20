//! 帧编解码与帧传输
//!
//! # 线缆格式（与既有联邦协议逐字节兼容）
//!
//! ```text
//! [4 字节大端长度][1 字节 kind][payload]
//! ```
//!
//! 其中「长度」= 1（kind 字节）+ payload 长度。
//!
//! 注意：帧头的 kind 字段**只有 1 字节**。对外暴露的 [`Frame::kind`] 为 `u16`
//! 仅为将来扩展预留；编码时若 `kind > 255` 会直接报错，以保证线缆不变。
//!
//! # 通用性
//!
//! 本模块不理解 kind 的语义，也不持有任何业务状态——它只负责把字节流切成帧、
//! 把帧写回字节流。消息含义由上层（应用）自行解释。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::Mutex as TokioMutex;

use crate::session::stats::SessionStats;
use crate::transport::TransportStream;

/// 帧头大小：4 字节长度 + 1 字节 kind
pub const FRAME_HEADER_SIZE: usize = 5;

/// 最大帧大小（16 MiB，防止恶意大包）
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// 接收缓冲上限：对端持续发送无法组帧的数据即断开，避免内存无界增长
const MAX_READ_BUF: usize = 2 * 1024 * 1024;

/// 默认写入超时
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// 高优先级发送的自旋重试上限（超过后退化为阻塞锁）
const HIGH_PRIORITY_SPIN_MAX_ATTEMPTS: u32 = 20;
/// 高优先级发送的自旋等待间隔
const HIGH_PRIORITY_SPIN_INTERVAL: Duration = Duration::from_millis(1);
/// 写入超时后的默认最大重试次数（不含首次）
const DEFAULT_WRITE_MAX_RETRIES: u32 = 3;
/// 写入重试退避基数（毫秒）
const DEFAULT_WRITE_RETRY_BASE_MS: u64 = 100;

/// 一帧数据
///
/// `kind` 由上层定义，本层只做转发。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: u16,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(kind: u16, payload: impl Into<Bytes>) -> Self {
        Self {
            kind,
            payload: payload.into(),
        }
    }

    /// 编码为线缆字节
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        encode_frame(self.kind, &self.payload)
    }
}

/// 编码一帧
///
/// `kind` 必须落在 `u8` 域内（帧头只有 1 字节），否则报错。
pub fn encode_frame(kind: u16, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    if kind > u8::MAX as u16 {
        anyhow::bail!("kind 超出帧头容量（1 字节）: {}", kind);
    }
    let total_len = 1 + payload.len();
    if total_len > MAX_FRAME_SIZE {
        anyhow::bail!("帧过大: {} 字节 > {} 字节", total_len, MAX_FRAME_SIZE);
    }
    let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
    frame.extend_from_slice(&(total_len as u32).to_be_bytes());
    frame.push(kind as u8);
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// 解码完整帧（拷贝 payload）
pub fn decode_frame(data: &[u8]) -> anyhow::Result<Frame> {
    if data.len() < FRAME_HEADER_SIZE {
        anyhow::bail!(
            "数据不足，需要至少 {} 字节，实际 {}",
            FRAME_HEADER_SIZE,
            data.len()
        );
    }
    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if length == 0 || length > MAX_FRAME_SIZE {
        anyhow::bail!("无效帧长度: {}", length);
    }
    if data.len() < FRAME_HEADER_SIZE + length - 1 {
        anyhow::bail!(
            "数据不完整，需要 {} 字节，实际 {}",
            FRAME_HEADER_SIZE + length - 1,
            data.len()
        );
    }
    let kind = data[4] as u16;
    let payload = Bytes::copy_from_slice(&data[FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + length - 1]);
    Ok(Frame { kind, payload })
}

/// 零拷贝解析：从已切好的完整帧缓冲构造 `Frame`
fn parse_frame(buf: BytesMut) -> anyhow::Result<Frame> {
    if buf.len() < FRAME_HEADER_SIZE {
        anyhow::bail!("帧缓冲不足 {} 字节", FRAME_HEADER_SIZE);
    }
    let frozen = buf.freeze();
    let kind = frozen[4] as u16;
    let payload = frozen.slice(FRAME_HEADER_SIZE..);
    Ok(Frame { kind, payload })
}

/// 检查缓冲区中是否有完整帧，返回完整帧的总字节数（含帧头）
///
/// - `Ok(Some(total))`：有完整帧
/// - `Ok(None)`：数据不足，需继续读
/// - `Err`：帧长度非法（0 或超上限）——调用方应据此断开，而不是继续累积
pub fn frame_size_in_buffer(data: &[u8]) -> anyhow::Result<Option<usize>> {
    if data.len() < 4 {
        return Ok(None);
    }
    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if length == 0 || length > MAX_FRAME_SIZE {
        anyhow::bail!("非法帧长度: {}", length);
    }
    let total = FRAME_HEADER_SIZE + length - 1;
    if data.len() >= total {
        Ok(Some(total))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// 帧传输
// ---------------------------------------------------------------------------

/// TCP 读取端（含读取缓冲区）
struct FrameReader {
    reader: ReadHalf<Box<dyn TransportStream>>,
    read_buf: BytesMut,
}

/// TCP 写入端
struct FrameWriter {
    writer: WriteHalf<Box<dyn TransportStream>>,
}

/// 帧传输层（读写分离，独立锁，可并发收发）
///
/// 内部使用 owned split 分离读写，读写各持独立锁，发送与接收可并发执行。
pub struct FrameTransport {
    reader: TokioMutex<FrameReader>,
    writer: TokioMutex<FrameWriter>,
    peer: Option<SocketAddr>,
    local: Option<SocketAddr>,
    /// 字节级统计（可选，由会话注册表注入）
    stats: Option<SessionStats>,
    write_timeout: Duration,
    max_retries: u32,
    retry_base_ms: u64,
}

impl FrameTransport {
    /// 从已有的传输流创建
    pub fn from_stream(stream: Box<dyn TransportStream>) -> Self {
        let peer = stream.peer_addr();
        let local = stream.local_addr();
        let (reader, writer) = tokio::io::split(stream);
        Self {
            reader: TokioMutex::new(FrameReader {
                reader,
                read_buf: BytesMut::with_capacity(8192),
            }),
            writer: TokioMutex::new(FrameWriter { writer }),
            peer,
            local,
            stats: None,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            max_retries: DEFAULT_WRITE_MAX_RETRIES,
            retry_base_ms: DEFAULT_WRITE_RETRY_BASE_MS,
        }
    }

    /// 注入统计句柄（链式调用）
    pub fn with_stats(mut self, stats: SessionStats) -> Self {
        self.stats = Some(stats);
        self
    }

    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    pub fn with_retry_config(mut self, max_retries: u32, retry_base_ms: u64) -> Self {
        self.max_retries = max_retries;
        self.retry_base_ms = retry_base_ms;
        self
    }

    /// 带重试的帧写入（持写锁）
    ///
    /// 单次 `write_all` 受 `write_timeout` 限制；超时后按指数退避重试最多
    /// `max_retries` 次。非超时的硬 IO 错误（如连接重置）不重试，立即上抛。
    async fn write_frame_with_retry(
        &self,
        writer: &mut WriteHalf<Box<dyn TransportStream>>,
        frame: &[u8],
    ) -> anyhow::Result<()> {
        let mut attempt: u32 = 0;
        loop {
            match tokio::time::timeout(self.write_timeout, writer.write_all(frame)).await {
                Ok(Ok(())) => {
                    let _ = writer.flush().await;
                    return Ok(());
                }
                Ok(Err(e)) => return Err(anyhow::anyhow!("写入失败: {}", e)),
                Err(_elapsed) => {
                    if attempt >= self.max_retries {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("write timeout（重试 {} 次后仍超时）", self.max_retries),
                        )
                        .into());
                    }
                    // 指数退避：base * 2^attempt（移位次数做钳制，避免溢出）
                    let backoff_ms = self.retry_base_ms.saturating_mul(1u64 << attempt.min(20));
                    tracing::debug!(
                        "[session] write 超时，第 {}/{} 次重试（等待 {}ms）",
                        attempt + 1,
                        self.max_retries,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    attempt += 1;
                }
            }
        }
    }

    /// 发送一帧
    pub async fn send_frame(&self, kind: u16, payload: &[u8]) -> anyhow::Result<()> {
        let frame = encode_frame(kind, payload)?;
        let frame_len = frame.len();
        let mut writer = self.writer.lock().await;
        self.write_frame_with_retry(&mut writer.writer, &frame)
            .await?;
        if let Some(stats) = &self.stats {
            stats.record_bytes_sent(frame_len as u64);
            stats.record_frame_sent();
        }
        Ok(())
    }

    /// 高优先级发送：短时间内自旋 `try_lock`，避免心跳排在大同步消息之后
    pub async fn send_frame_high_priority(&self, kind: u16, payload: &[u8]) -> anyhow::Result<()> {
        let frame = encode_frame(kind, payload)?;
        let frame_len = frame.len();
        let mut writer = {
            let mut attempts = 0;
            // [ALLOWED-SLEEP] 高优先级发送的自旋等待：try_lock 失败时让出 1ms 重试，
            // 达到上限后回退为阻塞锁；非周期性定时任务。
            loop {
                match self.writer.try_lock() {
                    Ok(guard) => break guard,
                    Err(_) => {
                        attempts += 1;
                        if attempts >= HIGH_PRIORITY_SPIN_MAX_ATTEMPTS {
                            break self.writer.lock().await;
                        }
                        tokio::time::sleep(HIGH_PRIORITY_SPIN_INTERVAL).await;
                    }
                }
            }
        };
        self.write_frame_with_retry(&mut writer.writer, &frame)
            .await?;
        if let Some(stats) = &self.stats {
            stats.record_bytes_sent(frame_len as u64);
            stats.record_frame_sent();
        }
        Ok(())
    }

    /// 发送已编码的完整帧字节（用于批量合并帧等预编码场景）
    pub async fn send_encoded(&self, encoded: &[u8]) -> anyhow::Result<()> {
        let mut writer = self.writer.lock().await;
        self.write_frame_with_retry(&mut writer.writer, encoded)
            .await?;
        if let Some(stats) = &self.stats {
            stats.record_bytes_sent(encoded.len() as u64);
            stats.record_frame_sent();
        }
        Ok(())
    }

    /// 接收一帧（循环读取直到组帧完成）
    pub async fn recv_frame(&self) -> anyhow::Result<Frame> {
        let mut reader = self.reader.lock().await;
        loop {
            if let Some(frame_len) = frame_size_in_buffer(&reader.read_buf)? {
                let frame_buf = reader.read_buf.split_to(frame_len);
                let frame = parse_frame(frame_buf)?;
                if let Some(stats) = &self.stats {
                    stats.record_bytes_recv(frame_len as u64);
                    stats.record_frame_recv();
                }
                return Ok(frame);
            }

            if reader.read_buf.len() > MAX_READ_BUF {
                anyhow::bail!("接收缓冲区超限（{}B），断开连接", reader.read_buf.len());
            }

            let mut tmp = [0u8; 8192];
            let n = reader
                .reader
                .read(&mut tmp)
                .await
                .map_err(|e| anyhow::anyhow!("读取失败: {}", e))?;
            if n == 0 {
                anyhow::bail!("连接已关闭");
            }
            reader.read_buf.put_slice(&tmp[..n]);
        }
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local
    }

    /// 关闭连接（关闭写半端）
    pub async fn close(&self) -> anyhow::Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .writer
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!("关闭连接失败: {}", e))?;
        Ok(())
    }
}

impl std::fmt::Debug for FrameTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameTransport")
            .field("peer", &self.peer)
            .field("local", &self.local)
            .finish()
    }
}

/// 供集成测试与直连场景使用：把任意 `TransportStream` 包成 `Arc<FrameTransport>`
pub fn transport_from_stream(stream: Box<dyn TransportStream>) -> Arc<FrameTransport> {
    Arc::new(FrameTransport::from_stream(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{TcpTransportStream, TransportKind};
    use tokio::net::TcpListener;

    fn tcp_stream(s: tokio::net::TcpStream) -> Box<dyn TransportStream> {
        Box::new(TcpTransportStream::new(s, TransportKind::Tcp))
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let payload = b"hello-frame";
        let encoded = encode_frame(42, payload).unwrap();
        assert_eq!(encoded.len(), FRAME_HEADER_SIZE + payload.len());
        let frame = decode_frame(&encoded).unwrap();
        assert_eq!(frame.kind, 42);
        assert_eq!(&frame.payload[..], payload);
    }

    #[test]
    fn test_kind_must_fit_one_byte() {
        assert!(encode_frame(255, b"x").is_ok());
        // 帧头只有 1 字节，越界必须报错而不是静默截断
        assert!(encode_frame(256, b"x").is_err());
        assert!(encode_frame(u16::MAX, b"x").is_err());
    }

    #[test]
    fn test_frame_size_in_buffer() {
        let encoded = encode_frame(2, b"abcdefgh").unwrap();
        let total = encoded.len();

        // 不足 4 字节
        assert_eq!(frame_size_in_buffer(&encoded[..3]).unwrap(), None);
        // 有帧头但数据不足
        assert_eq!(frame_size_in_buffer(&encoded[..total - 1]).unwrap(), None);
        // 完整帧
        assert_eq!(frame_size_in_buffer(&encoded).unwrap(), Some(total));
        // 非法长度（0）
        assert!(frame_size_in_buffer(&[0u8; 4]).is_err());
        // 多帧粘包
        let mut two = encoded.clone();
        two.extend_from_slice(&encoded);
        assert_eq!(frame_size_in_buffer(&two).unwrap(), Some(total));
    }

    #[test]
    fn test_decode_rejects_bad_input() {
        assert!(decode_frame(&[0, 0, 0]).is_err());
        assert!(decode_frame(&[0, 0, 0, 10, 0]).is_err());
        assert!(decode_frame(&[0xff, 0xff, 0xff, 0xff, 0]).is_err());
    }

    #[test]
    fn test_max_frame_size() {
        let big = vec![0u8; MAX_FRAME_SIZE + 1];
        assert!(encode_frame(3, &big).is_err());
    }

    #[tokio::test]
    async fn test_transport_frame_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let t = FrameTransport::from_stream(tcp_stream(stream));
            let frame = t.recv_frame().await.unwrap();
            assert_eq!(frame.kind, 7);
            assert_eq!(&frame.payload[..], b"ping");
            t.send_frame(8, b"pong").await.unwrap();
            t
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let c = FrameTransport::from_stream(tcp_stream(client));
        c.send_frame(7, b"ping").await.unwrap();
        let resp = c.recv_frame().await.unwrap();
        assert_eq!(resp.kind, 8);
        assert_eq!(&resp.payload[..], b"pong");

        let _server = server.await.unwrap();
    }

    #[tokio::test]
    async fn test_transport_partial_and_sticky_frames() {
        // 服务端故意把一帧拆成多次写，再把两帧粘在一起写
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let f1 = encode_frame(1, b"AAAA").unwrap();
            let f2 = encode_frame(2, b"BBBBBB").unwrap();

            // 半包：先写 f1 的前 3 字节
            stream.write_all(&f1[..3]).await.unwrap();
            stream.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            // 补齐 f1 剩余 + 紧接 f2（粘包）
            stream.write_all(&f1[3..]).await.unwrap();
            stream.write_all(&f2).await.unwrap();
            stream.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let c = FrameTransport::from_stream(tcp_stream(client));

        let a = c.recv_frame().await.unwrap();
        assert_eq!(a.kind, 1);
        assert_eq!(&a.payload[..], b"AAAA");

        let b = c.recv_frame().await.unwrap();
        assert_eq!(b.kind, 2);
        assert_eq!(&b.payload[..], b"BBBBBB");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_transport_stats_counting() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let t = FrameTransport::from_stream(tcp_stream(stream));
            let _ = t.recv_frame().await.unwrap();
        });

        let stats = SessionStats::new();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let c = FrameTransport::from_stream(tcp_stream(client)).with_stats(stats.clone());
        c.send_frame(1, b"12345").await.unwrap();

        assert_eq!(stats.frames_sent(), 1);
        assert_eq!(stats.bytes_sent() as usize, FRAME_HEADER_SIZE + 5);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_transport_recv_detects_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream); // 立刻关闭
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let c = FrameTransport::from_stream(tcp_stream(client));
        let r = c.recv_frame().await;
        assert!(r.is_err());

        server.await.unwrap();
    }
}
