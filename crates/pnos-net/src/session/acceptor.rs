//! 入站受理与接收循环
//!
//! 这两个循环是**事件驱动**的长驻任务（等待连接到达 / 等待数据到达），
//! 不是周期定时任务，因此可以 `spawn`；
//! 周期性的维护动作统一走 [`SessionManager::tick`]。
//!
//! [`SessionManager::tick`]: crate::session::SessionManager::tick

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use crate::session::frame::FrameTransport;
use crate::session::session::{Direction, DisconnectReason, Session};
use crate::session::{RejectReason, SessionEvent, SessionManager};
use crate::transport::{TcpTransportStream, TransportKind};

/// 单次 accept 失败后的退避间隔（避免 fd 耗尽时空转打满 CPU）
const ACCEPT_RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// 启动 accept 循环
pub(crate) fn spawn_accept_loop(mgr: Arc<SessionManager>) {
    tokio::spawn(async move {
        let listener = match mgr.listener.lock().await.take() {
            Some(l) => l,
            None => {
                warn!("[session] accept loop 启动失败：监听器已被取走");
                return;
            }
        };

        loop {
            if mgr.is_shutting_down() {
                break;
            }
            tokio::select! {
                _ = mgr.wait_shutdown() => break,
                res = listener.accept() => {
                    match res {
                        Ok((stream, addr)) => spawn_inbound(mgr.clone(), stream, addr),
                        Err(e) => {
                            // 单次 accept 失败不应终止整个循环（如 fd 耗尽）
                            warn!("[session] accept 失败: {}", e);
                            tokio::time::sleep(ACCEPT_RETRY_BACKOFF).await;
                        }
                    }
                }
            }
        }
        debug!("[session] accept loop 退出");
    });
}

/// 受理一条入站连接：限额 → 握手 → 激活
fn spawn_inbound(mgr: Arc<SessionManager>, stream: TcpStream, addr: SocketAddr) {
    tokio::spawn(async move {
        if mgr.is_shutting_down() {
            return;
        }

        // 限额（握手前先挡掉，避免为投机连接付出握手成本）
        if mgr.registry.len() >= mgr.cfg.max_sessions {
            mgr.stats.record_inbound_rejected();
            let _ = mgr.events.send(SessionEvent::Rejected {
                addr,
                reason: RejectReason::MaxSessions,
            });
            warn!(
                "[session] 入站 {} 被拒：会话数已达上限 {}",
                addr, mgr.cfg.max_sessions
            );
            return;
        }

        let transport = Arc::new(
            FrameTransport::from_stream(Box::new(TcpTransportStream::new(
                stream,
                TransportKind::Tcp,
            )))
            .with_stats(mgr.stats.clone())
            .with_write_timeout(mgr.cfg.write_timeout)
            .with_retry_config(mgr.cfg.write_max_retries, mgr.cfg.write_retry_base_ms),
        );

        let identity = match tokio::time::timeout(
            mgr.auth.timeout(),
            mgr.auth.authenticate_inbound(&transport),
        )
        .await
        {
            Ok(Ok(id)) => id,
            Ok(Err(e)) => {
                mgr.stats.record_inbound_rejected();
                let _ = mgr.events.send(SessionEvent::Rejected {
                    addr,
                    reason: RejectReason::HandshakeFailed(e.to_string()),
                });
                debug!("[session] 入站 {} 握手失败: {}", addr, e);
                let _ = transport.close().await;
                return;
            }
            Err(_) => {
                mgr.stats.record_inbound_rejected();
                let _ = mgr.events.send(SessionEvent::Rejected {
                    addr,
                    reason: RejectReason::HandshakeFailed("握手超时".into()),
                });
                debug!("[session] 入站 {} 握手超时", addr);
                let _ = transport.close().await;
                return;
            }
        };

        if !identity.verified {
            mgr.stats.record_inbound_rejected();
            let _ = mgr.events.send(SessionEvent::Rejected {
                addr,
                reason: RejectReason::Unauthenticated,
            });
            debug!("[session] 入站 {} 身份校验未通过", addr);
            let _ = transport.close().await;
            return;
        }

        let session = Session::new(
            identity.peer_id,
            addr,
            Direction::Inbound,
            transport,
            identity.metadata,
        );

        if let Err(e) = mgr.activate(session).await {
            debug!("[session] 入站 {} 激活失败: {}", addr, e);
        }
    });
}

/// 启动某条会话的接收循环
///
/// 退出路径有三条：收到关闭信号 / 会话被标记关闭 / 读取出错。
/// 无论走哪条，最后都会调用一次 [`SessionManager::disconnect`] 兜底清理——
/// 它是幂等的，因此不会造成重复计数。
pub(crate) fn spawn_recv_loop(mgr: Arc<SessionManager>, session: Arc<Session>) {
    info!(
        "[session] {} 接收循环启动（对端={}）",
        session.id, session.peer_id
    );
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = mgr.wait_shutdown() => {
                    mgr.disconnect(session.id, DisconnectReason::Shutdown).await;
                    break;
                }
                _ = session.wait_closed() => break,
                res = session.transport.recv_frame() => {
                    match res {
                        Ok(frame) => {
                            session.touch_recv();
                            // 保活帧由本层内部消化，不上抛业务
                            if mgr.handle_keepalive(&session, &frame).await {
                                continue;
                            }
                            let _ = mgr.events.send(SessionEvent::Frame {
                                session: session.id,
                                peer: session.peer_id,
                                frame,
                            });
                        }
                        Err(e) => {
                            mgr.disconnect(session.id, DisconnectReason::Io(e.to_string())).await;
                            break;
                        }
                    }
                }
            }
        }
        // 兜底：幂等清理
        mgr.hb.forget(session.id);
        mgr.disconnect(session.id, DisconnectReason::Remote).await;
        debug!("[session] {} 接收循环退出", session.id);
    });
}
