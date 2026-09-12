//! 极简 UDP Echo Server
//!
//! 用于测试压测工具的上限：收到 BEP15 包后立即回响应，零业务处理。

use clap::Parser;
use tokio::net::UdpSocket;

#[derive(Parser)]
#[command(name = "udp-echo-server")]
struct Args {
    #[arg(short, long, default_value = "6881")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let socket = UdpSocket::bind(format!("0.0.0.0:{}", args.port)).await?;
    println!("UDP Echo Server: 0.0.0.0:{}", args.port);

    let mut buf = vec![0u8; 2048];
    let mut announce_resp = vec![0u8; 20];
    announce_resp[0..4].copy_from_slice(&1u32.to_be_bytes()); // action=announce

    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        if len >= 16 {
            let action = u32::from_be_bytes(buf[8..12].try_into().unwrap());
            let tx_id = u32::from_be_bytes(buf[12..16].try_into().unwrap());

            match action {
                0 => {
                    // connect: 16 字节
                    let mut resp = [0u8; 16];
                    resp[4..8].copy_from_slice(&tx_id.to_be_bytes());
                    resp[8..16].copy_from_slice(&0x123456789ABCDEF0u64.to_be_bytes());
                    let _ = socket.send_to(&resp, addr).await;
                }
                1 => {
                    // announce: 20 字节
                    announce_resp[4..8].copy_from_slice(&tx_id.to_be_bytes());
                    let _ = socket.send_to(&announce_resp, addr).await;
                }
                2 => {
                    // scrape: 20 字节
                    let mut resp = [0u8; 20];
                    resp[0..4].copy_from_slice(&2u32.to_be_bytes());
                    resp[4..8].copy_from_slice(&tx_id.to_be_bytes());
                    let _ = socket.send_to(&resp, addr).await;
                }
                _ => {}
            }
        }
    }
}
