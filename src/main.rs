use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

const UDP_EOF_SEQ: u32 = 0xFFFF_FFFF;

struct Config {
    bind_host: String,
    port_a: u16,
    port_b: u16,
    gif_file: String,
    video_file: String,
    udp_chunk_size: usize,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn load_config() -> Config {
    Config {
        bind_host: env_or("BIND_HOST", "0.0.0.0"),
        port_a: env_or("PORT_A", "9001")
            .parse()
            .expect("PORT_A must be a number"),
        port_b: env_or("PORT_B", "9002")
            .parse()
            .expect("PORT_B must be a number"),
        gif_file: env_or("GIF_FILE", "cat.gif"),
        video_file: env_or("VIDEO_FILE", "video.mp4"),
        udp_chunk_size: env_or("UDP_CHUNK_SIZE", "1400")
            .parse()
            .expect("UDP_CHUNK_SIZE must be a number"),
    }
}

fn build_candidates(filename: &str) -> Vec<String> {
    vec![format!("./data/{filename}"), format!("/data/{filename}")]
}

async fn load_file_or_die(filename: &str) -> Vec<u8> {
    let candidates = build_candidates(filename);

    for candidate in &candidates {
        if Path::new(candidate).is_file() {
            return fs::read(candidate)
                .await
                .unwrap_or_else(|e| panic!("failed to read {candidate}: {e}"));
        }
    }

    eprintln!(
        "ERROR: could not find '{filename}'. Tried: {}",
        candidates.join(", ")
    );
    eprintln!(
        "Place the file at one of the paths above, or point GIF_FILE / VIDEO_FILE at a different filename."
    );
    std::process::exit(1);
}

fn build_gif_page(gif_bytes: &[u8]) -> Vec<u8> {
    let b64 = STANDARD.encode(gif_bytes);
    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>cat</title>
<style>
  html, body {{
    margin: 0;
    height: 100%;
    background: #111;
    display: flex;
    align-items: center;
    justify-content: center;
  }}
  img {{
    max-width: 90vw;
    max-height: 90vh;
    border-radius: 12px;
    box-shadow: 0 0 40px rgba(0,0,0,0.6);
  }}
</style>
</head>
<body>
  <img src="data:image/gif;base64,{b64}" alt="cat gif">
</body>
</html>"#
    );
    let body = html.into_bytes();
    let mut resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    resp.extend_from_slice(&body);
    resp
}

async fn handle_http_conn(mut stream: TcpStream, page: Arc<Vec<u8>>, peer: SocketAddr) {
    let mut buf = [0u8; 1024];
    let _ = stream.read(&mut buf).await;

    if let Err(e) = stream.write_all(&page).await {
        eprintln!("[HTTP] write error to {peer}: {e}");
        return;
    }
    let _ = stream.shutdown().await;
}

async fn run_http_server(host: &str, port: u16, gif_bytes: Vec<u8>) {
    let page = Arc::new(build_gif_page(&gif_bytes));
    let listener = TcpListener::bind((host, port))
        .await
        .unwrap_or_else(|e| panic!("failed to bind HTTP {host}:{port}: {e}"));
    println!("[HTTP:gif] listening on {host}:{port}");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let page = Arc::clone(&page);
                tokio::spawn(async move {
                    handle_http_conn(stream, page, peer).await;
                });
            }
            Err(e) => eprintln!("[HTTP:gif] accept error: {e}"),
        }
    }
}

async fn handle_raw_tcp_conn(
    mut stream: TcpStream,
    data: Arc<Vec<u8>>,
    label: &'static str,
    peer: SocketAddr,
) {
    println!(
        "[TCP:{label}] connection from {peer}, sending {} bytes",
        data.len()
    );
    if let Err(e) = stream.write_all(&(data.len() as u64).to_be_bytes()).await {
        eprintln!("[TCP:{label}] length write error to {peer}: {e}");
        return;
    }
    if let Err(e) = stream.write_all(&data).await {
        eprintln!("[TCP:{label}] body write error to {peer}: {e}");
        return;
    }
    let _ = stream.shutdown().await;
}

async fn run_raw_tcp_server(host: &str, port: u16, data: Vec<u8>, label: &'static str) {
    let data = Arc::new(data);
    let listener = TcpListener::bind((host, port))
        .await
        .unwrap_or_else(|e| panic!("failed to bind TCP {host}:{port}: {e}"));
    println!("[TCP:{label}] listening on {host}:{port}");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let data = Arc::clone(&data);
                tokio::spawn(async move {
                    handle_raw_tcp_conn(stream, data, label, peer).await;
                });
            }
            Err(e) => eprintln!("[TCP:{label}] accept error: {e}"),
        }
    }
}

async fn run_udp_server(
    host: &str,
    port: u16,
    data: Vec<u8>,
    label: &'static str,
    chunk_size: usize,
) {
    let data = Arc::new(data);
    let socket = Arc::new(
        UdpSocket::bind((host, port))
            .await
            .unwrap_or_else(|e| panic!("failed to bind UDP {host}:{port}: {e}")),
    );
    println!("[UDP:{label}] listening on {host}:{port}");

    let mut buf = [0u8; 65535];
    loop {
        let (_len, peer) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[UDP:{label}] recv error: {e}");
                continue;
            }
        };
        println!(
            "[UDP:{label}] request from {peer}, sending {} bytes in {chunk_size}-byte chunks",
            data.len()
        );
        let data = Arc::clone(&data);
        let socket = Arc::clone(&socket);
        tokio::spawn(async move {
            let mut seq: u32 = 0;
            for chunk in data.chunks(chunk_size) {
                let mut packet = Vec::with_capacity(4 + chunk.len());
                packet.extend_from_slice(&seq.to_be_bytes());
                packet.extend_from_slice(chunk);
                if let Err(e) = socket.send_to(&packet, peer).await {
                    eprintln!("[UDP:{label}] send error to {peer}: {e}");
                    return;
                }
                seq = seq.wrapping_add(1);
            }
            let _ = socket.send_to(&UDP_EOF_SEQ.to_be_bytes(), peer).await;
            println!("[UDP:{label}] finished sending to {peer} ({seq} chunks)");
        });
    }
}

#[tokio::main]
async fn main() {
    let cfg = load_config();

    let gif_bytes = load_file_or_die(&cfg.gif_file).await;
    let video_bytes = load_file_or_die(&cfg.video_file).await;

    let host_http = cfg.bind_host.clone();
    let host_udp_a = cfg.bind_host.clone();
    let host_tcp_b = cfg.bind_host.clone();
    let host_udp_b = cfg.bind_host.clone();

    let port_a = cfg.port_a;
    let port_b = cfg.port_b;
    let chunk_size = cfg.udp_chunk_size;

    let gif_for_udp = gif_bytes.clone();
    let video_for_udp = video_bytes.clone();

    let h1 = tokio::spawn(async move { run_http_server(&host_http, port_a, gif_bytes).await });
    let h2 = tokio::spawn(async move {
        run_udp_server(&host_udp_a, port_a, gif_for_udp, "gif", chunk_size).await
    });
    let h3 =
        tokio::spawn(
            async move { run_raw_tcp_server(&host_tcp_b, port_b, video_bytes, "video").await },
        );
    let h4 = tokio::spawn(async move {
        run_udp_server(&host_udp_b, port_b, video_for_udp, "video", chunk_size).await
    });

    println!("All servers up. Ctrl+C to stop.");
    let _ = tokio::join!(h1, h2, h3, h4);
}
