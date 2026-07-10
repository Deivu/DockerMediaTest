use base64::{Engine as _, engine::general_purpose::STANDARD};
use dashmap::DashMap;
use socket2::{Domain, Socket, Type};
use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio::time::timeout;

static SESSIONS: LazyLock<Arc<DashMap<u32, mpsc::Sender<Vec<u8>>>>> =
    LazyLock::new(|| Arc::new(DashMap::new()));
const MAX_ROUNDS: u32 = 20;
const REPORT_TIMEOUT: Duration = Duration::from_millis(1000);

#[derive(PartialEq)]
#[repr(u8)]
enum Kind {
    Hello = 0,
    Data = 1,
    Manifest = 2,
    Missing = 3,
}

impl Kind {
    fn from_u8(b: u8) -> Option<Kind> {
        match b {
            0 => Some(Kind::Hello),
            1 => Some(Kind::Data),
            2 => Some(Kind::Manifest),
            3 => Some(Kind::Missing),
            _ => None,
        }
    }
}

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

#[cfg(windows)]
fn disable_conn_reset(socket: &Socket) -> std::io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{SOCKET, WSAIoctl};

    const IOC_IN: u32 = 0x8000_0000;
    const IOC_VENDOR: u32 = 0x1800_0000;
    const SIO_UDP_CONNRESET: u32 = IOC_IN | IOC_VENDOR | 12;

    let mut bytes_returned: u32 = 0;
    let enable: u32 = 0;

    let ret = unsafe {
        WSAIoctl(
            socket.as_raw_socket() as SOCKET,
            SIO_UDP_CONNRESET,
            &enable as *const u32 as *mut _,
            size_of::<u32>() as u32,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned,
            std::ptr::null_mut(),
            None,
        )
    };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(windows))]
fn disable_conn_reset(_socket: &Socket) -> std::io::Result<()> {
    Ok(())
}

async fn bind_udp(host: &str, port: u16) -> UdpSocket {
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .unwrap_or_else(|e| panic!("bad bind addr {host}:{port}: {e}"));

    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, None)
        .unwrap_or_else(|e| panic!("failed to create UDP socket: {e}"));
    socket
        .set_nonblocking(true)
        .unwrap_or_else(|e| panic!("failed to set nonblocking: {e}"));
    socket
        .bind(&addr.into())
        .unwrap_or_else(|e| panic!("failed to bind UDP {addr}: {e}"));

    if let Err(e) = disable_conn_reset(&socket) {
        eprintln!("warning: failed to disable SIO_UDP_CONNRESET: {e}");
    }

    UdpSocket::from_std(socket.into())
        .unwrap_or_else(|e| panic!("failed to convert to tokio UdpSocket: {e}"))
}

async fn run_udp_server(
    host: &str,
    port: u16,
    data: Vec<u8>,
    label: &'static str,
    chunk_size: usize,
) {
    let chunks: Arc<Vec<Vec<u8>>> = Arc::new(data.chunks(chunk_size).map(|c| c.to_vec()).collect());
    let socket = Arc::new(bind_udp(host, port).await);
    println!("[UDP:{label}] listening on {host}:{port}");

    let mut buf = [0u8; 65535];
    loop {
        let (len, peer) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[UDP:{label}] recv error: {e}");
                continue;
            }
        };

        if len < 5 {
            eprintln!("[UDP:{label}] {peer} sent undersized packet ({len} bytes), dropping");
            continue;
        }

        let kind = match Kind::from_u8(buf[0]) {
            Some(k) => k,
            None => {
                eprintln!(
                    "[UDP:{label}] {peer} sent unknown kind byte {}, dropping",
                    buf[0]
                );
                continue;
            }
        };
        let request_id = u32::from_be_bytes(buf[1..5].try_into().unwrap());
        let is_known_session = SESSIONS.contains_key(&request_id);

        match kind {
            Kind::Hello if !is_known_session => {
                if len != 5 {
                    eprintln!("[UDP:{label}] {peer} malformed Hello (len {len}), dropping");
                    continue;
                }
                println!(
                    "[UDP:{label}] hello from {peer}, request_id={request_id}, {} chunks",
                    chunks.len()
                );
                let (tx, rx) = mpsc::channel(8);
                SESSIONS.insert(request_id, tx);
                let chunks = Arc::clone(&chunks);
                let socket = Arc::clone(&socket);
                tokio::spawn(async move {
                    handle_request(&socket, peer, request_id, &chunks, rx, label).await;
                    SESSIONS.remove(&request_id);
                });
            }

            Kind::Missing if is_known_session => {
                if len < 5 || (len - 5) % 4 != 0 {
                    eprintln!(
                        "[UDP:{label}] {peer} malformed Missing for request {request_id} (len {len}), terminating session"
                    );
                    SESSIONS.remove(&request_id);
                    continue;
                }
                if let Some(tx) = SESSIONS.get(&request_id) {
                    let _ = tx.send(buf[..len].to_vec()).await;
                }
            }

            _ => {
                eprintln!(
                    "[UDP:{label}] {peer} sent unexpected kind for request {request_id} (known_session={is_known_session}), terminating session"
                );
                SESSIONS.remove(&request_id);
            }
        }
    }
}

async fn handle_request(
    socket: &UdpSocket,
    peer: SocketAddr,
    request_id: u32,
    chunks: &[Vec<u8>],
    mut reports: mpsc::Receiver<Vec<u8>>,
    label: &'static str,
) {
    let total = chunks.len() as u32;
    let mut pending: Vec<u32> = (0..total).collect();

    for round in 0..MAX_ROUNDS {
        if pending.is_empty() {
            break;
        }

        for &seq in &pending {
            let chunk = &chunks[seq as usize];
            let mut pkt = Vec::with_capacity(9 + chunk.len());
            pkt.push(Kind::Data as u8);
            pkt.extend_from_slice(&request_id.to_be_bytes());
            pkt.extend_from_slice(&seq.to_be_bytes());
            pkt.extend_from_slice(chunk);
            if let Err(e) = socket.send_to(&pkt, peer).await {
                eprintln!("[UDP:{label}] send error to {peer}: {e}");
                return;
            }
        }

        let mut manifest = Vec::with_capacity(9);
        manifest.push(Kind::Manifest as u8);
        manifest.extend_from_slice(&request_id.to_be_bytes());
        manifest.extend_from_slice(&total.to_be_bytes());
        let _ = socket.send_to(&manifest, peer).await;

        match timeout(REPORT_TIMEOUT, reports.recv()).await {
            Ok(Some(report)) => {
                pending = parse_missing(&report);
                if pending.is_empty() {
                    println!("[UDP:{label}] request {request_id} ({peer}) complete, round {round}");
                    return;
                }
                println!(
                    "[UDP:{label}] request {request_id} missing {} chunks, round {round}",
                    pending.len()
                );
            }
            Ok(None) => {
                println!("[UDP:{label}] request {request_id} session terminated, aborting send");
                return;
            }
            Err(_) => {
                println!(
                    "[UDP:{label}] request {request_id} report timeout, resending {} chunks",
                    pending.len()
                );
            }
        }
    }
    eprintln!(
        "[UDP:{label}] request {request_id} gave up after {MAX_ROUNDS} rounds, {} still missing",
        pending.len()
    );
}

fn parse_missing(buf: &[u8]) -> Vec<u32> {
    buf[5..]
        .chunks_exact(4)
        .map(|b| u32::from_be_bytes(b.try_into().unwrap()))
        .collect()
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
