mod bindings {
    wit_bindgen::generate!({ world: "tls-client", generate_all });
}
use bindings::wasi::{
    sockets::types::{
        IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress, TcpSocket,
    },
    tls::client::Connector,
};
struct Component;
impl bindings::exports::example::tls::client::Guest for Component {
    async fn ping(address: String, server_name: String) -> Result<String, String> {
        crate::deadline::within(
            ping(address, server_name),
            bindings::wasi::clocks::monotonic_clock::wait_for(5_000_000_000),
        )
        .await
        .unwrap_or_else(|| Err("TLS call deadline exceeded".into()))
    }
}
async fn ping(address: String, server_name: String) -> Result<String, String> {
    let address: std::net::SocketAddr = address.parse().map_err(|e| format!("address: {e}"))?;
    let (family, address) = match address {
        std::net::SocketAddr::V4(address) => {
            let a = address.ip().octets();
            (
                IpAddressFamily::Ipv4,
                IpSocketAddress::Ipv4(Ipv4SocketAddress {
                    port: address.port(),
                    address: (a[0], a[1], a[2], a[3]),
                }),
            )
        }
        std::net::SocketAddr::V6(address) => {
            let a = address.ip().segments();
            (
                IpAddressFamily::Ipv6,
                IpSocketAddress::Ipv6(Ipv6SocketAddress {
                    port: address.port(),
                    address: (a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7]),
                    flow_info: address.flowinfo(),
                    scope_id: address.scope_id(),
                }),
            )
        }
    };
    let socket = TcpSocket::create(family).map_err(|e| format!("create: {e:?}"))?;
    socket
        .connect(address)
        .await
        .map_err(|e| format!("connect: {e:?}"))?;
    let (encrypted_rx, sock_rx_done) = socket.receive();
    let connector = Connector::new();
    // Attach both directions before starting the handshake: the host needs
    // these streams to read and write the handshake's own TLS records.
    let (mut plaintext_rx, tls_rx_done) = connector.receive(encrypted_rx);
    let (mut writer, plaintext_tx) = bindings::wit_stream::new();
    let (encrypted_tx, tls_tx_done) = connector.send(plaintext_tx);
    let sock_tx_done = socket.send(encrypted_tx);
    // This name controls SNI and certificate identity verification. Keep the
    // original DNS name here even when TCP used its resolved IP address.
    Connector::connect(connector, server_name)
        .await
        .map_err(|e| format!("TLS handshake: {}", e.to_debug_string()))?;
    // Application data is sent only after the verified handshake succeeds.
    if !writer.write_all(b"PING\r\n".to_vec()).await.is_empty() {
        return Err("write failed".into());
    }
    drop(writer); // The TLS transform emits close_notify for the output half.
    let mut response = Vec::new();
    loop {
        let (_, chunk) = plaintext_rx.read(Vec::with_capacity(128)).await;
        if chunk.is_empty() {
            break;
        }
        if response.len() + chunk.len() > 1024 {
            return Err("TLS response exceeds the 1024-byte example limit".into());
        }
        response.extend_from_slice(&chunk);
    }
    // An ended stream alone does not prove success. The completion futures
    // also report socket failures, truncated TLS records and missing close_notify.
    sock_rx_done
        .await
        .map_err(|e| format!("socket receive: {e:?}"))?;
    sock_tx_done
        .await
        .map_err(|e| format!("socket send: {e:?}"))?;
    tls_rx_done
        .await
        .map_err(|e| format!("TLS receive: {}", e.to_debug_string()))?;
    tls_tx_done
        .await
        .map_err(|e| format!("TLS send: {}", e.to_debug_string()))?;
    String::from_utf8(response).map_err(|e| e.to_string())
}
bindings::export!(Component with_types_in bindings);
