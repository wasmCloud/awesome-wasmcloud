#[cfg(test)]
mod tls;
#[cfg(test)]
mod tests {
    fn example_root() -> std::path::PathBuf {
        std::env::var_os("TLS_EXAMPLE_ROOT")
            .expect("set by run.py")
            .into()
    }
    use super::tls::{
        EchoServer, PeerMode, engine_with_p3_and_tls, install_default_crypto_provider,
        start_tls_echo_server,
    };
    use anyhow::Result;
    use std::{collections::HashMap, sync::Arc, time::Duration};
    use tokio::time::timeout;
    use wash_runtime::{
        host::{HostApi, HostBuilder, allowed_loopback::AllowedLoopbackPort},
        types::{LocalResources, Workload, WorkloadStartRequest, WorkloadState},
    };
    async fn plugin_probe(
        trust_server: bool,
        server_name: &str,
        allow_egress: bool,
        mode: PeerMode,
    ) -> Result<(u16, String, bool)> {
        use wash_runtime::{
            engine::Engine,
            host::http::{DevRouter, Ingress},
            plugin::component_host::ComponentHostPlugin,
            wit::WitInterface,
        };
        install_default_crypto_provider();
        let EchoServer {
            addr,
            cert_der,
            ping_rx,
        } = start_tls_echo_server(mode).await?;
        let policy = Arc::new(wash_runtime::sockets::policy::SocketPolicy {
            host_loopback_enabled: true,
            ..Default::default()
        });
        let engine = if trust_server {
            engine_with_p3_and_tls(&cert_der)?
        } else {
            Engine::builder()
                .with_socket_policy(policy.clone())
                .build()?
        };
        let ingress = Ingress::new(DevRouter::default(), "127.0.0.1:0".parse()?).await?;
        let ingress_addr = ingress.addr();
        let plugin_wasm = std::fs::read(
            example_root().join("target/wasm32-wasip2/release/tls_client_plugin.wasm"),
        )?;
        let grant: Arc<[AllowedLoopbackPort]> = if allow_egress {
            Arc::from([AllowedLoopbackPort::tcp(addr.port())])
        } else {
            Arc::from([])
        };
        let plugin = ComponentHostPlugin::builder()
            .id("tls-proof")
            .wasm(&plugin_wasm)
            .engine(engine.clone())
            .allowed_host_loopback_ports(grant)
            .socket_policy(policy)
            .build()
            .await?;
        let host = HostBuilder::new()
            .with_engine(engine)
            .with_http_handler(Arc::new(ingress))
            .with_plugin(Arc::new(plugin))?
            .build()?
            .start()
            .await?;
        let caller = std::fs::read(
            example_root()
                .join("verification/caller/target/wasm32-wasip2/release/tls_client_caller.wasm"),
        )?;
        let http = WitInterface {
            namespace: "wasi".into(),
            package: "http".into(),
            interfaces: ["incoming-handler".into()].into_iter().collect(),
            version: Some(semver::Version::parse("0.2.2")?),
            name: None,
            config: HashMap::from([("host".into(), "tls-proof".into())]).into(),
        };
        let tls = WitInterface {
            namespace: "example".into(),
            package: "tls".into(),
            interfaces: ["client".into()].into_iter().collect(),
            version: Some(semver::Version::parse("0.1.0")?),
            name: None,
            config: Default::default(),
        };
        let start = WorkloadStartRequest {
            workload_id: uuid::Uuid::new_v4().to_string(),
            workload: Workload {
                namespace: "test".into(),
                name: "tls-proof".into(),
                annotations: HashMap::new(),
                service: None,
                components: vec![wash_runtime::types::Component {
                    name: "caller".into(),
                    bytes: bytes::Bytes::from(caller),
                    local_resources: LocalResources {
                        memory_limit_mb: 256,
                        cpu_limit: 1,
                        ..Default::default()
                    },
                    pool_size: 1,
                    max_invocations: 100,
                    max_concurrency: 1,
                    ..Default::default()
                }],
                host_interfaces: vec![http, tls],
                volumes: vec![],
            },
        };
        let started = host.workload_start(start).await?;
        assert_eq!(
            started.workload_status.workload_state,
            WorkloadState::Running,
            "{}",
            started.workload_status.message
        );
        let response = reqwest::Client::new()
            .get(format!(
                "http://{ingress_addr}/?address={addr}&name={server_name}"
            ))
            .timeout(Duration::from_secs(10))
            .send()
            .await?;
        let status = response.status().as_u16();
        let body = response.text().await?;
        let got_ping = matches!(
            timeout(Duration::from_millis(250), ping_rx).await,
            Ok(Ok(_))
        );
        println!(
            "TLS plugin trust={trust_server} name={server_name} egress={allow_egress}: HTTP {status}, {body:?}, application_data={got_ping}"
        );
        Ok((status, body, got_ping))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_trusted_certificate_round_trip() -> Result<()> {
        let (status, body, ping) = plugin_probe(true, "localhost", true, PeerMode::Echo).await?;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body, "PONG\r\n");
        assert!(ping);
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_clean_tls_close_does_not_wait_for_tcp_fin() -> Result<()> {
        let (status, body, ping) =
            plugin_probe(true, "localhost", true, PeerMode::KeepTcpOpen).await?;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body, "PONG\r\n");
        assert!(ping);
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_untrusted_certificate_fails_closed() -> Result<()> {
        let (status, body, ping) = plugin_probe(false, "localhost", true, PeerMode::Echo).await?;
        assert_eq!(status, 502);
        assert!(
            body.to_lowercase().contains("unknownissuer")
                || body.to_lowercase().contains("unknown issuer"),
            "{body}"
        );
        assert!(!ping);
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_wrong_hostname_fails_closed() -> Result<()> {
        let (status, body, ping) =
            plugin_probe(true, "wrong.example", true, PeerMode::Echo).await?;
        assert_eq!(status, 502);
        assert!(
            body.to_lowercase().contains("not valid for name")
                || body.to_lowercase().contains("notvalidforname"),
            "{body}"
        );
        assert!(!ping);
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_denied_egress_fails_before_tls() -> Result<()> {
        let (status, body, ping) = plugin_probe(true, "localhost", false, PeerMode::Echo).await?;
        assert_eq!(status, 502);
        assert!(body.contains("connect:"), "{body}");
        assert!(body.contains("AccessDenied"), "{body}");
        assert!(!ping);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_expired_certificate_fails_closed() -> Result<()> {
        let (status, body, ping) = plugin_probe(true, "localhost", true, PeerMode::Expired).await?;
        assert_eq!(status, 502);
        assert!(body.to_lowercase().contains("expired"), "{body}");
        assert!(!ping);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_caps_the_reply_size() -> Result<()> {
        let (status, body, ping) =
            plugin_probe(true, "localhost", true, PeerMode::Oversized).await?;
        assert_eq!(status, 502);
        assert!(body.contains("1024-byte"), "{body}");
        assert!(ping);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_times_out_a_stalled_handshake() -> Result<()> {
        let start = std::time::Instant::now();
        let (status, body, ping) = plugin_probe(true, "localhost", true, PeerMode::Stalled).await?;
        assert_eq!(status, 502);
        assert!(body.contains("deadline exceeded"), "{body}");
        assert!(!ping);
        assert!(start.elapsed() < Duration::from_secs(8));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_rejects_a_reply_without_tls_close_notify() -> Result<()> {
        let (status, body, ping) =
            plugin_probe(true, "localhost", true, PeerMode::Truncated).await?;
        assert_eq!(status, 502);
        assert!(body.contains("TLS receive:"), "{body}");
        assert!(
            body.contains("close_notify") || body.to_lowercase().contains("unexpected end"),
            "{body}"
        );
        assert!(ping);
        Ok(())
    }

    #[test]
    fn only_the_linked_tls_draft_is_a_host_builtin() {
        use wash_runtime::wit::WitInterface;
        assert!(WitInterface::from("wasi:tls/client@0.3.0-draft").is_host_builtin());
        for version in ["0.2.0-draft", "0.3.0", "0.4.0-draft"] {
            assert!(
                !WitInterface::from(format!("wasi:tls/client@{version}").as_str())
                    .is_host_builtin()
            );
        }
    }
}
