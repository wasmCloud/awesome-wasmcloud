//! The shared forty-step scenario against real Couchbase KV TLS and HTTPS.
use anyhow::Result;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use wash_runtime::{
    host::{
        HostApi, HostBuilder,
        allowed_loopback::AllowedLoopbackPort,
        http::{
            DefaultOutgoingHandler, DevRouter, Ingress, OutgoingHandler, RequestIoFuture,
            SendFuture,
        },
        http_client::ClientTlsOptions,
    },
    plugin::component_host::ComponentHostPlugin,
    types::{LocalResources, Workload, WorkloadStartRequest, WorkloadState},
    wit::WitInterface,
};

// The reserved hostname routes raw sockets to host loopback. HTTP uses the
// native resolver, so this test-only adapter routes its HTTPS authority to
// localhost, a second verified identity on the fixture's certificate.
struct LoopbackQueryHttp(DefaultOutgoingHandler);

impl OutgoingHandler for LoopbackQueryHttp {
    fn send_request(
        &self,
        workload_id: &str,
        mut request: hyper::Request<wasmtime_wasi_http::WasiBody>,
        options: Option<wasmtime_wasi_http::RequestOptions>,
        future: RequestIoFuture,
    ) -> SendFuture {
        assert_eq!(request.uri().scheme_str(), Some("https"));
        assert_eq!(request.uri().host(), Some("host.wasmcloud.internal"));
        let port = request.uri().port_u16().expect("fixture query port");
        let mut parts = request.uri().clone().into_parts();
        parts.authority = Some(
            format!("localhost:{port}")
                .parse()
                .expect("fixture authority"),
        );
        *request.uri_mut() = hyper::Uri::from_parts(parts).expect("fixture HTTPS URI");
        self.0.send_request(workload_id, request, options, future)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the --couchbase fixture"]
async fn couchbase_tls_and_https_scenario() -> Result<()> {
    super::tls::install_default_crypto_provider();
    let root = PathBuf::from(std::env::var_os("COUCHBASE_TLS_ROOT").expect("set by run.mjs"));
    let kv_port: u16 = std::env::var("COUCHBASE_TLS_KV_PORT")?.parse()?;
    let query_port: u16 = std::env::var("COUCHBASE_TLS_QUERY_PORT")?.parse()?;
    let ca_der = std::fs::read(std::env::var("COUCHBASE_TLS_CA_DER")?)?;
    let engine = super::tls::engine_with_p3_and_tls(&ca_der)?;
    let outgoing = DefaultOutgoingHandler::from_tls_options(
        ClientTlsOptions::default().with_ca_paths([std::env::var("COUCHBASE_TLS_CA_PEM")?.into()]),
    )?;
    let ingress = Ingress::builder(DevRouter::default(), "127.0.0.1:0".parse()?)
        .outgoing_handler(LoopbackQueryHttp(outgoing))
        .build()
        .await?;
    let ingress_addr = ingress.addr();
    let builder = HostBuilder::new()
        .with_engine(engine.clone())
        .with_http_handler(Arc::new(ingress))
        .with_plugin(Arc::new(
            wash_runtime::plugin::wasmcloud_secrets::WasmcloudSecrets::new(),
        ))?;
    let policy = Arc::new(wash_runtime::sockets::policy::SocketPolicy {
        host_loopback_enabled: true,
        ..Default::default()
    });
    let plugin_bytes = std::fs::read(
        root.join("target/tls/wasm32-wasip2/release/couchbase_wasi_sockets_p3_plugin.wasm"),
    )?;
    let plugin = ComponentHostPlugin::builder()
        .id("couchbase-tls")
        .wasm(&plugin_bytes)
        .engine(engine.clone())
        .native_plugins(builder.native_plugins())
        .maybe_host_ref(Some(builder.host_ref()))
        .socket_policy(policy)
        .allowed_host_loopback_ports(Arc::from([
            AllowedLoopbackPort::tcp(kv_port),
            AllowedLoopbackPort::tcp(query_port),
        ]))
        .allowed_hosts(Arc::from([
            format!("host.wasmcloud.internal:{query_port}").parse()?
        ]))
        .build()
        .await?;
    let host = builder
        .with_plugin(Arc::new(plugin))?
        .build()?
        .start()
        .await?;
    let workload_bytes = std::fs::read(
        root.join("../couchbase/verification/scenario/target/wasm32-wasip2/release/cbtest.wasm"),
    )?;
    let http = WitInterface {
        namespace: "wasi".into(),
        package: "http".into(),
        interfaces: ["incoming-handler".into()].into_iter().collect(),
        version: Some(semver::Version::parse("0.2.2")?),
        name: None,
        config: HashMap::from([("host".into(), "couchbase-tls".into())]).into(),
    };
    let couchbase = WitInterface {
        namespace: "wasmcloud".into(),
        package: "couchbase".into(),
        interfaces: [
            "types",
            "sqlpp-types",
            "document-read",
            "document-write",
            "sqlpp",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
        version: Some(semver::Version::parse("0.3.0")?),
        name: None,
        config: HashMap::from([
            (
                "endpoint".into(),
                format!("couchbases://host.wasmcloud.internal:{kv_port}"),
            ),
            ("query-port".into(), query_port.to_string()),
            ("bucket".into(), "testbucket".into()),
            ("username".into(), "appuser".into()),
            ("password".into(), "apppass123".into()),
        ])
        .into(),
    };
    let workload_id = uuid::Uuid::new_v4().to_string();
    let request = WorkloadStartRequest {
        workload_id,
        workload: Workload {
            namespace: "test".into(),
            name: "couchbase-tls".into(),
            annotations: HashMap::new(),
            service: None,
            components: vec![wash_runtime::types::Component {
                name: "scenario".into(),
                bytes: bytes::Bytes::from(workload_bytes),
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
            host_interfaces: vec![http, couchbase],
            volumes: vec![],
        },
    };
    let started = host.workload_start(request.clone()).await?;
    assert_eq!(
        started.workload_status.workload_state,
        WorkloadState::Running,
        "{}",
        started.workload_status.message
    );
    let response = reqwest::Client::new()
        .get(format!("http://{ingress_addr}/"))
        .timeout(Duration::from_secs(180))
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    println!("Couchbase KV TLS and SQL++ HTTPS: HTTP {status}\n{body}");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body.lines().filter(|line| line.contains(": OK")).count(),
        40,
        "{body}"
    );
    assert!(
        !["UNEXPECTED", "WRONG", "FAIL"]
            .iter()
            .any(|marker| body.contains(marker)),
        "{body}"
    );
    let mut reader = request.clone();
    reader.workload_id = uuid::Uuid::new_v4().to_string();
    reader.workload.components[0].bytes = bytes::Bytes::from(std::fs::read(root.join(
        "../../../tests/probe/target/read-only/wasm32-wasip2/release/component_fault_probe.wasm",
    ))?);
    reader.workload.host_interfaces[1].interfaces = ["types", "document-read"]
        .into_iter()
        .map(String::from)
        .collect();
    let started = host.workload_start(reader).await?;
    assert_eq!(
        started.workload_status.workload_state,
        WorkloadState::Running,
        "{}",
        started.workload_status.message
    );
    let read = reqwest::Client::new()
        .get(format!("http://{ingress_addr}/scenario:binary"))
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .text()
        .await?;
    assert!(read.contains("READ OK"), "{read}");

    let mut denied = request.clone();
    denied.workload_id = uuid::Uuid::new_v4().to_string();
    denied.workload.host_interfaces[1]
        .interfaces
        .remove("document-write");
    let rejection = match host.workload_start(denied).await {
        Ok(result) => {
            assert_eq!(result.workload_status.workload_state, WorkloadState::Error);
            result.workload_status.message
        }
        Err(error) => format!("{error:#}"),
    };
    assert!(rejection.contains("document-write"), "{rejection}");

    let mut conflicting = request.clone();
    conflicting.workload_id = uuid::Uuid::new_v4().to_string();
    let mut write_binding = conflicting.workload.host_interfaces[1].clone();
    conflicting.workload.host_interfaces[1].interfaces =
        ["document-read"].into_iter().map(String::from).collect();
    write_binding.interfaces.remove("document-read");
    write_binding.config.insert("bucket", "another-bucket");
    conflicting.workload.host_interfaces.push(write_binding);
    let rejection = match host.workload_start(conflicting).await {
        Ok(result) => {
            assert_eq!(result.workload_status.workload_state, WorkloadState::Error);
            result.workload_status.message
        }
        Err(error) => format!("{error:#}"),
    };
    assert!(rejection.contains("conflicting"), "{rejection}");
    for (endpoint, reason) in [
        (
            format!("couchbases://127.255.255.254:{kv_port}"),
            "certificate",
        ),
        (
            "couchbases://host.wasmcloud.internal:1".into(),
            "AccessDenied",
        ),
    ] {
        let mut rejected = request.clone();
        rejected.workload_id = uuid::Uuid::new_v4().to_string();
        rejected.workload.host_interfaces[1]
            .config
            .insert("endpoint", endpoint);
        let result = host.workload_start(rejected).await?;
        assert_eq!(result.workload_status.workload_state, WorkloadState::Error);
        assert!(
            result.workload_status.message.contains(reason),
            "{}",
            result.workload_status.message
        );
    }
    Ok(())
}
