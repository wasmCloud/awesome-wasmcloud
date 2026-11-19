//! One KV connection: the socket, the handshake, and a request/response pair.
//!
//! Every wait here is an `await` on `wasi:sockets@0.3.0`, so a call waiting on
//! the cluster yields the plugin's executor to its other callers rather than
//! holding it.
//!
//! The protocol is pipelined in principle — requests carry an opaque the
//! response echoes — but this connection issues one request at a time, so the
//! opaque is a consistency check rather than a demultiplexing key.

use crate::{
    bindings,
    config::Binding,
    proto::{self, op, status, ResponseHeader},
};
use bindings::wasi::sockets::types::{
    IpAddress, IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress, TcpSocket,
};
use std::{
    future::{poll_fn, Future},
    pin::{pin, Pin},
    task::Poll,
};

type Completion = Pin<Box<dyn Future<Output = Result<(), String>>>>;

/// Drive the bounded ciphertext bridge and monitor transport failures only
/// while a protocol operation is active. Dropping the connection cancels both.
#[derive(Default)]
struct Transport {
    forwarding: Option<Pin<Box<dyn Future<Output = ()>>>>,
    completions: Vec<Completion>,
}

impl Transport {
    async fn drive<F: Future>(&mut self, operation: F) -> Result<F::Output, String> {
        let mut operation = pin!(operation);
        poll_fn(|cx| {
            if let Some(forwarding) = self.forwarding.as_mut() {
                if forwarding.as_mut().poll(cx).is_ready() {
                    self.forwarding = None;
                }
            }
            let mut index = 0;
            while index < self.completions.len() {
                match self.completions[index].as_mut().poll(cx) {
                    Poll::Pending => index += 1,
                    Poll::Ready(result) => {
                        drop(self.completions.swap_remove(index));
                        if let Err(error) = result {
                            return Poll::Ready(Err(error));
                        }
                    }
                }
            }
            operation.as_mut().poll(cx).map(Ok)
        })
        .await
    }
}

struct Io {
    writer: wit_bindgen::StreamWriter<u8>,
    reader: wit_bindgen::StreamReader<u8>,
    pending: Vec<u8>,
}
/// A connected, authenticated connection with a bucket selected.
pub struct Connection {
    io: Io,
    transport: Transport,
    _socket: TcpSocket,
    /// How many vbuckets the bucket has, for routing.
    pub vbuckets: u16,
    /// The collection this binding addresses.
    pub collection_id: u32,
    opaque: u32,
}

/// One response, headers and body separated.
pub struct Reply {
    pub header: ResponseHeader,
    pub extras: Vec<u8>,
    pub value: Vec<u8>,
}

impl Reply {
    pub fn ok(&self) -> bool {
        self.header.status == status::OK
    }
}

fn socket_address(ip: IpAddress, port: u16) -> IpSocketAddress {
    match ip {
        IpAddress::Ipv4(address) => IpSocketAddress::Ipv4(Ipv4SocketAddress { port, address }),
        IpAddress::Ipv6(address) => IpSocketAddress::Ipv6(Ipv6SocketAddress {
            port,
            address,
            flow_info: 0,
            scope_id: 0,
        }),
    }
}

impl Connection {
    /// Open a connection and complete the handshake:
    /// `HELLO` → `SASL_AUTH` → `SELECT_BUCKET` → cluster config → collection id.
    pub async fn open(address: IpAddress, binding: &Binding) -> Result<Self, String> {
        let family = match address {
            IpAddress::Ipv4(_) => IpAddressFamily::Ipv4,
            IpAddress::Ipv6(_) => IpAddressFamily::Ipv6,
        };
        let socket = TcpSocket::create(family).map_err(|e| format!("socket: {e:?}"))?;
        socket
            .connect(socket_address(address, binding.port))
            .await
            .map_err(|e| format!("connect: {e:?}"))?;

        let (encrypted_rx, socket_rx_done) = socket.receive();
        let mut transport = Transport::default();
        transport.completions.push(Box::pin(async move {
            socket_rx_done
                .await
                .map_err(|error| format!("socket receive: {error:?}"))
        }));
        let (writer, reader) = if binding.tls {
            #[cfg(not(feature = "tls"))]
            return Err("this plugin was built without TLS support".into());

            #[cfg(feature = "tls")]
            {
                use bindings::wasi::tls::client::Connector;
                let mut encrypted_rx = encrypted_rx;
                let connector = Connector::new();
                let (mut tls_input_writer, tls_input) = bindings::wit_stream::new();
                let (reader, tls_rx_done) = connector.receive(tls_input);
                let (writer, plaintext_tx) = bindings::wit_stream::new();
                let (encrypted_tx, tls_tx_done) = connector.send(plaintext_tx);
                let socket_tx_done = socket.send(encrypted_tx);
                transport.completions.push(Box::pin(async move {
                    tls_rx_done
                        .await
                        .map_err(|error| format!("TLS receive: {}", error.to_debug_string()))
                }));
                transport.completions.push(Box::pin(async move {
                    tls_tx_done
                        .await
                        .map_err(|error| format!("TLS send: {}", error.to_debug_string()))
                }));
                transport.completions.push(Box::pin(async move {
                    socket_tx_done
                        .await
                        .map_err(|error| format!("socket send: {error:?}"))
                }));
                transport.forwarding = Some(Box::pin(async move {
                    loop {
                        let (_, chunk) = encrypted_rx.read(Vec::with_capacity(4096)).await;
                        if chunk.is_empty() || !tls_input_writer.write_all(chunk).await.is_empty() {
                            break;
                        }
                    }
                }));

                // Wire both directions before the handshake. No KV frames or
                // credentials are written until the original DNS name verifies.
                transport
                    .drive(Connector::connect(connector, binding.host.clone()))
                    .await?
                    .map_err(|error| format!("TLS handshake: {}", error.to_debug_string()))?;
                (writer, reader)
            }
        } else {
            let (writer, send_body) = bindings::wit_stream::new();
            let socket_tx_done = socket.send(send_body);
            transport.completions.push(Box::pin(async move {
                socket_tx_done
                    .await
                    .map_err(|error| format!("socket send: {error:?}"))
            }));
            (writer, encrypted_rx)
        };
        let mut conn = Self {
            io: Io {
                writer,
                reader,
                pending: Vec::new(),
            },
            transport,
            _socket: socket,
            vbuckets: 0,
            collection_id: 0,
            opaque: 0,
        };
        let Binding {
            username,
            password,
            bucket,
            scope,
            collection,
            ..
        } = binding;

        // HELLO. A server that declines COLLECTIONS would need bare keys, and
        // this implementation only speaks the prefixed form.
        let hello = conn
            .call(
                op::HELLO,
                0,
                b"wasmcloud-couchbase",
                &[],
                &proto::hello_features(),
                0,
            )
            .await?;
        if !hello.ok() {
            return Err(format!(
                "HELLO refused: status 0x{:04x}",
                hello.header.status
            ));
        }
        let agreed = proto::parse_hello_response(&hello.value);
        if !agreed.contains(&proto::feature::COLLECTIONS) {
            return Err(
                "the cluster did not agree to COLLECTIONS, which this plugin requires".to_string(),
            );
        }

        // SASL PLAIN. The mechanism list is not consulted: PLAIN is the only
        // one implemented, and a server that refuses it says so here.
        let auth = conn
            .call(
                op::SASL_AUTH,
                0,
                b"PLAIN",
                &[],
                &proto::sasl_plain(username, password),
                0,
            )
            .await?;
        if !auth.ok() {
            return Err(match auth.header.status {
                status::AUTH_ERROR => "authentication failed".to_string(),
                other => format!("SASL PLAIN refused: status 0x{other:04x}"),
            });
        }

        let selected = conn
            .call(op::SELECT_BUCKET, 0, bucket.as_bytes(), &[], &[], 0)
            .await?;
        if !selected.ok() {
            return Err(match selected.header.status {
                // The server answers the same for a bucket that does not exist,
                // so as not to reveal which buckets do.
                status::ACCESS_DENIED | status::NOT_FOUND => format!(
                    "bucket `{bucket}` does not exist, or user `{username}` has no role on it"
                ),
                other => format!("could not select bucket `{bucket}`: status 0x{other:04x}"),
            });
        }

        // Routing needs the vbucket count, which only the cluster config has.
        let config = conn
            .call(op::GET_CLUSTER_CONFIG, 0, &[], &[], &[], 0)
            .await?;
        if !config.ok() {
            return Err(format!(
                "could not read the cluster config: status 0x{:04x}",
                config.header.status
            ));
        }
        conn.vbuckets = proto::parse_vbucket_count(&config.value)?;

        // The collection id turns a scope/collection name into the key prefix.
        let path = format!("{scope}.{collection}");
        let cid = conn
            .call(op::GET_COLLECTION_ID, 0, path.as_bytes(), &[], &[], 0)
            .await?;
        if !cid.ok() {
            return Err(match cid.header.status {
                status::UNKNOWN_COLLECTION => format!("no such scope or collection: `{path}`"),
                other => format!("could not resolve `{path}`: status 0x{other:04x}"),
            });
        }
        conn.collection_id = proto::parse_collection_id(&cid.extras)?;

        Ok(conn)
    }

    /// Send one request and read its response.
    pub async fn call(
        &mut self,
        opcode: u8,
        vbucket: u16,
        key: &[u8],
        extras: &[u8],
        value: &[u8],
        cas: u64,
    ) -> Result<Reply, String> {
        self.opaque = self.opaque.wrapping_add(1);
        let opaque = self.opaque;
        let frame = proto::request(opcode, vbucket, key, extras, value, opaque, cas)?;

        self.transport
            .drive(self.io.exchange(frame, opcode, opaque))
            .await?
    }

    /// The vbucket a document id routes to on this connection.
    pub fn vbucket_for(&self, key: &str) -> u16 {
        proto::vbucket_for(key, self.vbuckets)
    }

    /// The wire form of a document id for this connection's collection.
    pub fn wire_key(&self, key: &str) -> Result<Vec<u8>, String> {
        proto::encode_key(self.collection_id, key)
    }
}

impl Io {
    async fn exchange(&mut self, frame: Vec<u8>, opcode: u8, opaque: u32) -> Result<Reply, String> {
        let rejected = self.writer.write_all(frame).await;
        if !rejected.is_empty() {
            return Err("the connection stopped accepting writes".to_string());
        }

        let head = self.read_exactly(proto::HEADER_LEN).await?;
        let header = ResponseHeader::parse(&head)?;
        if header.opcode != opcode {
            return Err(format!(
                "response opcode 0x{:02x} does not match 0x{opcode:02x}",
                header.opcode
            ));
        }
        let body = self.read_exactly(header.body_len).await?;

        // A mismatch means the stream has desynchronized; continuing would
        // attribute one call's answer to another.
        if header.opaque != opaque {
            return Err(format!(
                "response out of order: expected opaque {opaque}, got {}",
                header.opaque
            ));
        }

        let value_at = header.value_offset();
        Ok(Reply {
            extras: body[..header.extras_len].to_vec(),
            value: body[value_at..].to_vec(),
            header,
        })
    }

    /// Read exactly `want` bytes, buffering whatever arrives past them.
    async fn read_exactly(&mut self, want: usize) -> Result<Vec<u8>, String> {
        while self.pending.len() < want {
            // An empty read means the stream ended: the cluster hung up, or
            // the socket was closed under us. Either way there is no more of
            // this response coming.
            let (_result, buffer) = self.reader.read(Vec::with_capacity(want)).await;
            if buffer.is_empty() {
                return Err("the cluster closed the connection".to_string());
            }
            self.pending.extend_from_slice(&buffer);
        }
        Ok(self.pending.drain(..want).collect())
    }
}
