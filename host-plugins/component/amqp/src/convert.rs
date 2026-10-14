//! Moving messages between the WIT types workloads use and the amqprs types
//! the client uses.
//!
//! amqprs panics on input it cannot encode, such as a string over 255 bytes in
//! a field AMQP limits to that. A panic in the plugin kills the instance every
//! workload shares, so everything a workload sends is checked here first, and
//! a bad value comes back to that workload as `error::invalid`.
//!
//! The plugin both exports and imports `wasmcloud:amqp/types`, and wit-bindgen
//! generates a separate Rust copy for each. They line up with the two
//! directions: what a workload *publishes* arrives as the `published` types,
//! and what the plugin *delivers* to a handler goes out as the `delivered`
//! ones.

use amqprs::{BasicProperties, ByteArray, Deliver, FieldTable, FieldValue, LongStr, ShortStr};

use crate::bindings::exports::wasmcloud::amqp::types as published;
use crate::bindings::wasmcloud::amqp::types as delivered;
use crate::config::MAX_NAME_BYTES;

/// The largest body the plugin publishes. It is RabbitMQ's default
/// `max_message_size`, and bigger bodies would be copied several times inside
/// an instance that every workload shares.
pub const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
/// The most the headers may take once encoded. They travel in one frame that
/// the broker bounds, and an oversized frame makes it close the connection for
/// every workload, so the plugin refuses them first.
pub const MAX_HEADERS_BYTES: usize = 16 * 1024;

const TRANSIENT: u8 = 1;
const PERSISTENT: u8 = 2;

/// A publish that has been checked and is ready for amqprs.
pub struct Outgoing {
    pub exchange: String,
    pub routing_key: String,
    pub properties: BasicProperties,
    pub body: Vec<u8>,
}

/// Checks a workload's publish and turns it into what the client sends.
pub fn outgoing(
    exchange: String,
    routing_key: String,
    message: published::Message,
) -> Result<Outgoing, published::Error> {
    short("exchange", &exchange)?;
    short("routing-key", &routing_key)?;
    if message.body.len() > MAX_BODY_BYTES {
        return Err(published::Error::Invalid(format!(
            "the body is {} bytes, and the plugin publishes at most {MAX_BODY_BYTES}",
            message.body.len()
        )));
    }

    let published::Properties {
        content_type,
        content_encoding,
        headers,
        delivery_mode,
        priority,
        correlation_id,
        reply_to,
        expiration,
        message_id,
        timestamp,
        message_type,
        user_id,
        app_id,
    } = message.properties;

    for (name, text) in [
        ("content-type", &content_type),
        ("content-encoding", &content_encoding),
        ("correlation-id", &correlation_id),
        ("reply-to", &reply_to),
        ("expiration", &expiration),
        ("message-id", &message_id),
        ("message-type", &message_type),
        ("user-id", &user_id),
        ("app-id", &app_id),
    ] {
        if let Some(text) = text {
            short(name, text)?;
        }
    }

    let properties = BasicProperties::new(
        content_type,
        content_encoding,
        field_table(headers)?,
        delivery_mode.map(|mode| match mode {
            published::DeliveryMode::Transient => TRANSIENT,
            published::DeliveryMode::Persistent => PERSISTENT,
        }),
        priority,
        correlation_id,
        reply_to,
        expiration,
        message_id,
        timestamp,
        message_type,
        user_id,
        app_id,
        None,
    );

    Ok(Outgoing {
        exchange,
        routing_key,
        properties,
        body: message.body,
    })
}

/// An AMQP short string holds at most 255 bytes.
fn short(what: &str, value: &str) -> Result<(), published::Error> {
    if value.len() > MAX_NAME_BYTES {
        return Err(published::Error::Invalid(format!(
            "{what} is {} bytes, and AMQP allows {MAX_NAME_BYTES}",
            value.len()
        )));
    }
    Ok(())
}

fn field_table(
    headers: Vec<(String, published::HeaderValue)>,
) -> Result<Option<FieldTable>, published::Error> {
    if headers.is_empty() {
        return Ok(None);
    }

    let mut table = FieldTable::new();
    let mut encoded = 0;
    for (name, value) in headers {
        short("a header name", &name)?;
        let (value, value_bytes) = field_value(value)?;
        // A one-byte name length, the name, a one-byte type tag, the value.
        encoded += 1 + name.len() + 1 + value_bytes;
        if encoded > MAX_HEADERS_BYTES {
            return Err(published::Error::Invalid(format!(
                "the headers take more than {MAX_HEADERS_BYTES} bytes"
            )));
        }
        let name = ShortStr::try_from(name).map_err(|err| {
            published::Error::Invalid(format!("a header name is not valid: {err}"))
        })?;
        table.insert(name, value);
    }
    Ok(Some(table))
}

/// The AMQP value, and how many bytes it takes once encoded.
fn field_value(value: published::HeaderValue) -> Result<(FieldValue, usize), published::Error> {
    use published::HeaderValue;

    let too_long = |err| published::Error::Invalid(format!("a header value is too long: {err}"));
    Ok(match value {
        HeaderValue::Boolean(value) => (FieldValue::t(value), 1),
        HeaderValue::Integer(value) => (FieldValue::l(value), 8),
        HeaderValue::Float(value) => (FieldValue::d(value), 8),
        HeaderValue::Timestamp(value) => (FieldValue::T(value), 8),
        HeaderValue::Text(value) => {
            let bytes = 4 + value.len();
            (
                FieldValue::S(LongStr::try_from(value).map_err(too_long)?),
                bytes,
            )
        }
        HeaderValue::Bytes(value) => {
            let bytes = 4 + value.len();
            (
                FieldValue::x(ByteArray::try_from(value).map_err(too_long)?),
                bytes,
            )
        }
    })
}

/// Turns what the broker delivered into what a workload's handler receives.
pub fn incoming(
    queue: &str,
    deliver: &Deliver,
    properties: &BasicProperties,
    body: Vec<u8>,
) -> delivered::Delivery {
    delivered::Delivery {
        queue: queue.to_string(),
        exchange: deliver.exchange().clone(),
        routing_key: deliver.routing_key().clone(),
        redelivered: deliver.redelivered(),
        message: delivered::Message {
            properties: properties_of(properties),
            body,
        },
    }
}

fn properties_of(properties: &BasicProperties) -> delivered::Properties {
    use delivered::DeliveryMode;

    delivered::Properties {
        content_type: properties.content_type().cloned(),
        content_encoding: properties.content_encoding().cloned(),
        headers: properties.headers().map(headers_of).unwrap_or_default(),
        delivery_mode: match properties.delivery_mode() {
            Some(TRANSIENT) => Some(DeliveryMode::Transient),
            Some(PERSISTENT) => Some(DeliveryMode::Persistent),
            _ => None,
        },
        priority: properties.priority(),
        correlation_id: properties.correlation_id().cloned(),
        reply_to: properties.reply_to().cloned(),
        expiration: properties.expiration().cloned(),
        message_id: properties.message_id().cloned(),
        timestamp: properties.timestamp(),
        message_type: properties.message_type().cloned(),
        user_id: properties.user_id().cloned(),
        app_id: properties.app_id().cloned(),
    }
}

/// The headers a workload can be given, in name order so a delivery reads the
/// same every time. A header of a shape the WIT type has no case for is left
/// out.
fn headers_of(table: &FieldTable) -> Vec<(String, delivered::HeaderValue)> {
    use delivered::HeaderValue;

    let mut headers: Vec<(String, HeaderValue)> = table
        .as_ref()
        .iter()
        .filter_map(|(name, value)| {
            let value = match value {
                FieldValue::t(value) => HeaderValue::Boolean(*value),
                FieldValue::b(value) => HeaderValue::Integer(i64::from(*value)),
                FieldValue::B(value) => HeaderValue::Integer(i64::from(*value)),
                FieldValue::s(value) => HeaderValue::Integer(i64::from(*value)),
                FieldValue::u(value) => HeaderValue::Integer(i64::from(*value)),
                FieldValue::I(value) => HeaderValue::Integer(i64::from(*value)),
                FieldValue::i(value) => HeaderValue::Integer(i64::from(*value)),
                FieldValue::l(value) => HeaderValue::Integer(*value),
                FieldValue::f(value) => HeaderValue::Float(f64::from(*value)),
                FieldValue::d(value) => HeaderValue::Float(*value),
                FieldValue::S(value) => HeaderValue::Text(String::from(value.clone())),
                FieldValue::T(value) => HeaderValue::Timestamp(*value),
                FieldValue::x(value) => HeaderValue::Bytes(Vec::from(value.clone())),
                FieldValue::D(_) | FieldValue::A(_) | FieldValue::F(_) | FieldValue::V => {
                    return None;
                }
            };
            Some((String::from(name.clone()), value))
        })
        .collect();
    headers.sort_by(|(a, _), (b, _)| a.cmp(b));
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    use delivered::HeaderValue as DeliveredHeader;
    use published::{Error, HeaderValue};

    /// Properties with nothing set, built from either set of generated types.
    macro_rules! bare {
        ($types:ident) => {
            $types::Properties {
                content_type: None,
                content_encoding: None,
                headers: Vec::new(),
                delivery_mode: None,
                priority: None,
                correlation_id: None,
                reply_to: None,
                expiration: None,
                message_id: None,
                timestamp: None,
                message_type: None,
                user_id: None,
                app_id: None,
            }
        };
    }

    /// Properties with everything set, built from either set of generated
    /// types. Headers are in name order, which is how a delivery lists them.
    macro_rules! full {
        ($types:ident) => {
            $types::Properties {
                content_type: Some("application/json".into()),
                content_encoding: Some("gzip".into()),
                headers: vec![
                    ("attempt".into(), $types::HeaderValue::Integer(3)),
                    ("blob".into(), $types::HeaderValue::Bytes(vec![1, 2, 3])),
                    ("ratio".into(), $types::HeaderValue::Float(0.5)),
                    ("sent".into(), $types::HeaderValue::Timestamp(1_700_000_000)),
                    ("source".into(), $types::HeaderValue::Text("billing".into())),
                    ("urgent".into(), $types::HeaderValue::Boolean(true)),
                ],
                delivery_mode: Some($types::DeliveryMode::Persistent),
                priority: Some(5),
                correlation_id: Some("corr".into()),
                reply_to: Some("replies".into()),
                expiration: Some("60000".into()),
                message_id: Some("msg-1".into()),
                timestamp: Some(1_700_000_001),
                message_type: Some("order.created".into()),
                user_id: Some("wasmcloud".into()),
                app_id: Some("checkout".into()),
            }
        };
    }

    fn message(properties: published::Properties) -> published::Message {
        published::Message {
            properties,
            body: b"hello".to_vec(),
        }
    }

    fn publish(properties: published::Properties) -> Result<Outgoing, Error> {
        outgoing("".into(), "q".into(), message(properties))
    }

    #[test]
    fn properties_survive_a_round_trip() {
        let sent = outgoing("ex".into(), "key".into(), message(full!(published))).unwrap();
        assert_eq!(sent.exchange, "ex");
        assert_eq!(sent.routing_key, "key");
        assert_eq!(properties_of(&sent.properties), full!(delivered));
    }

    #[test]
    fn a_message_without_properties_stays_bare() {
        let sent = outgoing("".into(), "queue".into(), message(bare!(published))).unwrap();
        assert_eq!(sent.exchange, "");
        assert_eq!(sent.routing_key, "queue");
        assert_eq!(sent.body, b"hello");
        assert_eq!(properties_of(&sent.properties), bare!(delivered));
    }

    #[test]
    fn delivery_modes_map_to_the_amqp_codes() {
        for (mode, code) in [
            (published::DeliveryMode::Transient, TRANSIENT),
            (published::DeliveryMode::Persistent, PERSISTENT),
        ] {
            let properties = published::Properties {
                delivery_mode: Some(mode),
                ..bare!(published)
            };
            let sent = publish(properties).unwrap();
            assert_eq!(sent.properties.delivery_mode(), Some(code));
        }
    }

    #[test]
    fn a_name_over_255_bytes_is_invalid() {
        let long = "x".repeat(MAX_NAME_BYTES + 1);
        let ok = "x".repeat(MAX_NAME_BYTES);
        assert!(outgoing(ok.clone(), ok.clone(), message(bare!(published))).is_ok());
        assert!(matches!(
            outgoing(long.clone(), "k".into(), message(bare!(published))),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            outgoing("e".into(), long, message(bare!(published))),
            Err(Error::Invalid(_))
        ));
    }

    #[test]
    fn every_short_string_property_is_checked() {
        let long = Some("x".repeat(MAX_NAME_BYTES + 1));
        let cases = [
            published::Properties {
                content_type: long.clone(),
                ..bare!(published)
            },
            published::Properties {
                content_encoding: long.clone(),
                ..bare!(published)
            },
            published::Properties {
                correlation_id: long.clone(),
                ..bare!(published)
            },
            published::Properties {
                reply_to: long.clone(),
                ..bare!(published)
            },
            published::Properties {
                expiration: long.clone(),
                ..bare!(published)
            },
            published::Properties {
                message_id: long.clone(),
                ..bare!(published)
            },
            published::Properties {
                message_type: long.clone(),
                ..bare!(published)
            },
            published::Properties {
                user_id: long.clone(),
                ..bare!(published)
            },
            published::Properties {
                app_id: long.clone(),
                ..bare!(published)
            },
        ];
        for properties in cases {
            assert!(matches!(publish(properties), Err(Error::Invalid(_))));
        }
    }

    #[test]
    fn a_header_name_over_255_bytes_is_invalid() {
        let properties = published::Properties {
            headers: vec![("x".repeat(256), HeaderValue::Boolean(true))],
            ..bare!(published)
        };
        assert!(matches!(publish(properties), Err(Error::Invalid(_))));
    }

    #[test]
    fn oversized_headers_are_invalid() {
        let properties = published::Properties {
            headers: vec![(
                "big".into(),
                HeaderValue::Text("x".repeat(MAX_HEADERS_BYTES)),
            )],
            ..bare!(published)
        };
        assert!(matches!(publish(properties), Err(Error::Invalid(_))));
    }

    #[test]
    fn an_oversized_body_is_invalid() {
        let big = published::Message {
            properties: bare!(published),
            body: vec![0; MAX_BODY_BYTES + 1],
        };
        assert!(matches!(
            outgoing("".into(), "q".into(), big),
            Err(Error::Invalid(_))
        ));
        let limit = published::Message {
            properties: bare!(published),
            body: vec![0; MAX_BODY_BYTES],
        };
        assert!(outgoing("".into(), "q".into(), limit).is_ok());
    }

    #[test]
    fn headers_of_unsupported_shapes_are_left_out_of_a_delivery() {
        let mut table = FieldTable::new();
        table.insert("kept".try_into().unwrap(), FieldValue::I(7));
        table.insert(
            "nested".try_into().unwrap(),
            FieldValue::F(FieldTable::new()),
        );
        table.insert("nothing".try_into().unwrap(), FieldValue::V);
        assert_eq!(
            headers_of(&table),
            vec![("kept".to_string(), DeliveredHeader::Integer(7))]
        );
    }

    #[test]
    fn narrow_integers_are_widened() {
        let mut table = FieldTable::new();
        table.insert("a".try_into().unwrap(), FieldValue::b(-1));
        table.insert("b".try_into().unwrap(), FieldValue::B(255));
        table.insert("c".try_into().unwrap(), FieldValue::s(-300));
        table.insert("d".try_into().unwrap(), FieldValue::u(65_535));
        table.insert("e".try_into().unwrap(), FieldValue::i(4_000_000_000));
        let values: Vec<_> = headers_of(&table)
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        assert_eq!(
            values,
            [
                DeliveredHeader::Integer(-1),
                DeliveredHeader::Integer(255),
                DeliveredHeader::Integer(-300),
                DeliveredHeader::Integer(65_535),
                DeliveredHeader::Integer(4_000_000_000),
            ]
        );
    }
}
