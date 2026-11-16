//! Representation of log messages in JSON. The rest of the node treats a message as opaque
//! bytes; only the types here know how they look on the wire:
//! - [`JsonValue`]: a client's message, any JSON value (Maelstrom sends integers);
//! - [`Base64Bytes`]: raw bytes passed between nodes.

use std::fmt;

use base64::{Engine, display::Base64Display, engine::general_purpose::STANDARD};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Visitor},
    ser,
};
use serde_json::value::RawValue;
use tokio_util::bytes::Bytes;

/// A client's message: any JSON value, kept as its JSON text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonValue(pub Bytes);

impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // `RawValue` would keep the client's text without parsing it, but it can't be read from
        // inside `#[serde(flatten)]` or an internally tagged enum, and `Body`/`Payload` are both.
        let value = serde_json::Value::deserialize(deserializer)?;
        let text = serde_json::to_vec(&value).map_err(de::Error::custom)?;
        Ok(Self(Bytes::from(text)))
    }
}

impl Serialize for JsonValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Written verbatim, without building a `Value`. Every message stored through the client
        // protocol is JSON text, so this fails only on a bug: the write fails and the node stops.
        let raw: &RawValue = serde_json::from_slice(&self.0)
            .map_err(|e| ser::Error::custom(format!("stored message is not a JSON value: {e}")))?;
        raw.serialize(serializer)
    }
}

/// Raw bytes, as a standard base64 string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Base64Bytes(pub Bytes);

impl Serialize for Base64Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&Base64Display::new(&self.0, &STANDARD))
    }
}

impl<'de> Deserialize<'de> for Base64Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Base64Visitor)
    }
}

struct Base64Visitor;

impl Visitor<'_> for Base64Visitor {
    type Value = Base64Bytes;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a base64 string")
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<Self::Value, E> {
        let bytes = STANDARD.decode(text).map_err(E::custom)?;
        Ok(Base64Bytes(Bytes::from(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_value_roundtrips_any_json() {
        for value in [json!(7), json!("text"), json!({"a": [1, null]})] {
            let parsed: JsonValue = serde_json::from_value(value.clone()).expect("deserializable");
            assert_eq!(serde_json::to_value(&parsed).expect("serializable"), value);
        }
    }

    #[test]
    fn json_value_rejects_non_json_bytes() {
        let stored = JsonValue(Bytes::from_static(&[0xff, 0x01]));
        assert!(serde_json::to_string(&stored).is_err());
    }

    #[test]
    fn base64_roundtrips_arbitrary_bytes() {
        let bytes = Base64Bytes(Bytes::from_static(&[0, 1, 0xfe, 0xff]));
        let text = serde_json::to_string(&bytes).expect("serializable");
        assert_eq!(text, r#""AAH+/w==""#);
        let parsed: Base64Bytes = serde_json::from_str(&text).expect("deserializable");
        assert_eq!(parsed, bytes);
    }

    #[test]
    fn base64_rejects_invalid_input() {
        assert!(serde_json::from_str::<Base64Bytes>(r#""not base64!""#).is_err());
    }
}
