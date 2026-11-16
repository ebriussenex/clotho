use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, Stdin};

use crate::io::{IoError, MessageReader, MessageWriter};
use crate::maelstrom::protocol::wire::{Msg, MsgHeader};

/// Failures of stdin/stdout. All of them are fatal for the node.
#[derive(Debug, Error)]
pub enum StdioErr {
    /// Reading stdin failed.
    #[error("failed to read msg from stdio: {0}")]
    Read(#[source] std::io::Error),
    /// Writing stdout failed.
    #[error("failed to write msg to stdout: {0}")]
    Write(#[source] std::io::Error),
    /// A message couldn't be serialized, e.g. a stored message is not valid JSON. A bug.
    #[error("failed to serialize msg to json: {0}")]
    Serialize(#[source] serde_json::Error),
}

pub struct StdioReader {
    lines: Lines<BufReader<Stdin>>,
}

impl Default for StdioReader {
    fn default() -> Self {
        Self {
            lines: BufReader::new(tokio::io::stdin()).lines(),
        }
    }
}

impl MessageReader for StdioReader {
    async fn next<'a, T: DeserializeOwned + 'a>(&'a mut self) -> Result<Option<Msg<T>>, IoError> {
        match self.lines.next_line().await.map_err(StdioErr::Read)? {
            Some(line) => decode(&line).map(Some),
            None => Ok(None),
        }
    }
}

fn decode<T: DeserializeOwned>(line: &str) -> Result<Msg<T>, IoError> {
    serde_json::from_str(line).map_err(|source| {
        let header = serde_json::from_str::<MsgHeader>(line).ok();
        IoError::Malformed {
            src: header.as_ref().map(|header| header.src.clone()),
            msg_id: header.and_then(|header| header.body.id),
            source,
        }
    })
}

pub struct StdioWriter {
    stdout: tokio::io::Stdout,
}

impl Default for StdioWriter {
    fn default() -> Self {
        Self {
            stdout: tokio::io::stdout(),
        }
    }
}

impl MessageWriter for StdioWriter {
    async fn send<T: Serialize + Send>(&mut self, msg: Msg<T>) -> Result<(), IoError> {
        let mut line = serde_json::to_string(&msg).map_err(StdioErr::Serialize)?;
        line.push('\n');

        self.stdout
            .write_all(line.as_bytes())
            .await
            .map_err(StdioErr::Write)?;
        self.stdout.flush().await.map_err(StdioErr::Write)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::maelstrom::protocol::wire::Payload;

    use super::*;

    #[test]
    fn decodes_valid_message() {
        let msg = decode::<Payload>(
            r#"{"src":"c1","dest":"n1","body":{"type":"send","msg_id":1,"key":"k","msg":7}}"#,
        )
        .expect("valid message");

        assert_eq!(msg.src, "c1");
        assert_eq!(msg.body.id, Some(1));
    }

    #[test]
    fn malformed_payload_keeps_envelope() {
        let result = decode::<Payload>(
            r#"{"src":"c1","dest":"n1","body":{"type":"send","msg_id":3,"key":7,"msg":1}}"#,
        );

        assert!(matches!(
            result,
            Err(IoError::Malformed { src: Some(src), msg_id: Some(3), .. }) if src == "c1"
        ));
    }

    #[test]
    fn unknown_type_keeps_envelope() {
        let result =
            decode::<Payload>(r#"{"src":"c1","dest":"n1","body":{"type":"nope","msg_id":4}}"#);

        assert!(matches!(
            result,
            Err(IoError::Malformed {
                msg_id: Some(4),
                ..
            })
        ));
    }

    #[test]
    fn garbage_has_no_envelope() {
        let result = decode::<Payload>("not json");

        assert!(matches!(
            result,
            Err(IoError::Malformed {
                src: None,
                msg_id: None,
                ..
            })
        ));
    }
}
