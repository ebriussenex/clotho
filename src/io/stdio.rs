use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, Stdin};

use crate::io::{IoError, MessageReader, MessageWriter};
use crate::maelstrom::protocol::wire::Msg;

#[derive(Debug, Error)]
pub enum StdioErr {
    #[error("failed to read msg from stdio: {0}")]
    Read(#[source] std::io::Error),
    #[error("failed to deserialize msg from json: {0}")]
    Deserialize(#[source] serde_json::Error),
    #[error("failed to write msg to stdout: {0}")]
    Write(#[source] std::io::Error),
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
            Some(line) => Ok(Some(
                serde_json::from_str(&line).map_err(StdioErr::Deserialize)?,
            )),
            None => Ok(None),
        }
    }
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
