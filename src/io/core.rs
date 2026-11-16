use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::io::stdio::StdioErr;
use crate::maelstrom::protocol::wire::Msg;

/// Errors of reading and writing messages.
#[derive(Debug, Error)]
pub enum IoError {
    /// The underlying stream failed. Fatal: no more messages can be read or written.
    #[error(transparent)]
    StdIo(#[from] StdioErr),
    /// A message could not be decoded; the stream is intact and the next message can be read.
    /// `src` and `msg_id` are set when they could be read, so the sender can be answered.
    #[error("malformed message (src: {src:?}, msg_id: {msg_id:?}): {source}")]
    Malformed {
        src: Option<String>,
        msg_id: Option<u64>,
        #[source]
        source: serde_json::Error,
    },
}

pub trait MessageReader: Send + 'static {
    /// Reads the next message; `Ok(None)` at the end of input.
    ///
    /// # Errors
    ///
    /// [`IoError::Malformed`] if the message can't be decoded, [`IoError::StdIo`] if reading
    /// failed.
    fn next<'a, T: DeserializeOwned + 'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<Option<Msg<T>>, IoError>> + Send + 'a;
}

pub trait MessageWriter: Send + 'static {
    /// # Errors
    ///
    /// [`IoError::StdIo`] if the message can't be serialized or written.
    fn send<T: Serialize + Send>(
        &mut self,
        msg: Msg<T>,
    ) -> impl Future<Output = Result<(), IoError>> + Send;
}
