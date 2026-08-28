use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::io::stdio::StdioErr;
use crate::maelstrom::protocol::wire::Msg;

#[derive(Debug, Error)]
pub enum IoError {
    #[error(transparent)]
    StdIo(#[from] StdioErr),
}

pub trait MessageReader: Send + 'static {
    fn next<'a, T: DeserializeOwned + 'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<Option<Msg<T>>, IoError>> + Send + 'a;
}

pub trait MessageWriter: Send + 'static {
    fn send<T: Serialize + Send>(
        &mut self,
        msg: Msg<T>,
    ) -> impl Future<Output = Result<(), IoError>> + Send;
}
