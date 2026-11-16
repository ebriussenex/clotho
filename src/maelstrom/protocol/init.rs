use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    io::{IoError, MessageReader, MessageWriter},
    maelstrom::protocol::wire::{Body, Msg},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    node_id: String,
    node_ids: Vec<String>,
}

/// Errors of the Maelstrom handshake. All of them are fatal: without `init` the node doesn't
/// know who it is.
#[derive(Debug, Error)]
pub enum InitErr {
    /// The input ended before any message.
    #[error("no init message received")]
    NoInitMsg,
    /// The first message was not `init`.
    #[error("first message wasn't init")]
    NotInit,
    /// The first message couldn't be read, decoded, or answered.
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Reads Maelstrom's `init`, replies `init_ok` and returns this node's id and its peers' ids.
///
/// # Errors
///
/// - [`InitErr::NoInitMsg`] if the input ended before any message.
/// - [`InitErr::NotInit`] if the first message was not `init`.
/// - [`InitErr::Io`] if it couldn't be read or decoded (any malformed message, unlike after
///   the handshake), or `init_ok` couldn't be written.
pub async fn handshake(
    reader: &mut impl MessageReader,
    writer: &mut impl MessageWriter,
) -> Result<(String, Vec<String>), InitErr> {
    let init_msg = reader
        .next::<InitPayload>()
        .await?
        .ok_or(InitErr::NoInitMsg)?;

    let Msg { src, dest, body } = init_msg;
    let Body { id, payload, .. } = body;

    let Init { node_id, node_ids } = match payload {
        InitPayload::Init(init) => init,
        InitPayload::InitOk => return Err(InitErr::NotInit),
    };

    let peers_ids = node_ids
        .into_iter()
        .filter(|n_id| **n_id != node_id)
        .collect();

    let reply = Msg {
        src: dest,
        dest: src,
        body: Body {
            id: None,
            in_reply_to: id,
            payload: InitPayload::InitOk,
        },
    };
    writer.send(reply).await?;

    Ok((node_id, peers_ids))
}
