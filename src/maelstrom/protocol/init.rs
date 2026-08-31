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

#[derive(Debug, Error)]
pub enum InitErr {
    #[error("no init message received")]
    NoInitMsg,
    #[error("first message wasn't init")]
    NotInit,
    #[error(transparent)]
    Io(#[from] IoError),
}

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
