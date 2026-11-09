use std::sync::Arc;

use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinSet},
};
use tokio_util::{bytes::Bytes, sync::CancellationToken};

use crate::{
    app::{
        command::{CommitOffsetsCommand, ListCommittedOffsetsCommand},
        offsets::CommitedOffsets,
        ownership::Ownership,
        response::{CommandError, ErrorKind},
    },
    logs::{StorageErr, Store},
    transport::{Transport, TransportErr},
};

use super::command::{AppCommand, Command, CommandSource, PollCommand, ReplyContext, SendCommand};
use super::response::{CommandResponse, ListCommittedOffsetsRes, PollRes, SendRes};

#[derive(Debug, Error)]
pub enum AppErr {
    #[error(transparent)]
    Storage(#[from] StorageErr),
    #[error(transparent)]
    Transport(#[from] TransportErr),
    #[error("command handler task failed: {0}")]
    Handler(#[from] JoinError),
}

pub struct App {
    logs: Store,
    transport: Transport,
    commited_offsets: CommitedOffsets,
    ownership: Ownership,
    poll_max_msgs_size: usize,
    max_in_flight: usize,
}

impl App {
    pub fn new(
        logs: Store,
        transport: Transport,
        commited_offsets: CommitedOffsets,
        ownership: Ownership,
        poll_max_msgs_size: usize,
        max_in_flight: usize,
    ) -> Self {
        Self {
            logs,
            transport,
            commited_offsets,
            ownership,
            poll_max_msgs_size,
            // 0 would never accept a command
            max_in_flight: max_in_flight.max(1),
        }
    }

    /// Runs app, handling commands *concurrently*, at most `max_if_flight` at a time. Respects two,
    /// forwarding to each other, nodes.
    pub async fn run(
        self,
        token: CancellationToken,
        mut command_rx: mpsc::Receiver<AppCommand>,
    ) -> Result<(), AppErr> {
        let app = Arc::new(self);
        let handlers_token = token.child_token();
        let mut handlers = JoinSet::new();

        let result = loop {
            let has_capacity = handlers.len() < app.max_in_flight;

            tokio::select! {
                biased;
                _ = token.cancelled() => break Ok(()),
                Some(joined) = handlers.join_next() => {
                    if let Err(e) = flatten(joined) {
                        break Err(e);
                    }
                }
                command = command_rx.recv(), if has_capacity => match command {
                    Some(command) => {
                        let app = Arc::clone(&app);
                        let token = handlers_token.clone();
                        handlers.spawn(async move { app.handle(&token, command).await });
                    }
                    None => break Ok(()),
                },
            }
        };

        if result.is_err() {
            handlers_token.cancel();
        }
        while let Some(joined) = handlers.join_next().await {
            if let Err(e) = flatten(joined) {
                tracing::warn!(error = %e, "command handler failed while app was stopping");
            }
        }

        result
    }

    async fn handle(&self, token: &CancellationToken, command: AppCommand) -> Result<(), AppErr> {
        let AppCommand {
            reply_ctx,
            kind,
            source,
        } = command;

        match kind {
            Command::Send(command) => self.handle_send(token, source, reply_ctx, command).await,
            // Multi-key commands are still served from local state only: they need partitioning
            // by owner, fan-out and merging of the responses.
            Command::Poll(command) => self.handle_poll(reply_ctx, command).await,
            Command::CommitOffsets(command) => self.handle_commit_offsets(reply_ctx, command).await,
            Command::ListCommittedOffsets(command) => {
                self.handle_list_committed_offsets(reply_ctx, command).await
            }
        }
    }

    async fn handle_send(
        &self,
        token: &CancellationToken,
        source: CommandSource,
        reply_ctx: ReplyContext,
        command: SendCommand,
    ) -> Result<(), AppErr> {
        // A command from a peer was already routed here by its coordinator: execute it locally,
        // never forward it again.
        if source == CommandSource::Client && !self.ownership.is_local(&command.key) {
            let owner = self.ownership.owner(&command.key);
            let result = self
                .transport
                .forward(token, owner, Command::Send(command))
                .await;
            return match result {
                Ok(response @ CommandResponse::Send(_)) => {
                    Ok(self.transport.reply(response, reply_ctx).await?)
                }
                Ok(response) => {
                    tracing::error!(owner, ?response, "owner replied to send with a wrong response");
                    let error = CommandError::new(ErrorKind::Internal, "unexpected reply from owner");
                    Ok(self.transport.reply_error(error, reply_ctx).await?)
                }
                Err(e) => self.forward_failed(e, owner, reply_ctx).await,
            };
        }

        match self.logs.append(&command.key, encode_msg(command.msg)).await {
            Ok(offset) => {
                let response = CommandResponse::Send(SendRes { offset });
                Ok(self.transport.reply(response, reply_ctx).await?)
            }
            // definite: nothing was appended, a peer coordinator will retry
            Err(StorageErr::ShuttingDown) => {
                let error = CommandError::new(ErrorKind::Unavailable, "node is shutting down");
                Ok(self.transport.reply_error(error, reply_ctx).await?)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Answers the requester when forwarding to the `owner` failed. Fatal transport errors
    /// stop the app.
    async fn forward_failed(
        &self,
        error: TransportErr,
        owner: &str,
        reply_ctx: ReplyContext,
    ) -> Result<(), AppErr> {
        let reply = match error {
            TransportErr::Timeout(_) => {
                tracing::warn!(owner, dest = %reply_ctx.dest, "owner did not reply in time");
                CommandError::new(ErrorKind::Timeout, format!("owner {owner} did not reply in time"))
            }
            TransportErr::Remote(error) => {
                tracing::warn!(owner, ?error, "owner replied with an error");
                error
            }
            // the node is stopping, nobody to answer to
            TransportErr::Cancelled => return Ok(()),
            e => return Err(e.into()),
        };
        Ok(self.transport.reply_error(reply, reply_ctx).await?)
    }

    async fn handle_poll(
        &self,
        reply_ctx: ReplyContext,
        command: PollCommand,
    ) -> Result<(), AppErr> {
        let msgs = self
            .logs
            .poll(&command.offsets, self.poll_max_msgs_size)
            .into_iter()
            .map(|(key, (from, entries))| (key, decode_entries(from, &entries)))
            .collect();

        self.transport
            .reply(CommandResponse::Poll(PollRes { msgs }), reply_ctx)
            .await?;
        Ok(())
    }

    async fn handle_list_committed_offsets(
        &self,
        reply_ctx: ReplyContext,
        command: ListCommittedOffsetsCommand,
    ) -> Result<(), AppErr> {
        let offsets = self.commited_offsets.list(&command).await;
        let response = CommandResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets });
        self.transport.reply(response, reply_ctx).await?;
        Ok(())
    }

    async fn handle_commit_offsets(
        &self,
        reply_ctx: ReplyContext,
        command: CommitOffsetsCommand,
    ) -> Result<(), AppErr> {
        self.commited_offsets.commit(&command).await;
        self.transport
            .reply(CommandResponse::CommitOffsets, reply_ctx)
            .await?;
        Ok(())
    }
}

fn flatten(joined: Result<Result<(), AppErr>, JoinError>) -> Result<(), AppErr> {
    joined?
}

fn encode_msg(msg: u32) -> Bytes {
    Bytes::copy_from_slice(&msg.to_be_bytes())
}

/// Pairs log entries with their offsets, starting at `from`. An entry that is not a 4-byte
/// message (never written by `encode_msg`) is skipped and logged instead of panicking.
fn decode_entries(from: u64, entries: &[Bytes]) -> Vec<(u64, u32)> {
    entries
        .iter()
        .enumerate()
        .filter_map(|(idx, entry)| {
            let offset = from.saturating_add(idx as u64);
            match <[u8; 4]>::try_from(entry.as_ref()) {
                Ok(bytes) => Some((offset, u32::from_be_bytes(bytes))),
                Err(_) => {
                    tracing::error!(offset, len = entry.len(), "skipping malformed log entry");
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_entries_roundtrips_encoded_messages() {
        let entries = [encode_msg(7), encode_msg(u32::MAX), encode_msg(0)];

        assert_eq!(
            decode_entries(10, &entries),
            vec![(10, 7), (11, u32::MAX), (12, 0)]
        );
    }

    #[test]
    fn decode_entries_skips_malformed_entries_keeping_offsets() {
        let entries = [encode_msg(1), Bytes::from_static(&[1, 2]), encode_msg(3)];

        assert_eq!(decode_entries(0, &entries), vec![(0, 1), (2, 3)]);
    }
}
