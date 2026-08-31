use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    app::{
        command::{CommitOffsetsCommand, ListCommittedOffsetsCommand},
        offsets::CommitedOffsets,
    },
    logs::{Logs, StorageErr},
    transport::{Transport, TransportErr},
};

use super::command::{AppCommand, Command, PollCommand, ReplyContext, SendCommand};
use super::response::{CommandResponse, ListCommittedOffsetsRes, PollRes, SendRes};

#[derive(Debug, Error)]
pub enum AppErr {
    #[error(transparent)]
    Storage(#[from] StorageErr),
    #[error(transparent)]
    Transport(#[from] TransportErr),
}

pub struct App {
    logs: Logs<u32>,
    transport: Transport,
    commited_offsets: CommitedOffsets,
    poll_max_msgs_size: usize,
}

impl App {
    pub fn new(
        logs: Logs<u32>,
        transport: Transport,
        commited_offsets: CommitedOffsets,
        poll_max_msgs_size: usize,
    ) -> Self {
        Self {
            logs,
            transport,
            commited_offsets,
            poll_max_msgs_size,
        }
    }

    pub async fn run(
        self,
        token: CancellationToken,
        mut command_rx: mpsc::Receiver<AppCommand>,
    ) -> Result<(), AppErr> {
        loop {
            let command = tokio::select! {
                biased;
                _ = token.cancelled() => return Ok(()),
                command = command_rx.recv() => command,
            };

            match command {
                Some(command) => self.handle(command).await?,
                None => return Ok(()),
            }
        }
    }

    async fn handle(&self, command: AppCommand) -> Result<(), AppErr> {
        let AppCommand { reply_ctx, kind } = command;
        match kind {
            Command::Send(command) => self.handle_send(reply_ctx, command).await,
            Command::Poll(command) => self.handle_poll(reply_ctx, command).await,
            Command::CommitOffsets(command) => self.handle_commit_offsets(reply_ctx, command).await,
            Command::ListCommittedOffsets(command) => {
                self.handle_list_committed_offsets(reply_ctx, command).await
            }
        }
    }

    async fn handle_poll(
        &self,
        reply_ctx: ReplyContext,
        command: PollCommand,
    ) -> Result<(), AppErr> {
        let response = CommandResponse::Poll(PollRes {
            msgs: self
                .logs
                .poll(command.offsets, self.poll_max_msgs_size)
                .await,
        });
        self.transport.reply(response, reply_ctx).await?;
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

    async fn handle_send(
        &self,
        reply_ctx: ReplyContext,
        command: SendCommand,
    ) -> Result<(), AppErr> {
        let offset = self.logs.next_offset(&command.key).await;
        self.logs.append(command.key, command.msg, offset).await?;
        self.transport
            .reply(CommandResponse::Send(SendRes { offset }), reply_ctx)
            .await?;
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
