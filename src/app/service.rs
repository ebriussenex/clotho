use backon::{BackoffBuilder, ExponentialBuilder};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    app::{
        command::{CommitOffsetsCommand, ListCommittedOffsetsCommand},
        offsets::CommitedOffsets,
    },
    kv::{CasReq, CasRes, CasResUnknown, ReadReq},
    logs::{Logs, StorageErr},
    transport::{Transport, TransportErr},
};

use super::command::{AppCommand, Command, PollCommand, ReplyContext, SendCommand};
use super::response::{ClientResponse, ListCommittedOffsetsRes, PollRes, SendRes};

#[derive(Debug, Error)]
pub enum AppErr {
    #[error("app cancelled")]
    Cancelled,
    #[error("unexpected cas result: code: {code} text: {text}")]
    UnexpectedCas { code: u8, text: String },
    #[error("cas conflict retry exhausted for key {key:?}")]
    CasConflictRetryExhausted { key: crate::domain::LogId },
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
    cas_backoff: ExponentialBuilder,
}

impl App {
    pub fn new(
        logs: Logs<u32>,
        transport: Transport,
        commited_offsets: CommitedOffsets,
        poll_max_msgs_size: usize,
        cas_backoff: ExponentialBuilder,
    ) -> Self {
        Self {
            logs,
            transport,
            commited_offsets,
            poll_max_msgs_size,
            cas_backoff,
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
                Some(command) => self.handle(&token, command).await?,
                None => return Ok(()),
            }
        }
    }

    async fn handle(&self, token: &CancellationToken, command: AppCommand) -> Result<(), AppErr> {
        let AppCommand { reply_ctx, kind } = command;
        match kind {
            Command::Send(command) => self.handle_send(token, reply_ctx, command).await,
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
        let resp = ClientResponse::Poll(PollRes {
            msgs: self
                .logs
                .poll(command.offsets, self.poll_max_msgs_size)
                .await,
        });

        self.transport.send_client_response(resp, reply_ctx).await?;

        Ok(())
    }

    async fn handle_list_committed_offsets(
        &self,
        reply_ctx: ReplyContext,
        command: ListCommittedOffsetsCommand,
    ) -> Result<(), AppErr> {
        let offsets = self.commited_offsets.list(&command).await;
        let resp = ClientResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets });

        self.transport.send_client_response(resp, reply_ctx).await?;

        Ok(())
    }

    async fn handle_send(
        &self,
        token: &CancellationToken,
        reply_ctx: ReplyContext,
        command: SendCommand,
    ) -> Result<(), AppErr> {
        let key = command.key;
        let next_offset = self.logs.next_offset(&key).await;

        let cas_req = CasReq {
            key: key.clone(),
            from: next_offset,
            to: crate::domain::Offset(next_offset.0 + 1),
        };

        let offset = self.rpc_cas(token, cas_req).await?;

        self.logs.append(key, command.msg, offset).await?;

        let resp = ClientResponse::Send(SendRes { offset });
        self.transport.send_client_response(resp, reply_ctx).await?;

        Ok(())
    }

    async fn handle_commit_offsets(
        &self,
        reply_ctx: ReplyContext,
        command: CommitOffsetsCommand,
    ) -> Result<(), AppErr> {
        self.commited_offsets.commit(&command).await;

        self.transport
            .send_client_response(ClientResponse::CommitOffsets, reply_ctx)
            .await?;

        Ok(())
    }

    async fn rpc_cas(
        &self,
        token: &CancellationToken,
        mut cas_req: CasReq,
    ) -> Result<crate::domain::Offset, AppErr> {
        let mut backoff = self.cas_backoff.build();
        let offset = cas_req.from;
        loop {
            match self.transport.rpc_cas(token, &cas_req).await? {
                CasRes::Ok(_) => return Ok(offset),
                CasRes::Retry(_) => {
                    let latest_offset = self
                        .transport
                        .rpc_read(
                            token,
                            &ReadReq {
                                key: cas_req.key.clone(),
                            },
                        )
                        .await?;

                    cas_req.from = latest_offset;
                    cas_req.to = crate::domain::Offset(latest_offset.0 + 1);

                    let Some(delay) = backoff.next() else {
                        return Err(AppErr::CasConflictRetryExhausted { key: cas_req.key });
                    };
                    tracing::warn!(key = %cas_req.key.0, ?delay, "retrying cas after conflict");
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => return Err(AppErr::Cancelled),
                        _ = tokio::time::sleep(delay) => {},
                    }
                }
                CasRes::Unknown(CasResUnknown { code, text, .. }) => {
                    return Err(AppErr::UnexpectedCas { code, text });
                }
            }
        }
    }
}
