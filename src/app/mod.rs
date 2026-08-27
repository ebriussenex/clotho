mod request;
mod response;

pub use request::{ClientRequest, PollCommand, Request, SendCommand};
pub use response::{ClientResponse, ListCommittedOffsetsRes, PollRes, SendRes};

use backon::BackoffBuilder;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    kv::{CasReq, CasRes, CasResUnknown, ReadReq},
    offsets::store::CommitedOffsets,
    retry::RetryPolicy,
    storage::{Logs, StorageErr},
    transport::{Transport, TransportErr},
};

use self::request::{CommitReq, ListReq, PollReq, SendReq};

#[derive(Debug, Error)]
pub enum AppErr {
    #[error("app cancelled")]
    Cancelled,
    #[error("unexpected cas result: code: {code} text: {text}")]
    UnexpectedCas { code: u8, text: String },
    #[error("cas conflict retry exhausted for key {key}")]
    CasConflictRetryExhausted { key: String },
    #[error(transparent)]
    Storage(#[from] StorageErr),
    #[error(transparent)]
    Transport(#[from] TransportErr),
}

pub struct App {
    logs: Logs,
    transport: Transport,
    commited_offsets: CommitedOffsets,
    poll_max_msgs_size: usize,
    cas_retry: RetryPolicy,
}

impl App {
    pub fn new(
        logs: Logs,
        transport: Transport,
        commited_offsets: CommitedOffsets,
        poll_max_msgs_size: usize,
        cas_retry: RetryPolicy,
    ) -> Self {
        Self {
            logs,
            transport,
            commited_offsets,
            poll_max_msgs_size,
            cas_retry,
        }
    }

    pub async fn run(
        self,
        token: CancellationToken,
        mut client_rx: mpsc::Receiver<ClientRequest>,
    ) -> Result<(), AppErr> {
        loop {
            let req = tokio::select! {
                _ = token.cancelled() => return Ok(()),
                req = client_rx.recv() => req,
            };

            match req {
                Some(req) => self.handle(&token, req).await?,
                None => return Ok(()),
            }
        }
    }

    async fn handle(&self, token: &CancellationToken, req: ClientRequest) -> Result<(), AppErr> {
        match req {
            ClientRequest::Send(req) => self.handle_send(token, req).await,
            ClientRequest::Poll(req) => self.handle_poll(req).await,
            ClientRequest::CommitOffsets(req) => self.handle_commit_offsets(req).await,
            ClientRequest::ListCommittedOffsets(req) => {
                self.handle_list_commited_offsets(req).await
            }
        }
    }

    async fn handle_poll(&self, req: PollReq) -> Result<(), AppErr> {
        let resp = ClientResponse::Poll(PollRes {
            msgs: self
                .logs
                .poll(req.command.offsets, self.poll_max_msgs_size)
                .await,
        });

        self.transport
            .send_client_response(resp, req.ctx.in_reply_to, req.ctx.reply_to)
            .await?;

        Ok(())
    }

    async fn handle_list_commited_offsets(&self, req: ListReq) -> Result<(), AppErr> {
        let offsets = self
            .commited_offsets
            .list(&req.command)
            .await
            .into_iter()
            .map(|(key, offset)| (key.0, offset.0 as usize))
            .collect();

        let resp = ClientResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets });
        self.transport
            .send_client_response(resp, req.ctx.in_reply_to, req.ctx.reply_to)
            .await?;

        Ok(())
    }

    async fn handle_send(&self, token: &CancellationToken, req: SendReq) -> Result<(), AppErr> {
        let SendReq { ctx, command } = req;
        let key = command.key;
        let next_offset = self.logs.next_offset(&key).await;

        let cas_req = CasReq {
            key: key.clone(),
            from: next_offset,
            to: next_offset + 1,
        };

        let offset = self.rpc_cas(token, cas_req).await?;

        self.logs.append(key, command.msg, offset).await?;

        let res = ClientResponse::Send(SendRes { offset });

        self.transport
            .send_client_response(res, ctx.in_reply_to, ctx.reply_to)
            .await?;

        Ok(())
    }

    async fn handle_commit_offsets(&self, req: CommitReq) -> Result<(), AppErr> {
        self.commited_offsets.commit(&req.command).await;

        self.transport
            .send_client_response(
                ClientResponse::CommitOffsets,
                req.ctx.in_reply_to,
                req.ctx.reply_to,
            )
            .await?;

        Ok(())
    }

    async fn rpc_cas(
        &self,
        token: &CancellationToken,
        mut cas_req: CasReq,
    ) -> Result<usize, AppErr> {
        let mut backoff = self.cas_retry.backoff().build();

        loop {
            let offset = cas_req.from;
            let key = cas_req.key.clone();
            let cas_res = self.transport.rpc_cas(&cas_req).await?;

            match cas_res {
                CasRes::Ok(_) => return Ok(offset),
                CasRes::Retry(_) => {
                    let latest_offset = self.transport.rpc_read(&ReadReq { key }).await?;

                    cas_req.from = latest_offset;
                    cas_req.to = latest_offset + 1;

                    match backoff.next() {
                        Some(delay) => {
                            tracing::warn!(
                                key = %cas_req.key,
                                ?delay,
                                "retrying cas after conflict"
                            );

                            tokio::select! {
                                _ = token.cancelled() => return Err(AppErr::Cancelled),
                                _ = tokio::time::sleep(delay) => {}
                            }
                        }
                        None => {
                            return Err(AppErr::CasConflictRetryExhausted {
                                key: cas_req.key.clone(),
                            });
                        }
                    }
                }
                CasRes::Unknown(CasResUnknown { code, text, .. }) => {
                    return Err(AppErr::UnexpectedCas { code, text });
                }
            }
        }
    }
}
