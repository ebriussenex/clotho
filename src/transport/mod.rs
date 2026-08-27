mod pending;

use std::sync::{
    Arc,
    atomic::{self, AtomicU64},
};

use backon::BackoffBuilder;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{
    app::ClientResponse,
    kv::{CasReq, CasRes, KvEvent, KvRequest, ReadReq},
    message::Outgoing,
    replication::ReplicationRequest,
    retry::RetryPolicy,
};

use self::pending::PendingTable;

#[derive(Debug, Error)]
pub enum TransportErr {
    #[error("got response, which not awaited")]
    NotAwaited,
    #[error("timeout receive {0}")]
    TimeoutRx(&'static str),
    #[error("sender dropped: {0}")]
    SenderDropped(&'static str),
    #[error("failed to send {item} to {channel} channel")]
    ChanSend {
        channel: &'static str,
        item: &'static str,
    },
}

impl TransportErr {
    pub fn chan_send(channel: &'static str, item: &'static str) -> Self {
        tracing::error!(channel, item, "channel send failed");
        Self::ChanSend { channel, item }
    }
}

#[derive(Clone)]
pub struct Transport {
    msg_id: Arc<AtomicU64>,
    write_tx: mpsc::Sender<Outgoing>,
    timeout: tokio::time::Duration,
    pending: PendingTable,
    retry_policy: RetryPolicy,
}

impl Transport {
    pub fn new(
        write_tx: mpsc::Sender<Outgoing>,
        timeout: tokio::time::Duration,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            msg_id: Arc::new(AtomicU64::new(1)),
            write_tx,
            timeout,
            pending: PendingTable::new(),
            retry_policy,
        }
    }

    pub async fn rpc_cas(&self, req: &CasReq) -> Result<CasRes, TransportErr> {
        let mut backoff = self.retry_policy.backoff().build();

        loop {
            match self.rpc_cas_once(req).await {
                Err(err @ TransportErr::TimeoutRx(_)) => match backoff.next() {
                    Some(delay) => {
                        tracing::warn!(?delay, "retrying cas rpc after timeout");
                        tokio::time::sleep(delay).await;
                    }
                    None => return Err(err),
                },
                res => return res,
            }
        }
    }

    async fn rpc_cas_once(&self, req: &CasReq) -> Result<CasRes, TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        let req = Outgoing::KvRequest {
            req: KvRequest::Cas(req.clone()),
            msg_id,
        };

        let rx = self.pending.add_cas(msg_id).await;
        self.write_tx
            .send(req)
            .await
            .map_err(|_| TransportErr::chan_send("write_tx", "cas request"))?;

        let res = match tokio::time::timeout(self.timeout, rx).await {
            Ok(res) => res,
            Err(_) => {
                self.pending.remove_cas(msg_id).await;
                return Err(TransportErr::TimeoutRx("cas"));
            }
        };

        res.map_err(|_| TransportErr::SenderDropped("cas_tx"))
    }

    pub async fn rpc_read(&self, req: &ReadReq) -> Result<usize, TransportErr> {
        let mut backoff = self.retry_policy.backoff().build();

        loop {
            match self.rpc_read_once(req).await {
                Err(err @ TransportErr::TimeoutRx(_)) => match backoff.next() {
                    Some(delay) => {
                        tracing::warn!(?delay, "retrying read rpc after timeout");
                        tokio::time::sleep(delay).await;
                    }
                    None => return Err(err),
                },
                res => return res,
            }
        }
    }

    async fn rpc_read_once(&self, req: &ReadReq) -> Result<usize, TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        let req = Outgoing::KvRequest {
            req: KvRequest::Read(req.clone()),
            msg_id,
        };

        let rx = self.pending.add_read(msg_id).await;
        self.write_tx
            .send(req)
            .await
            .map_err(|_| TransportErr::chan_send("write_tx", "read request"))?;

        let res = match tokio::time::timeout(self.timeout, rx).await {
            Ok(res) => res,
            Err(_) => {
                self.pending.remove_read(msg_id).await;
                return Err(TransportErr::TimeoutRx("read"));
            }
        };

        Ok(res
            .map_err(|_| TransportErr::SenderDropped("read_tx"))?
            .value)
    }

    pub async fn send_client_response(
        &self,
        res: ClientResponse,
        in_reply_to: usize,
        dest: String,
    ) -> Result<(), TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        self.write_tx
            .send(Outgoing::ClientResponse {
                res,
                msg_id,
                in_reply_to,
                dest,
            })
            .await
            .map_err(|_| TransportErr::chan_send("write_tx", "client response"))
    }

    pub async fn send_replication_request(
        &self,
        dest: &String,
        req: ReplicationRequest,
    ) -> Result<(), TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        self.write_tx
            .send(Outgoing::ReplicationRequest {
                req,
                msg_id,
                dest: dest.to_string(),
            })
            .await
            .map_err(|_| TransportErr::chan_send("write_tx", "replication request"))
    }

    pub async fn register_kv_event(&self, event: KvEvent) -> Result<(), TransportErr> {
        let msg_id = event.get_in_reply_to();
        let res = match event {
            KvEvent::CasRes(res) => self.pending.reg_cas(msg_id, res).await,
            KvEvent::ReadRes(res) => self.pending.reg_read(msg_id, res).await,
        };

        match res {
            Err(TransportErr::NotAwaited) => {
                tracing::warn!(msg_id, "ignoring response without pending request");
                Ok(())
            }
            res => res,
        }
    }
}
