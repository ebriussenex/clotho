use std::sync::{
    Arc,
    atomic::{self, AtomicU64},
};

use backon::{BackoffBuilder, ExponentialBuilder};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    app::{ClientResponse, ReplyContext},
    kv::{CasReq, CasRes, KvEvent, KvRequest, ReadReq},
    message::Outgoing,
    replication::ReplicationMessage,
};

use super::pending::PendingTable;

#[derive(Debug, Error)]
pub enum TransportErr {
    #[error("got response, which not awaited")]
    NotAwaited,
    #[error("timeout receive {0}")]
    TimeoutRx(&'static str),
    #[error("sender dropped: {0}")]
    SenderDropped(&'static str),
    #[error("transport cancelled")]
    Cancelled,
    #[error("failed to send to {channel} channel")]
    ChanSend { channel: &'static str },
}

impl TransportErr {
    pub fn chan_send<T>(channel: &'static str) -> Self {
        tracing::error!(
            channel,
            payload_type = std::any::type_name::<T>(),
            "channel send failed"
        );
        Self::ChanSend { channel }
    }
}

#[derive(Clone)]
pub struct Transport {
    msg_id: Arc<AtomicU64>,
    write_tx: mpsc::Sender<Outgoing>,
    timeout: tokio::time::Duration,
    pending: PendingTable,
    timeout_backoff: ExponentialBuilder,
}

impl Transport {
    pub fn new(
        write_tx: mpsc::Sender<Outgoing>,
        timeout: tokio::time::Duration,
        timeout_backoff: ExponentialBuilder,
    ) -> Self {
        Self {
            msg_id: Arc::new(AtomicU64::new(1)),
            write_tx,
            timeout,
            pending: PendingTable::new(),
            timeout_backoff,
        }
    }

    pub async fn rpc_cas(
        &self,
        token: &CancellationToken,
        request: &CasReq,
    ) -> Result<CasRes, TransportErr> {
        let mut backoff = self.timeout_backoff.build();
        loop {
            match self.rpc_cas_once(token, request).await {
                Err(err @ TransportErr::TimeoutRx(_)) => {
                    let Some(delay) = backoff.next() else {
                        return Err(err);
                    };
                    tracing::warn!(error = %err, ?delay, "retrying cas rpc");
                    wait_for_retry(token, delay).await?;
                }
                result => return result,
            }
        }
    }

    async fn rpc_cas_once(
        &self,
        token: &tokio_util::sync::CancellationToken,
        request: &CasReq,
    ) -> Result<CasRes, TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        let outgoing = Outgoing::KvRequest {
            request: KvRequest::Cas(request.clone()),
            msg_id,
        };

        let rx = self.pending.add_cas(msg_id).await;
        if self.write_tx.send(outgoing).await.is_err() {
            self.pending.remove_cas(msg_id).await;
            return Err(TransportErr::chan_send::<Outgoing>("write_tx"));
        }

        tokio::select! {
            biased;
            _ = token.cancelled() => {
                self.pending.remove_cas(msg_id).await;
                Err(TransportErr::Cancelled)
            }
            res = rx => res.map_err(|_| TransportErr::SenderDropped("cas_tx")),
            _ = tokio::time::sleep(self.timeout) => {
                self.pending.remove_cas(msg_id).await;
                Err(TransportErr::TimeoutRx("cas"))
            }
        }
    }

    pub async fn rpc_read(
        &self,
        token: &tokio_util::sync::CancellationToken,
        request: &ReadReq,
    ) -> Result<crate::domain::Offset, TransportErr> {
        let mut backoff = self.timeout_backoff.build();
        loop {
            match self.rpc_read_once(token, request).await {
                Err(err @ TransportErr::TimeoutRx(_)) => {
                    let Some(delay) = backoff.next() else {
                        return Err(err);
                    };
                    tracing::warn!(error = %err, ?delay, "retrying read rpc");
                    wait_for_retry(token, delay).await?;
                }
                result => return result,
            }
        }
    }

    async fn rpc_read_once(
        &self,
        token: &tokio_util::sync::CancellationToken,
        request: &ReadReq,
    ) -> Result<crate::domain::Offset, TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        let outgoing = Outgoing::KvRequest {
            request: KvRequest::Read(request.clone()),
            msg_id,
        };

        let rx = self.pending.add_read(msg_id).await;
        if self.write_tx.send(outgoing).await.is_err() {
            self.pending.remove_read(msg_id).await;
            return Err(TransportErr::chan_send::<KvRequest>("write_tx"));
        }

        let res = tokio::select! {
            biased;
            _ = token.cancelled() => {
                self.pending.remove_read(msg_id).await;
                return Err(TransportErr::Cancelled);
            }
            res = rx => res,
            _ = tokio::time::sleep(self.timeout) => {
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
        reply_ctx: ReplyContext,
    ) -> Result<(), TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        let ReplyContext { in_reply_to, dest } = reply_ctx;

        self.write_tx
            .send(Outgoing::ClientResponse {
                res,
                msg_id,
                in_reply_to,
                dest,
            })
            .await
            .map_err(|_| TransportErr::chan_send::<ClientResponse>("write_tx"))
    }

    pub async fn send_replication(
        &self,
        dest: &str,
        message: ReplicationMessage,
    ) -> Result<(), TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        self.write_tx
            .send(Outgoing::Replication {
                message,
                msg_id,
                dest: dest.to_owned(),
            })
            .await
            .map_err(|_| TransportErr::chan_send::<ReplicationMessage>("write_tx"))
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

async fn wait_for_retry(
    token: &tokio_util::sync::CancellationToken,
    delay: tokio::time::Duration,
) -> Result<(), TransportErr> {
    tokio::select! {
        biased;
        _ = token.cancelled() => Err(TransportErr::Cancelled),
        _ = tokio::time::sleep(delay) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use backon::ExponentialBuilder;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::{kv::ReadRes, message::Outgoing};

    use super::{Transport, TransportErr};

    fn transport(timeout: Duration) -> (Transport, mpsc::Receiver<Outgoing>) {
        let (write_tx, write_rx) = mpsc::channel(8);
        let backoff = ExponentialBuilder::default()
            .with_max_times(2)
            .with_min_delay(Duration::from_millis(1))
            .with_max_delay(Duration::from_millis(1));

        (Transport::new(write_tx, timeout, backoff), write_rx)
    }

    #[tokio::test]
    async fn read_rpc_retries_after_timeout() {
        let (transport, mut write_rx) = transport(Duration::from_millis(5));
        let token = CancellationToken::new();
        let task = tokio::spawn({
            let transport = transport.clone();
            let token = token.clone();
            async move {
                transport
                    .rpc_read(
                        &token,
                        &crate::kv::ReadReq {
                            key: crate::domain::LogId("key".to_owned()),
                        },
                    )
                    .await
            }
        });

        let first_id = match write_rx.recv().await.unwrap() {
            Outgoing::KvRequest { msg_id, .. } => msg_id,
            _ => panic!("expected kv request"),
        };
        let second_id = match write_rx.recv().await.unwrap() {
            Outgoing::KvRequest { msg_id, .. } => msg_id,
            _ => panic!("expected retried kv request"),
        };
        assert_ne!(first_id, second_id);

        transport
            .register_kv_event(crate::kv::KvEvent::ReadRes(ReadRes {
                in_reply_to: second_id,
                value: crate::domain::Offset(42),
            }))
            .await
            .unwrap();

        assert_eq!(task.await.unwrap().unwrap(), crate::domain::Offset(42));
    }

    #[tokio::test]
    async fn cancellation_stops_rpc_without_retrying() {
        let (transport, mut write_rx) = transport(Duration::from_secs(1));
        let token = CancellationToken::new();
        let task = tokio::spawn({
            let transport = transport.clone();
            let token = token.clone();
            async move {
                transport
                    .rpc_read(
                        &token,
                        &crate::kv::ReadReq {
                            key: crate::domain::LogId("key".to_owned()),
                        },
                    )
                    .await
            }
        });

        let _ = write_rx.recv().await.unwrap();
        token.cancel();

        assert!(matches!(task.await.unwrap(), Err(TransportErr::Cancelled)));
        assert!(write_rx.try_recv().is_err());
    }
}
